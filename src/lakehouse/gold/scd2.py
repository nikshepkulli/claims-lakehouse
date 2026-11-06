"""Type-2 slowly changing dimensions as a set-based merge (member, provider).

``scd2_merge`` folds ONE snapshot (the silver rows of a batch, taken as of the batch date) into
an existing SCD2 table. The rules (docs/gold.md, "SCD2 rules"):

* A key's history is a list of ``(effective_from, attributes)`` events. The first version of a
  key starts at ``EPOCH`` (1900-01-01) so claims dated before the first snapshot still resolve.
* The snapshot adds the event ``(batch_date, attributes)`` for every key it contains. If a version
  already starts exactly at ``batch_date`` the snapshot REPLACES it (a correction).
* Events are sorted by ``effective_from``; an event whose ``record_hash`` equals the previous one
  is not a change and is dropped (re-running a snapshot is therefore a no-op).
* ``effective_to`` is the next version's ``effective_from`` (NULL for the last), and
  ``is_current = effective_to IS NULL``. Versions of a key never overlap and never leave a gap.
* A snapshot older than versions that already exist (a late-arriving correction) is inserted
  into the middle of the history, and every following version stays as it was.
* Keys absent from a snapshot are untouched: a snapshot is evidence about the keys it contains,
  not a statement that other keys disappeared.

``record_hash`` is the SHA-256 of the tracked attributes (NULL is distinct from the empty string),
so "did anything change" is one comparison. Surrogate keys come from ``keys.derive_sk``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .keys import derive_sk, natural_id
from .schema import EPOCH, TABLES
from .tables import align, ensure_table, read


@dataclass(frozen=True)
class DimSpec:
    table: str
    kind: str  # keys.KINDS
    key: str  # natural key column (also the silver column name)
    sk: str  # surrogate key column
    attrs: tuple[str, ...]  # tracked (type-2) attributes, in hash order


MEMBER = DimSpec(
    "dim_member",
    "member",
    "member_id",
    "member_sk",
    ("birth_year", "sex", "state", "plan_type", "enrollment_date"),
)
PROVIDER = DimSpec(
    "dim_provider",
    "provider",
    "provider_id",
    "provider_sk",
    ("specialty", "state", "network_status", "cost_tier"),
)

_SEP = "\x1f"
_NULL = "\x00"


def record_hash(spec: DimSpec) -> F.Column:
    """SHA-256 over the tracked attributes; NULL hashes differently from ''."""
    parts = [F.coalesce(F.col(a).cast("string"), F.lit(_NULL)) for a in spec.attrs]
    return F.sha2(F.concat_ws(_SEP, *parts), 256)


@dataclass
class ScdResult:
    table: str
    snapshot_keys: int = 0
    versions_inserted: int = 0
    versions_updated: int = 0  # closed, reopened or corrected in place
    versions_removed: int = 0  # collapsed because a correction made them equal to a neighbour
    changed_keys: DataFrame | None = None  # EXISTING keys whose history changed (collected)

    @property
    def changed(self) -> bool:
        return bool(self.versions_inserted or self.versions_updated or self.versions_removed)


def _ntz(ts: datetime) -> F.Column:
    """A wall-clock literal, independent of the session time zone."""
    return F.lit(ts.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp_ntz")


def _version_columns(spec: DimSpec) -> list[str]:
    return [c.name for c in TABLES[spec.table]]


def scd2_merge(
    spark: SparkSession,
    dim_uri: str,
    spec: DimSpec,
    snapshot: DataFrame,
    as_of: date,
    batch_id: str,
    run_ts: datetime,
) -> ScdResult:
    """Merge ``snapshot`` (one row per key, silver column names) into the SCD2 table at ``dim_uri``.

    ``as_of`` is the snapshot's date (the silver batch date). Idempotent: running the same
    snapshot again changes nothing.
    """
    key, sk = spec.key, spec.sk
    cols = _version_columns(spec)
    as_of_ts = datetime(as_of.year, as_of.month, as_of.day)

    snap = typed_snapshot(snapshot, spec)
    if snap.groupBy(key).count().where("count > 1").limit(1).count():
        raise ValueError(f"{spec.table}: snapshot has several rows for one {key}; silver dedupes")
    result = ScdResult(spec.table, snapshot_keys=snap.count())
    if result.snapshot_keys == 0:
        return result

    ensure_table(spark, dim_uri, spec.table)
    target = read(spark, dim_uri)
    keys = snap.select(key)
    old = target.join(keys, key, "left_semi")
    old_keys = old.select(key).distinct().withColumn("_has_history", F.lit(True))

    # Events: existing versions + the snapshot (new keys start at EPOCH, others at `as_of`).
    ev_old = old.select(
        key,
        "effective_from",
        *spec.attrs,
        "record_hash",
        F.lit(0).alias("_src"),
    )
    ev_new = (
        snap.join(old_keys, key, "left")
        .withColumn(
            "effective_from",
            F.when(F.col("_has_history"), _ntz(as_of_ts)).otherwise(_ntz(EPOCH)),
        )
        .select(key, "effective_from", *spec.attrs, "record_hash", F.lit(1).alias("_src"))
    )
    events = ev_old.unionByName(ev_new)

    # Same (key, effective_from): the snapshot wins (a correction).
    w_dup = Window.partitionBy(key, "effective_from").orderBy(F.col("_src").desc())
    events = events.withColumn("_rn", F.row_number().over(w_dup)).where("_rn = 1").drop("_rn")
    # Drop events that do not change anything relative to the previous version.
    w_key = Window.partitionBy(key).orderBy("effective_from")
    events = (
        events.withColumn("_prev", F.lag("record_hash").over(w_key))
        .where(F.col("_prev").isNull() | (F.col("_prev") != F.col("record_hash")))
        .drop("_prev", "_src")
    )
    versions = (
        events.withColumn("effective_to", F.lead("effective_from").over(w_key))
        .withColumn("is_current", F.col("effective_to").isNull())
        .withColumn(sk, derive_sk(spec.kind, F.col(key), F.col("effective_from")))
    )

    # Carry load_batch_id / loaded_at of rows that did not change; stamp the rest.
    old_info = old.select(
        F.col(sk).alias("_o_sk"),
        F.col("record_hash").alias("_o_hash"),
        F.col("effective_to").alias("_o_to"),
        F.col("load_batch_id").alias("_o_batch"),
        F.col("loaded_at").alias("_o_loaded"),
    )
    joined = versions.join(old_info, F.col(sk) == F.col("_o_sk"), "left")
    unchanged = (
        F.col("_o_sk").isNotNull()
        & (F.col("_o_hash") == F.col("record_hash"))
        & F.col("_o_to").eqNullSafe(F.col("effective_to"))
    )
    new_rows = (
        joined.withColumn(
            "load_batch_id", F.when(unchanged, F.col("_o_batch")).otherwise(F.lit(batch_id))
        )
        .withColumn(
            "loaded_at",
            F.when(unchanged, F.col("_o_loaded")).otherwise(_ntz(run_ts)),
        )
        .withColumn("_changed", ~unchanged)
        .withColumn("_new_sk", F.col("_o_sk").isNull())
    )
    new_rows = new_rows.select(*cols, "_changed", "_new_sk").persist()

    tomb = (
        old.join(
            new_rows.select(F.col(sk).alias("_n_sk")), F.col(sk) == F.col("_n_sk"), "left_anti"
        )
        .select(*cols)
        .persist()
    )
    try:
        result.versions_inserted = new_rows.where("_new_sk").count()
        result.versions_updated = new_rows.where("_changed AND NOT _new_sk").count()
        result.versions_removed = tomb.count()
        # Keys that already had history and whose history moved: their facts may need re-keying.
        moved = new_rows.where("_changed").select(key).union(tomb.select(key)).distinct()
        # Collected, not persisted: unpersisting `new_rows` would re-cache a dependent DataFrame
        # lazily from the (by then merged) table and lose the pre-merge state.
        ids = [r[0] for r in moved.join(old_keys.select(key), key, "left_semi").collect()]
        result.changed_keys = spark.createDataFrame([(i,) for i in ids], f"{key} string")

        if result.changed:
            src = align(new_rows.where("_changed"), TABLES[spec.table]).withColumn(
                "_del", F.lit(False)
            )
            src = src.unionByName(tomb.withColumn("_del", F.lit(True)))
            _apply(spark, dim_uri, spec, src)
    finally:
        new_rows.unpersist()
        tomb.unpersist()
    return result


def typed_snapshot(df: DataFrame, spec: DimSpec) -> DataFrame:
    """Key + tracked attributes cast to gold types, plus ``record_hash`` over the typed values."""
    types = {c.name: c.type for c in TABLES[spec.table]}
    return df.select(
        natural_id(F.col(spec.key), spec.kind).alias(spec.key),
        *[F.col(a).cast(types[a]).alias(a) for a in spec.attrs],
    ).withColumn("record_hash", record_hash(spec))


def _apply(spark: SparkSession, uri: str, spec: DimSpec, src: DataFrame) -> None:
    cols = _version_columns(spec)
    sk = spec.sk
    differs = (
        "NOT (t.record_hash <=> s.record_hash AND t.effective_to <=> s.effective_to "
        "AND t.is_current = s.is_current)"
    )
    (
        DeltaTable.forPath(spark, uri)
        .alias("t")
        .merge(src.alias("s"), f"t.{sk} = s.{sk}")
        .whenMatchedDelete(condition="s._del")
        .whenMatchedUpdate(condition=f"NOT s._del AND {differs}", set={c: f"s.{c}" for c in cols})
        .whenNotMatchedInsert(condition="NOT s._del", values={c: f"s.{c}" for c in cols})
        .execute()
    )


def scd2_violations(dim: DataFrame, spec: DimSpec) -> dict[str, int]:
    """Integrity of a whole SCD2 table: exactly one current version per key, no overlap, no gap."""
    key = spec.key
    w = Window.partitionBy(key).orderBy("effective_from")
    cur = (
        dim.groupBy(key)
        .agg(F.sum(F.col("is_current").cast("int")).alias("n_cur"))
        .where("n_cur != 1")
        .count()
    )
    ordered = dim.withColumn("_next_from", F.lead("effective_from").over(w))
    overlap_or_gap = ordered.where(
        F.col("_next_from").isNotNull() & ~F.col("effective_to").eqNullSafe(F.col("_next_from"))
    ).count()
    flag = dim.where(F.col("is_current") != F.col("effective_to").isNull()).count()
    dup_sk = dim.groupBy(spec.sk).count().where("count > 1").count()
    return {
        "keys_without_exactly_one_current": cur,
        "overlap_or_gap": overlap_or_gap,
        "is_current_disagrees_with_effective_to": flag,
        "duplicate_surrogate_keys": dup_sk,
    }
