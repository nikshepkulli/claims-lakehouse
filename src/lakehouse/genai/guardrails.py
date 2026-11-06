"""SQL guardrails: a real parser (sqlglot, DuckDB dialect), not regexes.

Every rule appends a ``Violation(rule, reason)``; a query is allowed only with zero violations,
and the reasons are returned to the caller so a rejection is always explainable. The returned
``sql`` is the *rewritten* statement (qualified columns, expanded ``*`` over allowed columns,
LIMIT injected) and is the only text that is ever executed.

Rules, in order: length; comments; parse; one statement; SELECT-only; no DML/DDL/INTO anywhere;
no recursive CTE; table allow-list (no table functions, no schema-qualified names); function
allow-list; column allow-list (PII columns named in the reason); join discipline (equi-join
required, no CROSS/comma joins, join cap); static cost cap; mandatory LIMIT.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.errors import OptimizeError, ParseError, TokenError
from sqlglot.optimizer.qualify import qualify

from .catalog import Catalog
from .config import GenAIConfig

DIALECT = "duckdb"

# Functions the model may use. Everything else is rejected, which also removes file readers
# (read_parquet, read_csv, glob), environment/config probes (getenv, current_setting), random
# and system functions without having to enumerate every dangerous name.
ALLOWED_FUNCTIONS = frozenset(
    """
    count sum avg min max round ceil floor abs coalesce nullif greatest least cast try_cast
    case if extract date_trunc year month day quarter dayofweek week current_date lower upper
    length substring substr trim concat row_number rank dense_rank ntile lag lead first_value
    last_value stddev stddev_samp stddev_pop variance median quantile_cont quantile_disc
    approx_count_distinct count_if sum_if bool_or bool_and any_value mode strftime date_diff
    datediff date_add date_sub to_date mod power sqrt ln log exp sign string_agg
    """.split()
)

_BLOCKED_NODES = tuple(
    getattr(exp, n)
    for n in (
        "Insert",
        "Update",
        "Delete",
        "Create",
        "Drop",
        "Alter",
        "Command",
        "Copy",
        "Merge",
        "Set",
        "Pragma",
        "Use",
        "Attach",
        "Detach",
        "Transaction",
        "Commit",
        "Rollback",
        "Into",
        "Describe",
        "Show",
        "Analyze",
        "TruncateTable",
        "LoadData",
    )
    if hasattr(exp, n)
)
_SET_OPS = tuple(getattr(exp, n) for n in ("Union", "Intersect", "Except") if hasattr(exp, n))

TABLE_WEIGHT = {"fact_claim_line": 10, "fact_claim": 5}


@dataclass(frozen=True)
class Violation:
    rule: str
    reason: str


@dataclass
class GuardDecision:
    allowed: bool
    sql: str | None = None  # rewritten, safe-to-run SQL (None when rejected)
    violations: list[Violation] = field(default_factory=list)
    limit_applied: str | None = None  # "injected" | "clamped" | None
    cost: int = 0
    tables: list[str] = field(default_factory=list)

    @property
    def reasons(self) -> list[str]:
        return [f"[{v.rule}] {v.reason}" for v in self.violations]

    @property
    def rules(self) -> list[str]:
        return sorted({v.rule for v in self.violations})


def _reject(*violations: Violation) -> GuardDecision:
    return GuardDecision(False, None, list(violations))


def _function_name(node: exp.Expression) -> str:
    if isinstance(node, exp.Anonymous):
        return str(node.name).lower()
    return node.sql_name().lower()


def estimate_cost(root: exp.Expression, joins: int) -> int:
    """Static, deterministic plan-cost proxy: table-size weights x (1 + joins), plus windows.

    A heuristic, not a planner estimate. Hard caps that are real: the row LIMIT, the join cap
    and the wall-clock timeout. Documented as such in docs/genai.md.
    """
    weight = sum(TABLE_WEIGHT.get(t.name.lower(), 1) for t in root.find_all(exp.Table))
    windows = sum(1 for _ in root.find_all(exp.Window))
    return weight * (1 + joins) + 15 * windows


def _check_joins(root: exp.Expression, cfg: GenAIConfig) -> list[Violation]:
    out: list[Violation] = []
    joins = list(root.find_all(exp.Join))
    if len(joins) > cfg.max_joins:
        out.append(Violation("join_cap", f"{len(joins)} joins exceed the cap of {cfg.max_joins}"))
    for j in joins:
        kind = (j.args.get("kind") or "").upper()
        if kind == "CROSS" or (not j.args.get("on") and not j.args.get("using")):
            out.append(
                Violation(
                    "cross_join",
                    "cross/comma joins (or joins without ON/USING) multiply rows and are blocked",
                )
            )
            continue
        on = j.args.get("on")
        if on is not None:
            eqs = [
                e
                for e in on.find_all(exp.EQ)
                if isinstance(e.left, exp.Column)
                and isinstance(e.right, exp.Column)
                and e.left.table != e.right.table
            ]
            if not eqs:
                out.append(
                    Violation(
                        "cross_join",
                        "join ON must equate columns of the two tables (non-key joins explode)",
                    )
                )
    return out


def _diagnose_columns(root: exp.Expression, catalog: Catalog, msg: str) -> list[Violation]:
    denied = catalog.denied_column_names()
    names = {c.name.lower() for c in root.find_all(exp.Column)}
    hits = sorted(n for n in names if n in denied)
    if hits:
        return [
            Violation("pii_column", f"column not available: {denied[n]}; role={catalog.role}")
            for n in hits
        ]
    return [
        Violation("unknown_column", f"column cannot be resolved against allowed columns: {msg}")
    ]


def check_sql(sql: str, catalog: Catalog, cfg: GenAIConfig | None = None) -> GuardDecision:
    cfg = cfg or GenAIConfig(role=catalog.role)
    if not sql or not sql.strip():
        return _reject(Violation("empty", "no SQL to validate"))
    if len(sql) > cfg.max_sql_chars:
        return _reject(Violation("too_long", f"SQL longer than {cfg.max_sql_chars} characters"))

    try:
        tokens = sqlglot.tokenize(sql, dialect=DIALECT)
        statements = [s for s in sqlglot.parse(sql, dialect=DIALECT) if s is not None]
    except (ParseError, TokenError) as exc:
        return _reject(Violation("parse_error", f"SQL does not parse: {str(exc)[:120]}"))
    if any(t.comments for t in tokens):
        return _reject(
            Violation("comment", "SQL comments are blocked (they can smuggle text or hide clauses)")
        )
    if len(statements) != 1:
        return _reject(Violation("multi_statement", f"{len(statements)} statements; exactly 1"))
    root = statements[0]

    is_query = isinstance(root, (exp.Select, *_SET_OPS))
    if not is_query:
        return _reject(
            Violation("not_select", f"only read-only SELECT is allowed, got {root.key.upper()}")
        )
    violations: list[Violation] = []
    for node in root.walk():
        if isinstance(node, _BLOCKED_NODES):
            violations.append(
                Violation("not_select", f"{node.key.upper()} is not allowed inside a query")
            )
            break
    with_ = root.args.get("with_") or root.args.get("with")
    if with_ is not None and with_.args.get("recursive"):
        violations.append(Violation("recursive_cte", "WITH RECURSIVE can run unbounded"))

    cte_names = {c.alias.lower() for c in root.find_all(exp.CTE)}
    used: list[str] = []
    for t in root.find_all(exp.Table):
        name = t.name.lower()
        if not isinstance(t.this, exp.Identifier):
            violations.append(Violation("table_function", "table functions are blocked"))
        elif t.args.get("db") or t.args.get("catalog"):
            violations.append(
                Violation("table_not_allowed", f"schema-qualified table {t.sql()} is not allowed")
            )
        elif name in cte_names:
            continue
        elif name not in catalog.tables:
            violations.append(
                Violation(
                    "table_not_allowed",
                    f"table {name!r} is not in the allowed list for role {catalog.role}: "
                    f"{sorted(catalog.tables)}",
                )
            )
        else:
            used.append(name)

    for fn in root.find_all(exp.Func):
        name = _function_name(fn)
        if name not in ALLOWED_FUNCTIONS:
            violations.append(
                Violation("function_not_allowed", f"function {name}() is not allowed")
            )

    if violations:
        return _reject(*dict.fromkeys(violations))

    schema = {t: {c: "VARCHAR" for c in v.allowed} for t, v in catalog.tables.items()}
    try:
        qualified = qualify(
            root.copy(),
            schema=schema,
            dialect=DIALECT,
            validate_qualify_columns=True,
            expand_stars=True,
            identify=False,
            quote_identifiers=False,
        )
    except OptimizeError as exc:
        return _reject(*_diagnose_columns(root, catalog, str(exc)[:120]))
    root = qualified

    violations += _check_joins(root, cfg)
    join_count = len(list(root.find_all(exp.Join)))
    cost = estimate_cost(root, join_count)
    if cost > cfg.max_cost_units:
        violations.append(
            Violation("cost_cap", f"estimated cost {cost} exceeds the cap of {cfg.max_cost_units}")
        )
    if violations:
        return _reject(*dict.fromkeys(violations))

    applied = None
    limit = root.args.get("limit")
    lit = limit.expression if limit is not None else None
    if limit is None:
        applied = "injected"
        root.limit(cfg.max_rows, copy=False)
    elif not (isinstance(lit, exp.Literal) and lit.is_int) or int(lit.name) > cfg.max_rows:
        applied = "clamped"
        root.limit(cfg.max_rows, copy=False)
    final = root.sql(dialect=DIALECT, comments=False)
    return GuardDecision(True, final, [], applied, cost, sorted(set(used)))
