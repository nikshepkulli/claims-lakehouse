"""SparkSession factory with Delta Lake (and optionally s3a/MinIO) configured."""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

HADOOP_AWS = (
    "org.apache.hadoop:hadoop-aws:3.3.4"  # matches the Hadoop client bundled in pyspark 3.5.x
)


def build_spark(
    app_name: str = "bronze-ingest",
    master: str | None = None,
    bronze_root: str = "",
    conf: dict[str, str] | None = None,
) -> SparkSession:
    """Create (or reuse) a Delta-enabled session.

    ``bronze_root`` starting with ``s3a://`` adds hadoop-aws and the S3A settings read from the
    environment: ``LAKEHOUSE_S3_ENDPOINT`` (for MinIO, e.g. ``http://localhost:9000``),
    ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` (falls back to ``MINIO_ROOT_USER`` /
    ``MINIO_ROOT_PASSWORD``). Real AWS needs none of the endpoint settings.
    """
    b = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
    )
    if master:
        b = b.master(master)
    for k, v in (conf or {}).items():
        b = b.config(k, v)
    packages: list[str] = []
    if bronze_root.startswith(("s3a://", "s3://")):
        packages.append(HADOOP_AWS)
        endpoint = os.environ.get("LAKEHOUSE_S3_ENDPOINT")
        if endpoint:
            b = b.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            b = b.config("spark.hadoop.fs.s3a.path.style.access", "true")
            b = b.config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(endpoint.startswith("https")).lower(),
            )
        key = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("MINIO_ROOT_USER")
        secret = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get("MINIO_ROOT_PASSWORD")
        if key and secret:
            b = b.config("spark.hadoop.fs.s3a.access.key", key)
            b = b.config("spark.hadoop.fs.s3a.secret.key", secret)
    return configure_spark_with_delta_pip(b, extra_packages=packages).getOrCreate()
