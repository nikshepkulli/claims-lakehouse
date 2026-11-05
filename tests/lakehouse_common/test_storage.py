"""Unit tests for the storage URI abstraction. Pure Python: no Spark, Java or cloud."""

from __future__ import annotations

import copy
import re
from pathlib import Path

import pytest
import yaml

from lakehouse_common.storage import (
    LAYERS,
    StorageConfigError,
    _deep_merge,
    load_config,
    load_location,
    parse_uri,
    resolve_location,
    spark_conf_for,
)

REPO_CONFIG = Path(__file__).resolve().parents[2] / "config"

AWS = {
    "storage": {
        "cloud": "aws",
        "aws": {
            "buckets": {"bronze": "lh-bronze-1", "silver": "lh-silver-1", "gold": "lh-gold-1"},
            "region": "us-east-1",
        },
    }
}
AZURE = {
    "storage": {
        "cloud": "azure",
        "azure": {
            "account": "stlake123456",
            "filesystems": {"bronze": "bronze", "silver": "silver", "gold": "gold"},
        },
    }
}
LOCAL = {"storage": {"cloud": "local", "local": {"root": "/tmp/lake"}}}


def cfg(base, **storage_updates):
    out = copy.deepcopy(base)
    out["storage"].update(storage_updates)
    return out


# ---- URI resolution -------------------------------------------------------------------


def test_file_uris():
    loc = resolve_location(LOCAL)
    assert loc.scheme == "file"
    assert loc.uri("bronze") == "file:///tmp/lake/bronze"
    assert loc.uri("silver", "claims") == "file:///tmp/lake/silver/claims"


def test_relative_local_root_resolves_against_base_dir(tmp_path):
    loc = resolve_location(
        {"storage": {"cloud": "local", "local": {"root": "lake"}}}, base_dir=tmp_path
    )
    assert loc.uri("gold") == f"file://{tmp_path.as_posix()}/lake/gold"


def test_s3a_uris_with_partition_and_prefix():
    config = copy.deepcopy(AWS)
    config["storage"]["aws"]["prefix"] = "/claims/v1/"
    loc = resolve_location(config)
    assert loc.uri("bronze") == "s3a://lh-bronze-1/claims/v1"
    assert loc.uri("silver", "claims", {"service_year": 2024, "state": "KA"}) == (
        "s3a://lh-silver-1/claims/v1/claims/service_year=2024/state=KA"
    )


def test_s3a_layer_root_has_trailing_slash_without_prefix():
    assert resolve_location(AWS).uri("gold") == "s3a://lh-gold-1/"


def test_abfss_uris():
    loc = resolve_location(AZURE)
    assert loc.uri("bronze") == "abfss://bronze@stlake123456.dfs.core.windows.net/"
    assert loc.uri("silver", "claims", {"d": "2024-01-02"}) == (
        "abfss://silver@stlake123456.dfs.core.windows.net/claims/d=2024-01-02"
    )


@pytest.mark.parametrize("base", [LOCAL, AWS, AZURE])
def test_layer_uris_cover_all_layers(base):
    uris = resolve_location(base).layer_uris()
    assert list(uris) == list(LAYERS)
    assert all(u.startswith(("file://", "s3a://", "abfss://")) for u in uris.values())


def test_layer_uris_roundtrip_through_parse_uri():
    for base in (AWS, AZURE):
        loc = resolve_location(base)
        for layer, uri in loc.layer_uris().items():
            parsed = parse_uri(uri)
            assert parsed["scheme"] == loc.scheme
            assert parsed["container"] == loc.containers[layer]


# ---- Validation -----------------------------------------------------------------------


def test_unknown_layer_table_and_partition_rejected():
    loc = resolve_location(AWS)
    with pytest.raises(StorageConfigError, match="unknown layer"):
        loc.uri("platinum")
    for bad in ("../x", "a/b", "", " t", "t..x"):
        with pytest.raises(StorageConfigError, match="invalid table"):
            loc.uri("silver", bad)
    with pytest.raises(StorageConfigError, match="invalid partition"):
        loc.uri("silver", "t", {"k": "a/b"})
    with pytest.raises(StorageConfigError, match="requires a table"):
        loc.uri("silver", None, {"k": "v"})


def test_unknown_cloud_and_missing_storage():
    with pytest.raises(StorageConfigError, match="unknown cloud"):
        resolve_location({"storage": {"cloud": "gcp"}})
    with pytest.raises(StorageConfigError, match="no 'storage'"):
        resolve_location({})


def test_azure_missing_or_bad_account():
    bad = copy.deepcopy(AZURE)
    bad["storage"]["azure"]["account"] = None
    with pytest.raises(StorageConfigError, match="account is required"):
        resolve_location(bad)
    for name in ("ab", "UPPER1", "has-dash", "x" * 25):
        bad["storage"]["azure"]["account"] = name
        with pytest.raises(StorageConfigError, match="invalid storage account"):
            resolve_location(bad)


def test_missing_layer_and_extra_layer():
    bad = copy.deepcopy(AWS)
    del bad["storage"]["aws"]["buckets"]["gold"]
    with pytest.raises(StorageConfigError, match="missing"):
        resolve_location(bad)
    bad = copy.deepcopy(AWS)
    bad["storage"]["aws"]["buckets"]["extra"] = "lh-extra"
    with pytest.raises(StorageConfigError, match="unknown layers"):
        resolve_location(bad)


def test_null_bucket_in_base_config_is_rejected():
    config = load_config(None, REPO_CONFIG)
    config["storage"]["cloud"] = "aws"  # base file leaves bucket names null on purpose
    with pytest.raises(StorageConfigError, match="missing"):
        resolve_location(config)


def test_bad_bucket_filesystem_endpoint_and_modes():
    bad = copy.deepcopy(AWS)
    bad["storage"]["aws"]["buckets"]["bronze"] = "Bad_Bucket"
    with pytest.raises(StorageConfigError, match="invalid S3 bucket"):
        resolve_location(bad)
    bad = copy.deepcopy(AWS)
    bad["storage"]["aws"]["endpoint"] = "localhost:9000"
    with pytest.raises(StorageConfigError, match="http"):
        resolve_location(bad)
    bad = copy.deepcopy(AWS)
    bad["storage"]["aws"]["credentials"] = "inline"
    with pytest.raises(StorageConfigError, match="credentials mode"):
        resolve_location(bad)
    bad = copy.deepcopy(AZURE)
    bad["storage"]["azure"]["filesystems"]["silver"] = "Silver_"
    with pytest.raises(StorageConfigError, match="invalid ADLS filesystem"):
        resolve_location(bad)
    bad = copy.deepcopy(AZURE)
    bad["storage"]["azure"]["auth"] = "account_key"
    with pytest.raises(StorageConfigError, match="auth mode"):
        resolve_location(bad)


def test_prefix_traversal_rejected():
    bad = copy.deepcopy(AWS)
    bad["storage"]["aws"]["prefix"] = "a/../b"
    with pytest.raises(StorageConfigError, match="prefix"):
        resolve_location(bad)


@pytest.mark.parametrize(
    ("uri", "hint"),
    [
        ("s3://bucket/x", "s3a"),
        ("s3n://bucket/x", "s3a"),
        ("wasbs://c@a.blob.core.windows.net/x", "abfss"),
        ("abfs://c@a.dfs.core.windows.net/x", "abfss"),
        ("gs://b/x", "not supported"),
        ("https://example.com/x", "supported schemes"),
        ("/just/a/path", "supported schemes"),
    ],
)
def test_parse_uri_rejects_bad_schemes(uri, hint):
    with pytest.raises(StorageConfigError, match=re.escape(hint)):
        parse_uri(uri)


@pytest.mark.parametrize(
    "uri",
    [
        "abfss://bronze.dfs.core.windows.net/x",  # no filesystem@
        "abfss://bronze@stlake.blob.core.windows.net/x",  # wrong endpoint
        "abfss://bronze@/x",
        "s3a:///key",
        "file://host/path",
    ],
)
def test_parse_uri_rejects_malformed(uri):
    with pytest.raises(StorageConfigError):
        parse_uri(uri)


def test_parse_uri_ok():
    assert parse_uri("abfss://gold@stlake123456.dfs.core.windows.net/t/p=1") == {
        "scheme": "abfss",
        "container": "gold",
        "account": "stlake123456",
        "path": "t/p=1",
    }
    assert parse_uri("s3a://lh-gold-1/t")["container"] == "lh-gold-1"
    assert parse_uri("file:///tmp/x")["path"] == "tmp/x"


# ---- Spark config ---------------------------------------------------------------------


def test_spark_conf_file_is_empty():
    assert spark_conf_for(resolve_location(LOCAL)) == {}


def test_spark_conf_aws_default_chain():
    conf = spark_conf_for(resolve_location(AWS))
    assert conf["spark.hadoop.fs.s3a.aws.credentials.provider"].endswith(
        "DefaultAWSCredentialsProviderChain"
    )
    assert conf["spark.hadoop.fs.s3a.endpoint.region"] == "us-east-1"
    assert "spark.hadoop.fs.s3a.endpoint" not in conf
    assert not any("access.key" in k or "secret" in k for k in conf)


def test_spark_conf_minio_uses_env_references_not_secrets():
    loc = resolve_location(
        _deep_merge(
            AWS,
            {
                "storage": {
                    "aws": {
                        "endpoint": "http://localhost:9000",
                        "path_style_access": True,
                        "credentials": "env_keys",
                    }
                }
            },
        )
    )
    conf = spark_conf_for(loc)
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://localhost:9000"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert conf["spark.hadoop.fs.s3a.access.key"] == "${env.MINIO_ROOT_USER}"
    assert conf["spark.hadoop.fs.s3a.secret.key"] == "${env.MINIO_ROOT_PASSWORD}"


def test_spark_conf_azure_managed_identity():
    conf = spark_conf_for(resolve_location(AZURE))
    host = "stlake123456.dfs.core.windows.net"
    assert conf[f"spark.hadoop.fs.azure.account.auth.type.{host}"] == "OAuth"
    assert conf[f"spark.hadoop.fs.azure.account.oauth.provider.type.{host}"].endswith(
        "MsiTokenProvider"
    )
    assert not any("secret" in k for k in conf)


def test_spark_conf_azure_service_principal_secret_is_env_reference():
    loc = resolve_location(
        _deep_merge(AZURE, {"storage": {"azure": {"auth": "service_principal"}}})
    )
    conf = spark_conf_for(loc)
    secret = [
        v
        for k, v in conf.items()
        if k.startswith("spark.hadoop.fs.azure.account.oauth2.client.secret")
    ]
    assert secret == ["${env.AZURE_CLIENT_SECRET}"]


@pytest.mark.parametrize("base", [AWS, AZURE])
def test_spark_conf_never_contains_literal_secrets(base):
    for value in spark_conf_for(resolve_location(base)).values():
        assert "${env." in value or not re.search(r"(?i)secret|password|AKIA", value)


# ---- Config files ---------------------------------------------------------------------


def test_repo_configs_parse_and_overlays_resolve():
    base = resolve_location(load_config(None, REPO_CONFIG), base_dir=Path("/tmp"))
    assert base.scheme == "file"
    local = load_location("local", REPO_CONFIG)
    assert local.scheme == "s3a"
    assert local.endpoint == "http://localhost:9000"
    assert local.uri("bronze", "claims") == "s3a://bronze/claims"
    assert load_location("aws", REPO_CONFIG).scheme == "s3a"
    az = load_location("azure", REPO_CONFIG)
    assert az.uri("gold").startswith("abfss://gold@")


def test_env_var_selects_overlay(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ENV", "azure")
    assert load_location(config_dir=REPO_CONFIG).scheme == "abfss"


def test_unknown_or_malicious_env_rejected():
    with pytest.raises(StorageConfigError, match="no overlay"):
        load_config("nope", REPO_CONFIG)
    with pytest.raises(StorageConfigError, match="invalid environment"):
        load_config("../etc", REPO_CONFIG)


def test_non_mapping_yaml_rejected(tmp_path):
    (tmp_path / "lakehouse.yaml").write_text(yaml.safe_dump(["a"]))
    with pytest.raises(StorageConfigError, match="mapping"):
        load_config(None, tmp_path)


def test_no_secret_like_values_in_committed_configs():
    for path in REPO_CONFIG.glob("lakehouse*.yaml"):
        text = path.read_text()
        assert not re.search(
            r"AKIA[0-9A-Z]{16}|AccountKey=|client_secret:\s*\S|password:\s*\S", text
        ), path
