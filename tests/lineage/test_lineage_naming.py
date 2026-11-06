import os

import pytest

from lakehouse.lineage import dataset_ref, table_ref
from lakehouse.lineage.events import run_uuid
from lakehouse.lineage.naming import NamingError


def test_local_path_is_absolute_file_dataset(tmp_path):
    r = dataset_ref(str(tmp_path / "silver" / "claims"))
    assert r.namespace == "file://localhost"
    assert r.name == str(tmp_path / "silver" / "claims")


def test_relative_path_is_resolved_so_jobs_agree(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert dataset_ref("data/silver/claims") == dataset_ref(str(tmp_path / "data/silver/claims"))


def test_file_uri_equals_plain_path(tmp_path):
    p = tmp_path / "gold" / "fact_claim"
    assert dataset_ref(f"file://{p}") == dataset_ref(str(p))


def test_file_host_override(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_LINEAGE_FILE_HOST", "ci-runner")
    assert dataset_ref("/x/y").namespace == "file://ci-runner"


@pytest.mark.parametrize("scheme", ["s3a", "s3"])
def test_s3_bucket_is_namespace_key_is_name(scheme):
    r = dataset_ref(f"{scheme}://lake-silver/claims-prod/claims/")
    assert (r.namespace, r.name) == ("s3://lake-silver", "claims-prod/claims")


def test_abfss_container_account_namespace():
    uri = "abfss://silver@claimsstore.dfs.core.windows.net/lakehouse/claims"
    r = dataset_ref(uri)
    assert r.namespace == "abfss://silver@claimsstore.dfs.core.windows.net"
    assert r.name == "/lakehouse/claims"


def test_table_ref_joins_root_and_table_for_every_scheme():
    assert table_ref("s3a://b/p/", "claims").name == "p/claims"
    assert table_ref("s3a://b", "claims").name == "claims"  # bucket root: no leading slash
    assert table_ref("abfss://c@acct.dfs.core.windows.net", "t").name == "/t"
    assert table_ref("/data/gold", "dim_member").name == "/data/gold/dim_member"


def test_same_table_has_same_identity_whichever_scheme_spelling():
    assert dataset_ref("s3a://b/x/claims") == dataset_ref("s3://b/x/claims")


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "  ",
        "s3a:///no-bucket",
        "abfss://nocontainer.dfs.core.windows.net/x",
        "abfss://c@acct.blob.core.windows.net/x",
        "ftp://host/x",
        "file://otherhost/x",
    ],
)
def test_bad_uris_raise(bad):
    with pytest.raises(NamingError):
        dataset_ref(bad)


def test_run_uuid_keeps_uuid4_hex_and_is_deterministic_otherwise():
    h = "0123456789abcdef0123456789abcdef"
    assert run_uuid(h) == "01234567-89ab-cdef-0123-456789abcdef"
    assert run_uuid("r1-silver") == run_uuid("r1-silver") != run_uuid("r2-silver")
    assert len(run_uuid("r1-silver")) == 36


def test_namespace_env(monkeypatch):
    from lakehouse.lineage.naming import job_namespace

    monkeypatch.delenv("LAKEHOUSE_LINEAGE_NAMESPACE", raising=False)
    assert job_namespace() == "claims-lakehouse"
    monkeypatch.setenv("LAKEHOUSE_LINEAGE_NAMESPACE", "claims-prod")
    assert job_namespace() == "claims-prod"
    assert os.environ["LAKEHOUSE_LINEAGE_NAMESPACE"] == "claims-prod"
