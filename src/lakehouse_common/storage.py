"""Storage URI abstraction: one config, three schemes (ADR-0003).

Pipeline code asks for ``location.uri("silver", "claims", {"service_year": "2024"})`` and
never builds a provider path itself. This module is pure Python: it imports no Spark, no
cloud SDK and makes no network calls, so it is fully unit-testable.

Schemes
-------
* ``file://``   local filesystem (tests, laptop)
* ``s3a://``    S3, or any S3-compatible store (MinIO) when ``endpoint`` is set
* ``abfss://``  ADLS Gen2: ``abfss://<filesystem>@<account>.dfs.core.windows.net/``

Secrets are never read into, or returned from, this module. ``spark_conf_for`` emits Hadoop
``${env.VAR}`` references (expanded by Hadoop at runtime) or relies on the ambient identity
(AWS default credential chain, Azure managed identity).

Not verified against a live cluster: the Spark settings follow the hadoop-aws /
hadoop-azure 3.3.x documentation (the version bundled with Spark 3.5) and are covered by
unit tests of their shape only.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

LAYERS: tuple[str, ...] = ("bronze", "silver", "gold")
CLOUDS: tuple[str, ...] = ("local", "aws", "azure")
SCHEMES: tuple[str, ...] = ("file", "s3a", "abfss")

_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_FILESYSTEM_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$")
_ACCOUNT_RE = re.compile(r"^[a-z0-9]{3,24}$")
_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.=-]*$")
_ABFSS_HOST_RE = re.compile(r"^([a-z0-9]{3,24})\.dfs\.core\.windows\.net$")
_ENV_NAME = "LAKEHOUSE_ENV"
_DEFAULT_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"

# Common mistakes mapped to the right scheme, for actionable errors.
_SCHEME_HINTS = {
    "s3": "use s3a:// (the Hadoop connector scheme)",
    "s3n": "use s3a:// (s3n is removed)",
    "abfs": "use abfss:// (TLS)",
    "wasb": "use abfss:// (wasb is the legacy blob driver)",
    "wasbs": "use abfss:// (wasbs is the legacy blob driver)",
    "adl": "use abfss:// (ADLS Gen1 is retired)",
    "gs": "GCS is not supported",
}


class StorageConfigError(ValueError):
    """Raised for any invalid storage configuration, URI or path component."""


@dataclass(frozen=True)
class LakehouseLocation:
    """A resolved storage target. Build one with :func:`resolve_location`."""

    cloud: str
    scheme: str
    # layer -> bucket / filesystem name. Empty for ``file://`` (layers are directories).
    containers: Mapping[str, str]
    prefix: str = ""
    root: str | None = None  # file:// only: absolute directory
    account: str | None = None  # abfss:// only
    region: str | None = None  # s3a:// only
    endpoint: str | None = None  # s3a:// only; set => S3-compatible (MinIO)
    path_style_access: bool = False
    credentials: str = "default_chain"  # s3a: default_chain | env_keys
    auth: str = "managed_identity"  # abfss: managed_identity | service_principal

    def uri(
        self,
        layer: str,
        table: str | None = None,
        partition: Mapping[str, str | int] | None = None,
    ) -> str:
        """URI for a layer, a table in it, or one hive-style partition of a table."""
        _check_layer(layer)
        if partition and table is None:
            raise StorageConfigError("partition requires a table")
        segments = [s for s in self.prefix.split("/") if s]
        if table is not None:
            segments.append(_segment(table, "table"))
        for key, value in (partition or {}).items():
            segments.append(
                f"{_segment(key, 'partition key')}={_segment(str(value), 'partition value')}"
            )
        path = "/".join(segments)

        if self.scheme == "file":
            assert self.root is not None
            base = f"file://{self.root.rstrip('/')}/{layer}"
            return f"{base}/{path}" if path else base
        if self.scheme == "s3a":
            base = f"s3a://{self.containers[layer]}"
        else:
            base = f"abfss://{self.containers[layer]}@{self.account}.dfs.core.windows.net"
        return f"{base}/{path}" if path else f"{base}/"

    def layer_uris(self) -> dict[str, str]:
        """Layer roots, the same shape as the Terraform ``layer_uris`` output."""
        return {layer: self.uri(layer) for layer in LAYERS}


def _check_layer(layer: str) -> None:
    if layer not in LAYERS:
        raise StorageConfigError(f"unknown layer {layer!r}; expected one of {list(LAYERS)}")


def _segment(value: str, what: str) -> str:
    # Allows nested tables ("a/b") only through an explicit prefix, never via user input.
    if not isinstance(value, str) or not _SEGMENT_RE.match(value) or ".." in value:
        raise StorageConfigError(f"invalid {what} {value!r}: use letters, digits, _ - . =")
    return value


def _section(cfg: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = cfg.get(key) or {}
    if not isinstance(value, Mapping):
        raise StorageConfigError(f"storage.{key} must be a mapping")
    return value


def _layer_names(section: Mapping[str, Any], key: str, where: str) -> dict[str, str]:
    raw = section.get(key) or {}
    missing = [layer for layer in LAYERS if not raw.get(layer)]
    if missing:
        raise StorageConfigError(f"{where}.{key} is missing {missing}")
    extra = sorted(set(raw) - set(LAYERS))
    if extra:
        raise StorageConfigError(f"{where}.{key} has unknown layers {extra}")
    return {layer: str(raw[layer]) for layer in LAYERS}


def _prefix(section: Mapping[str, Any]) -> str:
    prefix = str(section.get("prefix") or "").strip("/")
    for part in filter(None, prefix.split("/")):
        _segment(part, "prefix segment")
    return prefix


def resolve_location(
    config: Mapping[str, Any], *, base_dir: Path | None = None
) -> LakehouseLocation:
    """Validate the ``storage`` block of a loaded config and build a location."""
    storage = config.get("storage")
    if not isinstance(storage, Mapping):
        raise StorageConfigError("config has no 'storage' mapping")
    cloud = storage.get("cloud")
    if cloud not in CLOUDS:
        raise StorageConfigError(f"unknown cloud {cloud!r}; expected one of {list(CLOUDS)}")

    if cloud == "local":
        section = _section(storage, "local")
        raw_root = section.get("root")
        if not raw_root:
            raise StorageConfigError("storage.local.root is required for cloud=local")
        root = Path(str(raw_root))
        if not root.is_absolute():
            root = (base_dir or Path.cwd()) / root
        return LakehouseLocation(
            cloud="local",
            scheme="file",
            containers={},
            prefix=_prefix(section),
            root=Path(os.path.abspath(root)).as_posix(),
        )

    if cloud == "aws":
        section = _section(storage, "aws")
        buckets = _layer_names(section, "buckets", "storage.aws")
        for layer, name in buckets.items():
            if not _BUCKET_RE.match(name):
                raise StorageConfigError(f"invalid S3 bucket name for {layer}: {name!r}")
        endpoint = section.get("endpoint")
        if endpoint:
            parsed = urlparse(str(endpoint))
            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                raise StorageConfigError(f"endpoint must be an http(s) URL, got {endpoint!r}")
        credentials = section.get("credentials", "default_chain")
        if credentials not in ("default_chain", "env_keys"):
            raise StorageConfigError(f"unknown aws credentials mode {credentials!r}")
        return LakehouseLocation(
            cloud="aws",
            scheme="s3a",
            containers=buckets,
            prefix=_prefix(section),
            region=str(section.get("region") or "") or None,
            endpoint=str(endpoint) if endpoint else None,
            path_style_access=bool(section.get("path_style_access", False)),
            credentials=credentials,
        )

    section = _section(storage, "azure")
    account = section.get("account")
    if not account:
        raise StorageConfigError("storage.azure.account is required for cloud=azure")
    if not _ACCOUNT_RE.match(str(account)):
        raise StorageConfigError(
            f"invalid storage account name {account!r} (3-24 lowercase alphanumerics)"
        )
    filesystems = _layer_names(section, "filesystems", "storage.azure")
    for layer, name in filesystems.items():
        if not _FILESYSTEM_RE.match(name):
            raise StorageConfigError(f"invalid ADLS filesystem name for {layer}: {name!r}")
    auth = section.get("auth", "managed_identity")
    if auth not in ("managed_identity", "service_principal"):
        raise StorageConfigError(f"unknown azure auth mode {auth!r}")
    return LakehouseLocation(
        cloud="azure",
        scheme="abfss",
        containers=filesystems,
        prefix=_prefix(section),
        account=str(account),
        auth=auth,
    )


def _deep_merge(base: dict[str, Any], over: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in over.items():
        if isinstance(value, Mapping) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise StorageConfigError(f"{path} must contain a YAML mapping")
    return data


def load_config(env: str | None = None, config_dir: Path | None = None) -> dict[str, Any]:
    """Load ``lakehouse.yaml`` and deep-merge ``lakehouse.<env>.yaml`` over it.

    ``env`` defaults to ``$LAKEHOUSE_ENV``; with neither set only the base file is used.
    """
    directory = config_dir or _DEFAULT_CONFIG_DIR
    config = _read_yaml(directory / "lakehouse.yaml")
    env = env or os.environ.get(_ENV_NAME)
    if env:
        if not re.fullmatch(r"[a-z][a-z0-9_-]*", env):
            raise StorageConfigError(f"invalid environment name {env!r}")
        overlay = directory / f"lakehouse.{env}.yaml"
        if not overlay.is_file():
            raise StorageConfigError(f"no overlay for environment {env!r}: {overlay}")
        config = _deep_merge(config, _read_yaml(overlay))
    return config


def load_location(env: str | None = None, config_dir: Path | None = None) -> LakehouseLocation:
    """Load config (base + overlay) and resolve it in one call."""
    return resolve_location(load_config(env, config_dir))


def parse_uri(uri: str) -> dict[str, str | None]:
    """Split a lakehouse URI into ``scheme``, ``container``, ``account`` and ``path``.

    Rejects schemes the platform does not support (``s3://``, ``wasbs://``, ...).
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme
    if scheme not in SCHEMES:
        hint = _SCHEME_HINTS.get(scheme, f"supported schemes: {list(SCHEMES)}")
        raise StorageConfigError(f"unsupported scheme {scheme!r} in {uri!r}: {hint}")
    path = parsed.path.lstrip("/")
    if scheme == "file":
        if parsed.netloc:
            raise StorageConfigError(f"file URI must be file:///absolute/path, got {uri!r}")
        return {"scheme": scheme, "container": None, "account": None, "path": path}
    if scheme == "s3a":
        if not parsed.netloc or not _BUCKET_RE.match(parsed.netloc):
            raise StorageConfigError(f"missing or invalid bucket in {uri!r}")
        return {"scheme": scheme, "container": parsed.netloc, "account": None, "path": path}
    host = _ABFSS_HOST_RE.match(parsed.hostname or "")
    if not parsed.username or not host:
        raise StorageConfigError(
            f"abfss URI must be abfss://<filesystem>@<account>.dfs.core.windows.net/, got {uri!r}"
        )
    if not _FILESYSTEM_RE.match(parsed.username):
        raise StorageConfigError(f"invalid ADLS filesystem name in {uri!r}")
    return {"scheme": scheme, "container": parsed.username, "account": host.group(1), "path": path}


def spark_conf_for(location: LakehouseLocation) -> dict[str, str]:
    """Spark settings needed to read/write ``location``. Contains no secret values.

    Secrets appear only as Hadoop ``${env.NAME}`` references that Hadoop expands from the
    executor environment, or are absent (default credential chain, managed identity).
    """
    if location.scheme == "file":
        return {}
    if location.scheme == "s3a":
        conf = {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": str(location.path_style_access).lower(),
        }
        if location.region:
            conf["spark.hadoop.fs.s3a.endpoint.region"] = location.region
        if location.endpoint:
            conf["spark.hadoop.fs.s3a.endpoint"] = location.endpoint
            conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] = str(
                location.endpoint.startswith("https://")
            ).lower()
        if location.credentials == "env_keys":
            conf["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            )
            conf["spark.hadoop.fs.s3a.access.key"] = "${env.MINIO_ROOT_USER}"
            conf["spark.hadoop.fs.s3a.secret.key"] = "${env.MINIO_ROOT_PASSWORD}"
        else:
            conf["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
            )
        return conf

    host = f"{location.account}.dfs.core.windows.net"
    prefix = "spark.hadoop.fs.azure.account"
    conf = {
        "spark.hadoop.fs.abfss.impl": "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
        f"{prefix}.auth.type.{host}": "OAuth",
    }
    if location.auth == "managed_identity":
        conf[f"{prefix}.oauth.provider.type.{host}"] = (
            "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
        )
        conf[f"{prefix}.oauth2.msi.tenant.{host}"] = "${env.AZURE_TENANT_ID}"
        # Only needed for user-assigned identities; empty/unset is treated as system-assigned.
        conf[f"{prefix}.oauth2.client.id.{host}"] = "${env.AZURE_CLIENT_ID}"
    else:
        conf[f"{prefix}.oauth.provider.type.{host}"] = (
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        )
        conf[f"{prefix}.oauth2.client.id.{host}"] = "${env.AZURE_CLIENT_ID}"
        conf[f"{prefix}.oauth2.client.secret.{host}"] = "${env.AZURE_CLIENT_SECRET}"
        conf[f"{prefix}.oauth2.client.endpoint.{host}"] = (
            "https://login.microsoftonline.com/${env.AZURE_TENANT_ID}/oauth2/token"
        )
    return conf
