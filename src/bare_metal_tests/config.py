"""Configuration loading.

The system has one source of truth for tunables: ``config/default.toml``.
A second TOML may be layered on top via ``--override`` so the advisor can
tweak durations live without editing the canonical file. The loader does a
recursive merge: later files win key by key, and tables (sections) are
combined rather than replaced.
"""

from __future__ import annotations

import os
import re
import tomllib
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from bare_metal_tests.constants import DEFAULT_CONFIG_PATH, SECRETS_PATH


class FioProfile(BaseModel):
    """One fio invocation profile (e.g. seq read, rand write)."""

    model_config = ConfigDict(extra="forbid")

    name: str
    rw: str
    bs: str
    iodepth: int


class SetupConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    apt_packages: list[str]
    ssh_connect_timeout_seconds: int
    set_mesh_mtu: bool = True


class StressCpuConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    duration_seconds: int
    workers: int


class StressMemoryConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    percent_of_total: int
    duration_seconds: int
    verify: bool


class StressDiskFioConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    size: str
    runtime_seconds: int
    ioengine: str
    direct: int
    profiles: list[FioProfile]


class SysbenchCpuConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    max_prime: int
    threads: int
    duration_seconds: int


class StressConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    cpu: StressCpuConfig
    memory: StressMemoryConfig
    disk_fio: StressDiskFioConfig
    sysbench_cpu: SysbenchCpuConfig


class NetworkMeshConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    duration_seconds: int
    parallel_streams: int
    mtu: int
    verify_jumbo_frames: bool
    min_throughput_gbps: float
    max_retransmits: int


class NetworkEncapConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tunnel_types: list[str]
    duration_seconds: int
    compare_offload: bool


class NetworkConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mesh: NetworkMeshConfig
    encapsulation: NetworkEncapConfig


class ReportConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    fail_on_smart_error: bool
    fail_on_memory_hw_error: bool


class TemporalTimeouts(BaseModel):
    model_config = ConfigDict(extra="allow")
    setup: int
    hardware_probe: int
    stress_cpu: int
    stress_memory: int
    stress_disk_fio: int
    sysbench_cpu: int
    network_mesh: int
    encapsulation: int
    persist_results: int


class TemporalRetry(BaseModel):
    model_config = ConfigDict(extra="forbid")
    initial_interval_seconds: int
    backoff_coefficient: float
    maximum_attempts: int


class TemporalConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    host: str
    namespace: str
    task_queue: str
    timeouts_seconds: TemporalTimeouts
    retry: TemporalRetry


class WorkerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    max_concurrent_activities: int
    max_concurrent_workflow_tasks: int


class DatabaseConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    db_schema: str = Field(alias="schema")
    pool_min_size: int
    pool_max_size: int


class BmtConfig(BaseModel):
    """Validated, fully typed view of the merged TOML configuration."""

    model_config = ConfigDict(extra="forbid")
    setup: SetupConfig
    stress: StressConfig
    network: NetworkConfig
    report: ReportConfig
    temporal: TemporalConfig
    worker: WorkerConfig
    database: DatabaseConfig


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``override`` into ``base``.

    Dict values are merged key-by-key. Lists and scalars in ``override``
    replace those in ``base`` outright; this keeps semantics predictable
    for things like fio profile lists.
    """
    merged = dict(base)
    for key, value in override.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = _deep_merge(existing, value)
        else:
            merged[key] = value
    return merged


def _read_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as fh:
        return tomllib.load(fh)


def load_config(
    default_path: Path = DEFAULT_CONFIG_PATH,
    override_path: Path | None = None,
) -> BmtConfig:
    """Load and validate the merged configuration.

    Args:
        default_path: Base TOML, must exist.
        override_path: Optional second TOML whose keys override the base.

    Returns:
        A fully validated :class:`BmtConfig`.
    """
    data = _read_toml(default_path)
    if override_path is not None and override_path.exists():
        data = _deep_merge(data, _read_toml(override_path))
    return BmtConfig.model_validate(data)


def load_secrets(path: Path = SECRETS_PATH) -> dict[str, str]:
    """Load ``KEY=value`` pairs from ``environment/secrets.env``.

    Lines starting with ``#`` and blank lines are ignored. Values may be
    quoted with single or double quotes. ``os.environ`` overrides file
    values for any key already in the environment.
    """
    pairs: dict[str, str] = {}
    if path.exists():
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            value = value.strip().strip('"').strip("'")
            pairs[key.strip()] = value
    pairs.update(
        {
            k: v
            for k, v in os.environ.items()
            if k in pairs or k.startswith("SSH_PASSWORD_")
        }
    )
    return pairs


def ssh_password_env_key(machine_name: str) -> str:
    """Return the env-var name that holds the SSH password for a host.

    The mapping is ``host-name`` → ``SSH_PASSWORD_HOST_NAME``. Letters
    are upper-cased and any non-alphanumeric character becomes ``_`` so
    the result is always a valid POSIX variable name.
    """
    return "SSH_PASSWORD_" + re.sub(r"[^A-Za-z0-9]", "_", machine_name).upper()


def postgres_dsn(secrets: dict[str, str]) -> str:
    """Build a libpq DSN from secrets, with sensible local defaults."""
    user = secrets.get("PG_USER") or os.environ.get("USER", "postgres")
    password = secrets.get("PG_PASSWORD", "")
    host = secrets.get("PG_HOST", "localhost")
    port = secrets.get("PG_PORT", "5432")
    db = secrets.get("PG_DATABASE", "bare_metal_tests")
    auth = f"{user}:{password}@" if password else f"{user}@"
    return f"postgresql://{auth}{host}:{port}/{db}"
