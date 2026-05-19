"""Constants shared across modules.

Keep names here rather than scattering string literals in code.
"""

from __future__ import annotations

from pathlib import Path

PACKAGE_ROOT: Path = Path(__file__).resolve().parent
PROJECT_ROOT: Path = PACKAGE_ROOT.parent.parent

DEFAULT_CONFIG_PATH: Path = PROJECT_ROOT / "config" / "default.toml"
DEMO_CONFIG_PATH: Path = PROJECT_ROOT / "config" / "demo.toml"
MACHINES_PATH: Path = PROJECT_ROOT / "environment" / "machines.json"
SECRETS_PATH: Path = PROJECT_ROOT / "environment" / "secrets.env"

DB_SCHEMA: str = "bmt"

PHASE_SSH_CHECK: str = "ssh_check"
PHASE_APT_INSTALL: str = "apt_install"

PHASE_HW_CPU: str = "hw_cpu"
PHASE_HW_MEMORY: str = "hw_memory_ecc"
PHASE_HW_DISKS: str = "hw_disks_smart"
PHASE_HW_NICS: str = "hw_nics"
PHASE_HW_NUMA: str = "hw_numa"
PHASE_HW_LLDP: str = "hw_lldp"
PHASE_HW_BIOS: str = "hw_bios_firmware"

PHASE_STRESS_CPU: str = "stress_cpu"
PHASE_STRESS_MEMORY: str = "stress_memory"
PHASE_FIO_SEQ_READ: str = "fio_seq_read"
PHASE_FIO_SEQ_WRITE: str = "fio_seq_write"
PHASE_FIO_RAND_READ: str = "fio_rand_read"
PHASE_FIO_RAND_WRITE: str = "fio_rand_write"
PHASE_SYSBENCH_CPU: str = "sysbench_cpu"

PHASE_IPERF3_PAIR: str = "iperf3_pair"
PHASE_VERIFY_JUMBO: str = "verify_jumbo_frames"
PHASE_VERIFY_MTU: str = "verify_mtu"

PHASE_VXLAN_SETUP: str = "vxlan_setup"
PHASE_VXLAN_IPERF: str = "vxlan_iperf3"
PHASE_VXLAN_TEARDOWN: str = "vxlan_teardown"

STATUS_RUNNING: str = "running"
STATUS_PASSED: str = "passed"
STATUS_FAILED: str = "failed"
STATUS_SKIPPED: str = "skipped"

AUTH_PASSWORD: str = "password"
AUTH_KEY: str = "ssh_key"
