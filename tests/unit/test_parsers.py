"""Tests for the lightweight output parsers in ``ssh.parsers``."""

from __future__ import annotations

import json

from bare_metal_tests.ssh.parsers import (
    parse_fio_json,
    parse_iperf3_json,
    parse_lscpu,
    parse_meminfo,
    parse_smartctl_json,
    parse_sysbench_cpu,
)

LSCPU_SAMPLE = """\
Architecture:                            x86_64
CPU(s):                                  80
Model name:                              Intel(R) Xeon(R) Gold 6138 CPU @ 2.00GHz
Thread(s) per core:                      2
Core(s) per socket:                      20
Socket(s):                               2
NUMA node(s):                            2
"""


def test_parse_lscpu_extracts_topology() -> None:
    parsed = parse_lscpu(LSCPU_SAMPLE)
    assert parsed["model"].startswith("Intel(R) Xeon(R) Gold 6138")
    assert parsed["cpus"] == 80
    assert parsed["sockets"] == 2
    assert parsed["cores_per_socket"] == 20
    assert parsed["numa_nodes"] == 2


def test_parse_meminfo_converts_to_bytes() -> None:
    sample = "MemTotal:       1610612736 kB\nMemFree:          2048 kB\n"
    parsed = parse_meminfo(sample)
    assert parsed["MemTotal"] == 1610612736 * 1024


def test_parse_iperf3_json_extracts_throughput() -> None:
    sample = {
        "end": {
            "sum_sent": {"bits_per_second": 20_000_000_000, "retransmits": 5},
            "sum_received": {"bits_per_second": 19_500_000_000},
        }
    }
    parsed = parse_iperf3_json(json.dumps(sample))
    assert parsed["gbps"] == 20.0
    assert parsed["retransmits"] == 5
    assert parsed["received_gbps"] == 19.5


def test_parse_fio_json_reads_iops_and_bandwidth() -> None:
    sample = {
        "jobs": [
            {
                "read": {"iops": 1234.5, "bw": 5000, "clat_ns": {"mean": 100_000.0}},
                "write": {"iops": 0.0, "bw": 0.0, "clat_ns": {"mean": 0.0}},
            }
        ]
    }
    parsed = parse_fio_json(json.dumps(sample))
    assert parsed["read_iops"] == 1234.5
    assert parsed["read_bw_mb_s"] == 5000 / 1024.0
    assert parsed["read_latency_us"] == 100.0


def test_parse_smartctl_json_healthy_disk() -> None:
    sample = {
        "model_name": "SAMSUNG MZ7L3480HCHQ-00A07",
        "serial_number": "S664NN0X417584",
        "user_capacity": {"bytes": 480103981056},
        "smart_status": {"passed": True},
        "ata_smart_attributes": {
            "table": [
                {"name": "Reallocated_Sector_Ct", "raw": {"value": 0}},
                {"name": "Current_Pending_Sector", "raw": {"value": 0}},
                {"name": "Offline_Uncorrectable", "raw": {"value": 0}},
            ]
        },
    }
    parsed = parse_smartctl_json(json.dumps(sample))
    assert parsed["passed"] is True
    assert parsed["model"] == "SAMSUNG MZ7L3480HCHQ-00A07"
    assert parsed["size_bytes"] == 480103981056
    assert parsed["reallocated_sector_ct"] == 0
    assert parsed["pending_sector_count"] == 0
    assert parsed["uncorrectable_errors"] == 0


def test_parse_smartctl_json_failing_disk() -> None:
    """A failing SMART self-test must propagate as ``passed = False``."""
    sample = {
        "model_name": "FAULTY_DISK_X1",
        "serial_number": "ZZZ001",
        "user_capacity": {"bytes": 1024},
        "smart_status": {"passed": False},
        "ata_smart_attributes": {
            "table": [
                {"name": "Reallocated_Sector_Ct", "raw": {"value": 137}},
                {"name": "Current_Pending_Sector", "raw": {"value": 42}},
                {"name": "Offline_Uncorrectable", "raw": {"value": 9}},
            ]
        },
    }
    parsed = parse_smartctl_json(json.dumps(sample))
    assert parsed["passed"] is False
    assert parsed["reallocated_sector_ct"] == 137
    assert parsed["pending_sector_count"] == 42
    assert parsed["uncorrectable_errors"] == 9


def test_parse_sysbench_cpu_summary() -> None:
    sample = """\
CPU speed:
    events per second:    27.34

Latency (ms):
         min:                                    1234.56
         avg:                                    5678.90
         max:                                    9999.99
         95th percentile:                        7777.77
         sum:                                    99999.99

General statistics:
    total time:                          10.0034s
    total number of events:              273
"""
    parsed = parse_sysbench_cpu(sample)
    assert parsed["events_per_second"] == 27.34
    assert parsed["avg_latency_ms"] == 5678.90
    assert parsed["total_time_s"] == 10.0034
