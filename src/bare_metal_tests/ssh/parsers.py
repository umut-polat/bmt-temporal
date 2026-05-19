"""Plain functions that turn raw command output into dictionaries.

Each parser is a single small function. They live here, away from the
activities, so they can be unit-tested with sample fixtures without
needing SSH.
"""

from __future__ import annotations

import json
import re
from typing import Any


def parse_lscpu(text: str) -> dict[str, Any]:
    """Parse ``lscpu`` output into a flat dict of interesting fields."""
    fields: dict[str, str] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        fields[key.strip()] = value.strip()
    return {
        "model": fields.get("Model name", ""),
        "architecture": fields.get("Architecture", ""),
        "cpus": int(fields.get("CPU(s)", "0") or "0"),
        "threads_per_core": int(fields.get("Thread(s) per core", "0") or "0"),
        "cores_per_socket": int(fields.get("Core(s) per socket", "0") or "0"),
        "sockets": int(fields.get("Socket(s)", "0") or "0"),
        "numa_nodes": int(fields.get("NUMA node(s)", "0") or "0"),
    }


def parse_meminfo(text: str) -> dict[str, Any]:
    """Parse ``/proc/meminfo`` into a dict of integers (bytes)."""
    result: dict[str, int] = {}
    for line in text.splitlines():
        match = re.match(r"^(\S+):\s+(\d+)\s*(kB)?$", line)
        if match:
            value = int(match.group(2))
            if match.group(3):
                value *= 1024
            result[match.group(1)] = value
    return result


_DMIDECODE_ENTRY_RE = re.compile(r"^\s*([A-Za-z][A-Za-z0-9 /-]*?):\s*(.*)$")


def parse_dmidecode_memory(text: str) -> list[dict[str, str]]:
    """Extract populated memory module records from ``dmidecode -t memory``."""
    modules: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for raw in text.splitlines():
        if raw.startswith("Memory Device"):
            if (
                current is not None
                and current.get("Size", "")
                and "No Module" not in current.get("Size", "")
            ):
                modules.append(current)
            current = {}
            continue
        if current is None:
            continue
        match = _DMIDECODE_ENTRY_RE.match(raw)
        if match:
            current[match.group(1).strip()] = match.group(2).strip()
    if (
        current is not None
        and current.get("Size", "")
        and "No Module" not in current.get("Size", "")
    ):
        modules.append(current)
    return modules


def parse_smartctl_json(text: str) -> dict[str, Any]:
    """Extract the fields we care about from ``smartctl -a -j``."""
    data = json.loads(text)
    health = data.get("smart_status", {})
    attrs = {
        a.get("name"): a.get("raw", {}).get("value")
        for a in data.get("ata_smart_attributes", {}).get("table", [])
    }
    return {
        "model": data.get("model_name", ""),
        "serial": data.get("serial_number", ""),
        "size_bytes": data.get("user_capacity", {}).get("bytes", 0),
        "passed": bool(health.get("passed", False)),
        "reallocated_sector_ct": attrs.get("Reallocated_Sector_Ct", 0),
        "pending_sector_count": attrs.get("Current_Pending_Sector", 0),
        "uncorrectable_errors": attrs.get("Offline_Uncorrectable", 0),
    }


def parse_ethtool(text: str) -> dict[str, Any]:
    """Parse the key-value section of ``ethtool eth0``."""
    out: dict[str, str] = {}
    for line in text.splitlines():
        if ":" in line:
            key, _, value = line.strip().partition(":")
            out[key.strip()] = value.strip()
    return {
        "speed": out.get("Speed", ""),
        "duplex": out.get("Duplex", ""),
        "link_detected": out.get("Link detected", "") == "yes",
        "driver_summary": out,
    }


def parse_ip_link_json(text: str) -> list[dict[str, Any]]:
    """Parse ``ip -j link`` output."""
    return json.loads(text) if text.strip() else []


def parse_iperf3_json(text: str) -> dict[str, Any]:
    """Extract throughput and retransmits from ``iperf3 --json`` output."""
    data = json.loads(text)
    summary = data.get("end", {}).get("sum_sent", {}) or data.get("end", {}).get(
        "sum_received", {}
    )
    received = data.get("end", {}).get("sum_received", {})
    return {
        "bits_per_second": float(summary.get("bits_per_second", 0.0)),
        "gbps": float(summary.get("bits_per_second", 0.0)) / 1e9,
        "retransmits": int(summary.get("retransmits", 0)),
        "received_gbps": float(received.get("bits_per_second", 0.0)) / 1e9,
    }


def parse_lldpctl_json(text: str) -> dict[str, Any]:
    """Pass through ``lldpctl -f json`` output as a dict."""
    return json.loads(text) if text.strip() else {}


def parse_fio_json(text: str) -> dict[str, Any]:
    """Pull iops / bandwidth / latency from a fio json result."""
    data = json.loads(text)
    job = data.get("jobs", [{}])[0]
    read = job.get("read", {})
    write = job.get("write", {})
    return {
        "read_iops": float(read.get("iops", 0.0)),
        "read_bw_mb_s": float(read.get("bw", 0.0)) / 1024.0,
        "read_latency_us": float(read.get("clat_ns", {}).get("mean", 0.0)) / 1000.0,
        "write_iops": float(write.get("iops", 0.0)),
        "write_bw_mb_s": float(write.get("bw", 0.0)) / 1024.0,
        "write_latency_us": float(write.get("clat_ns", {}).get("mean", 0.0)) / 1000.0,
    }


def parse_sysbench_cpu(text: str) -> dict[str, Any]:
    """Parse the ``sysbench cpu`` summary block."""
    events = re.search(r"events per second:\s+([\d.]+)", text)
    avg_lat = re.search(r"avg:\s+([\d.]+)", text)
    total_time = re.search(r"total time:\s+([\d.]+)", text)
    return {
        "events_per_second": float(events.group(1)) if events else 0.0,
        "avg_latency_ms": float(avg_lat.group(1)) if avg_lat else 0.0,
        "total_time_s": float(total_time.group(1)) if total_time else 0.0,
    }
