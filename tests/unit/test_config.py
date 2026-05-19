"""Tests for the TOML config loader and its deep-merge behaviour."""

from __future__ import annotations

from pathlib import Path

from bare_metal_tests.config import _deep_merge, load_config


def test_deep_merge_overrides_scalars_and_keeps_other_keys() -> None:
    base = {"a": 1, "b": {"c": 2, "d": 3}}
    override = {"b": {"c": 99}}
    merged = _deep_merge(base, override)
    assert merged == {"a": 1, "b": {"c": 99, "d": 3}}


def test_deep_merge_replaces_lists_outright() -> None:
    base = {"profiles": [{"name": "x"}, {"name": "y"}]}
    override = {"profiles": [{"name": "z"}]}
    merged = _deep_merge(base, override)
    assert merged == {"profiles": [{"name": "z"}]}


def test_load_default_config_has_all_sections() -> None:
    config = load_config()
    assert config.temporal.task_queue == "bare-metal-tests"
    assert config.stress.cpu.duration_seconds > 0
    assert len(config.stress.disk_fio.profiles) == 4
    assert config.network.mesh.mtu == 9000


def test_demo_override_lowers_durations(tmp_path: Path) -> None:
    here = Path(__file__).resolve().parent.parent.parent
    config = load_config(override_path=here / "config" / "demo.toml")
    # default cpu duration is 60s, demo should override to a small value.
    assert config.stress.cpu.duration_seconds <= 30
    assert config.stress.memory.percent_of_total <= 30
