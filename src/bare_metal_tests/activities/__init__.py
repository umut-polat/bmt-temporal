"""Temporal activities — every side effect (SSH, DB) lives here."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from bare_metal_tests.activities.encapsulation import ALL_ENCAP_ACTIVITIES
from bare_metal_tests.activities.hardware import ALL_HARDWARE_ACTIVITIES
from bare_metal_tests.activities.network import ALL_NETWORK_ACTIVITIES
from bare_metal_tests.activities.persist import ALL_PERSIST_ACTIVITIES
from bare_metal_tests.activities.setup import ALL_SETUP_ACTIVITIES
from bare_metal_tests.activities.stress import ALL_STRESS_ACTIVITIES

ALL_ACTIVITIES: list[Callable[..., Any]] = [
    *ALL_SETUP_ACTIVITIES,
    *ALL_HARDWARE_ACTIVITIES,
    *ALL_STRESS_ACTIVITIES,
    *ALL_NETWORK_ACTIVITIES,
    *ALL_ENCAP_ACTIVITIES,
    *ALL_PERSIST_ACTIVITIES,
]
