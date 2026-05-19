"""Integration test fixtures: real SSH and PostgreSQL.

These tests connect to the actual three test hosts described in
``environment/machines.json``. They are slow and require the local
Temporal dev server plus PostgreSQL to be up. Skip the file unless
``BMT_INTEGRATION=1`` is set.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("BMT_INTEGRATION") != "1",
    reason="set BMT_INTEGRATION=1 to run integration tests",
)
