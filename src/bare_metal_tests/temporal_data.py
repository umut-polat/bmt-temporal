"""Shared Temporal data converter that preserves Pydantic models.

By default Temporal's JSON converter round-trips through ``dict``; that
would force every workflow to re-validate models on each activity return.
``pydantic_data_converter`` keeps the model class intact across the
serialization boundary.
"""

from __future__ import annotations

from temporalio.contrib.pydantic import pydantic_data_converter

DATA_CONVERTER = pydantic_data_converter
