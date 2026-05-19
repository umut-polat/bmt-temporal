"""Temporal workflows: pure, deterministic orchestration only."""

from bare_metal_tests.workflows.batch import BatchWorkflow
from bare_metal_tests.workflows.encapsulation import EncapsulationWorkflow
from bare_metal_tests.workflows.machine import MachineWorkflow
from bare_metal_tests.workflows.network_mesh import NetworkMeshWorkflow

ALL_WORKFLOWS = [
    BatchWorkflow,
    MachineWorkflow,
    NetworkMeshWorkflow,
    EncapsulationWorkflow,
]
