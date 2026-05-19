# bmt-temporal

A pre-deployment validation suite for bare metal hosts. Runs hardware
discovery, stress, network mesh and VXLAN encapsulation tests over SSH,
orchestrated by Temporal, with results stored in PostgreSQL.

The repository contains four Temporal workflows and around twenty-six
activities. Workflows decide what runs and in what order; activities do
the work over SSH and write results to PostgreSQL. Test parameters live
in TOML so durations, fio profiles, retry counts and so on can be
changed without touching code.

## Requirements

- Python 3.12+
- Docker (for the local Temporal + PostgreSQL stack)
- SSH reachability and sudo access on each target host
- The host running the worker needs network access to all target hosts

## Layout

```
src/bare_metal_tests/   workflow + activity code
config/                 default.toml (full settings) + demo.toml (overrides)
environment/            machines.example.json, inventory.md, secrets.env.example
migrations/             PostgreSQL schema for the bmt result store
tests/                  unit and integration tests
docker-compose.yml      Temporal server, Temporal UI and PostgreSQL
```

## Setup

```bash
# 1. Bring up Temporal + PostgreSQL.
docker compose up -d

# 2. Local Python environment.
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# 3. Per-host configuration.
cp environment/machines.example.json environment/machines.json
$EDITOR environment/machines.json          # fill in real hosts

cp environment/secrets.env.example environment/secrets.env
$EDITOR environment/secrets.env            # SSH passwords / key path / sudo password
```

Each host whose `auth` is `password` expects an env variable named
`SSH_PASSWORD_<NAME>` where `<NAME>` is the machine name upper-cased
with non-alphanumeric characters replaced by `_`. For a machine named
`host-a` set `SSH_PASSWORD_HOST_A`. Hosts whose `auth` is `ssh_key` use
the private key at `SSH_KEY_PATH`.

## Running a batch

Two terminals:

```bash
# Terminal 1: worker (stays open, picks up tasks from Temporal).
bmt-worker

# Terminal 2: trigger a batch and wait for the result.
bmt-run --use-batch --batch all --phases hardware,mesh,encap \
        --override config/demo.toml
```

Useful combinations:

```bash
# One host, setup only (installs the required tools via apt).
bmt-run --machine host-a --phases setup

# All hosts, hardware probes only, persisted to PostgreSQL.
bmt-run --batch all --phases hardware --use-batch

# Full pipeline with the short durations from demo.toml.
bmt-run --batch all --phases setup,hardware,stress,mesh,encap \
        --use-batch --override config/demo.toml
```

`--use-batch` starts a single `BatchWorkflow` that fans out into
`MachineWorkflow` per host plus `NetworkMeshWorkflow` and
`EncapsulationWorkflow`. Without `--use-batch` the CLI starts each
workflow independently and writes batch row bookkeeping itself; this is
mainly useful when debugging a single phase.

The Temporal UI is served at <http://localhost:8233>. The
`openstack-test` namespace is created automatically.

## Configuration

`config/default.toml` carries every tunable: stress durations, fio
profiles, mesh duration and MTU, retry policy, activity timeouts.
`config/demo.toml` overrides a handful of them with short values so the
whole pipeline runs in a couple of minutes.

To change a value at runtime, either edit `default.toml` directly or
copy the override into another file and pass `--override path.toml`.

## Result storage

PostgreSQL holds the `bmt` schema with four tables:

- `batch_run`: one row per `bmt-run` invocation
- `machine_run`: one row per host inside a batch
- `phase_result`: per-phase outcome (`hw_cpu`, `fio_seq_read`,
  `mesh.iperf3_pair`, ...) with details and timing in JSONB
- `metric`: numeric measurements attached to a phase
  (throughput Gb/s, IOPS, latency)

`environment/secrets.env` controls the connection. Temporal and
PostgreSQL share the same Postgres container but use different
databases (`temporal` and `bare_metal_tests`).

## Tests

```bash
# Unit tests (no network, no DB).
pytest tests/unit

# Integration smoke (requires reachable hosts).
BMT_INTEGRATION=1 pytest tests/integration

# Lint and type-check.
ruff check src/ tests/
mypy src
```

## Disk safety

The fio activities only run against block devices that appear in the
`test_disks` list of a host's entry in `machines.json`. A target outside
that list raises `DiskSafetyViolation` and the activity refuses to run.
Never put the boot device or any device with data you want to keep in
`test_disks` — fio writes destructively.

## Resetting state

```bash
# Wipe Temporal history and the PostgreSQL volume, then start fresh.
docker compose down -v
docker compose up -d
```
