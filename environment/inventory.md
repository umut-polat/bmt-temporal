# Hardware Inventory

Document the hosts under test here so other contributors know what they
are working with. Copy `machines.example.json` to `machines.json` and
fill in real hostnames, IPs, NIC names and test-safe block devices.

## Suggested layout per host

| Field | Description |
|---|---|
| `name` | Short logical name used as workflow ID suffix |
| `host` | Reachable IP or DNS name |
| `user` | SSH user (must have sudo) |
| `auth` | `password` or `ssh_key` |
| `mesh_iface` | NIC used for the mesh + tunnel tests (e.g. `ens5f0`) |
| `test_disks` | Block devices safe to write to (NEVER the OS/boot device) |

## Safety notes

- `test_disks` is enforced by the fio activity. A target not present in
  this list raises `DiskSafetyViolation` and the activity refuses to run.
- The boot device (e.g. an OS RAID device) must not appear in
  `test_disks`. Treat the list as a destructive-write whitelist.
- The mesh phase tries to set the interface MTU to the value configured
  in `default.toml` (`network.mesh.mtu`). Make sure the upstream switch
  supports it before running the jumbo-frame check.
