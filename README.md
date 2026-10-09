# vws-compute

vws-compute is the VWS compute service. It serves `vws.compute.v1.ComputeService` over gRPC. Callers run, start, stop, reboot, and terminate instances, register and capture disk images, and manage SSH key pairs.

Every mutating call records desired state and returns an operation at once. A reconciler converges the hypervisor agent toward that state in the background. Callers poll `GetOperation` or block in `WaitOperation`. Each mutating request carries a `request_id`, so a retry is safe. Sending the same request again returns the original operation and changes nothing.

The API is defined in `proto/vws/compute/v1/compute.proto`. Contributor rules are in `AGENTS.md`.

## Run

```sh
go run ./cmd/vws-compute -listen 127.0.0.1:50051
```

`-db` selects storage. Empty, the default, keeps all state in memory. A path keeps desired state, observed status, and operations in that SQLite file, and one process holds the file at a time. The binary drives an in-memory fake agent, so its machines and image artifacts do not survive a restart.

## Test

```sh
go test ./...
```
