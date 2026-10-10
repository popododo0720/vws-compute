# vws-compute

vws-compute is the VWS compute service. It serves `InstanceService`, `ImageService`, and `KeyPairService` from `vws.compute.v1`, plus `google.longrunning.Operations` for `GetOperation` and `WaitOperation`.

Every mutating call records desired state and returns a long-running operation at once. A reconciler converges the hypervisor agent toward that state in the background. Callers poll `GetOperation` or block in `WaitOperation`. Each mutating request may carry a UUID `request_id`, so a retry is safe. Sending the same request again returns the original operation and changes nothing.

The wire contract is `github.com/popododo0720/vws-proto` (commit `a1c6084`), vendored at `third_party/vws-proto`. When vws-proto is public, remove the `replace` in `go.mod` and require a tagged version. This process stores key pairs under their resource name. It does not host volumes, snapshots, subnets, or security groups, so an instance or image spec that names one is rejected with `DEPENDENCY_MISSING`. One repository backs every project, and list responses name rows with the parent the caller sent. Contributor rules are in `AGENTS.md`.

## Run

```sh
go run ./cmd/vws-compute -listen 127.0.0.1:50051
```

`-db` selects storage. Empty, the default, keeps all state in memory. A path keeps desired state, observed status, and operations in that SQLite file, and one process holds the file at a time. The binary drives an in-memory fake agent. On startup it re-applies the stored specs, so a restart rebuilds that agent's machines and image artifacts from the repository.

## Test

```sh
go test ./...
```
