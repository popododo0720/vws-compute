# vws-compute

vws-compute is the VWS compute service. It serves `InstanceService`, `ImageService`, and `KeyPairService` from `vws.compute.v1`, plus `google.longrunning.Operations` for `GetOperation` and `WaitOperation`.

Every mutating call records desired state and returns a long-running operation at once. A reconciler converges the hypervisor agent toward that state in the background. Callers poll `GetOperation` or block in `WaitOperation`. Each mutating request may carry a UUID `request_id`, so a retry is safe. Sending the same request again returns the original operation and changes nothing.

The wire contract is `github.com/popododo0720/vws-proto` at tag v0.5.0, fetched through the module proxy. To move to a new contract, run `go get github.com/popododo0720/vws-proto@vX.Y.Z` and `go mod tidy`. This process stores key pairs under their resource name. `CreateImage` records a volume or snapshot resource name and the reconciler copies it; this process does not host those resources. `CreateInstance` verifies its volumes with vws-storage (`-storage`); without that flag it answers `DEPENDENCY_MISSING`. Subnet and security group names are checked for shape and stored. One repository backs every project, and list responses name rows with the parent the caller sent. Contributor rules are in `AGENTS.md`.

## Run

```sh
go run ./cmd/vws-compute -listen 127.0.0.1:50051
```

`-namespace` selects storage. Empty, the default, keeps all state in memory. A Kubernetes namespace keeps desired state, observed status, and operations as ConfigMaps there (no CRD). `-kubeconfig` names the cluster config. Empty uses the in-cluster service account, then `$KUBECONFIG`. Run one replica: startup repairs the namespace and assumes no other writer. The namespace must exist. `-hypervisor` picks the agent: `fake` (the default, in memory) or `kubevirt`, which needs `-namespace`. On startup the process re-applies the stored specs, so a restart rebuilds the fake agent's machines and image artifacts from the repository.

## Machine types

`machine_type` must be one of these (`internal/compute/names.go`). Any other name is `INVALID_ARGUMENT` / `INVALID_SPEC`.

| name | vCPU | memory |
| --- | --- | --- |
| `vws.small` | 1 | 1024 MiB |
| `vws.medium` | 2 | 4096 MiB |
| `vws.large` | 4 | 8192 MiB |
| `vws.xlarge` | 8 | 16384 MiB |

## Seed images

`-seed-images` names a JSON file of disk images to register before the server listens. The process opens the port only after every seed image is ready, so a running server can launch instances from those ids.

```sh
go run ./cmd/vws-compute -namespace vws -seed-images images.json
```

```json
{
  "images": [
    {"id": "debian-12", "family": "debian-12", "path": "/var/lib/vws/images/debian-12.qcow2"}
  ]
}
```

`id` is the image resource id (`vws.common.v1.ResourceId`). Clients address it as `projects/{project}/regions/{region}/images/{id}`, and an in-process launch passes the same id to `RunInstance`. `family` is `spec.family`. `path` is an absolute path to a regular file on the agent's host. vws-compute checks that the file exists at startup and does not read its bytes; the agent copies it while converging the image. The fake agent this binary runs records the path without reading it.

Every field is required. Unknown fields are errors. Starting again with the same file writes nothing. Changing an entry's family or path after that id was registered stops startup; give the image a new id. An image deleted with `DeleteImage` stays deleted. `GetImage` leaves `spec.volume` and `spec.snapshot` unset for a seeded image, because the proto source is a volume or a snapshot and the path is not on the wire.

`CreateImage` copies a volume or a snapshot and mints the image id (`img-` and 17 hex digits). The caller's `image_id` is a request field, not the stored name. `GetImage` on the operation's image name returns the source it copied. A filesystem path is not a `CreateImage` source.

## Test

```sh
go test ./...
```
