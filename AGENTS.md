# AGENTS.md

Rules for anyone changing this repository, human or agent. A rule with an enforcer fails a check when it is broken. A rule marked judgment has no check, so the author and the reviewer carry it.

## Layout

| Package | Owns |
| --- | --- |
| `internal/compute` | Domain types, `Plan`, `Settle`, and request fingerprints. Pure functions only. |
| `internal/store` | Repository interfaces, split by writer. `storetest` is the suite every repository passes. |
| `internal/store/memstore`, `internal/store/sqlitestore` | The two repositories. |
| `internal/controller` | `Submit`, the only mutation path. |
| `internal/reconciler` | The loop that converges the agent toward desired state. |
| `internal/hypervisor` | The agent contract. `fakeagent` implements it in memory. |
| `internal/grpcapi` | The gRPC edge. Wire stubs come from `github.com/popododo0720/vws-proto`. |
| `cmd/vws-compute` | Wiring, flags (`-storage` is the vws-storage address), and the `-seed-images` loader. |

## Rules

| Rule | Enforced by |
| --- | --- |
| Only `internal/controller` calls `CommitDesired`, and it never imports `internal/hypervisor`. | `TestArchitectureRules` in `internal/archtest` |
| Only `internal/reconciler` calls `CommitInstanceStatus`, `CommitImageStatus`, `ConvergeMachine`, and `ConvergeImage`. | `TestArchitectureRules` |
| `internal/compute` imports nothing from this module and no gRPC, protobuf, `database/sql`, or SQLite package. | `TestArchitectureRules` |
| `internal/hypervisor` imports nothing from this module. `fakeagent` imports only `internal/hypervisor`. | `TestArchitectureRules` |
| Wire types stay under `internal/grpcapi`. | Go `internal/` visibility and `TestArchitectureRules` |
| `database/sql` and the SQLite driver stay in `sqlitestore`, and its row structs stay unexported. | `TestArchitectureRules` |
| Production packages do not import `testing`. `storetest` is the one declared exception. | `TestArchitectureRules` |
| An operation changes state only through `compute.Settle`, and terminal states are absorbing. | Unexported fields, `TestOperationStateIsUnexported`, and the `Settle` table in `internal/compute` |
| Transition rules live only in `compute.Plan`. Each intent carries its own `plan` method. | The compiler, because `Intent` is sealed by unexported methods |
| Private keys are never stored. | `store.DesiredChange` has no field for one, `TestSchemaHasNoPrivateKeyColumn`, and `TestComputeMessagesCarryNoPrivateKey` |
| Every repository behaves the same. | `storetest.Run`, called from both store test packages |
| A capture finishes before its source instance is destroyed, and a launch finishes before its image is deregistered. | `TestCaptureBeforeTerminate` and `TestLaunchBeforeDeregister` |
| A new desired generation does not wait out the previous retry. | `TestNewGenerationDoesNotInheritBackoff` |
| A commit rejects a plan whose observed generation or parked bit moved. | `storetest` conflict case and `TestStopReplansWhenTheGenerationParksBeforeCommit` |
| Shutdown cancels an in-flight RPC instead of waiting out its timeout. | `TestStopServerCancelsHandlersAfterGrace` |
| A second process cannot open a SQLite file this process holds, including one that already has a schema. | `TestSecondOpenFails` |
| Startup re-applies converged specs to the agent. | `TestRestartRebuildsTheAgent` |
| A seed file registers each image once. A restart with the same file writes nothing, and a changed path stops startup. | `TestSeedIsIdempotentAcrossRestarts` |
| `CreateImage` stores a volume or snapshot source and `GetImage` returns it. A disk path is registered only by `-seed-images`. | `TestCreateImageRoundTrip` and `TestSeededImageLaunchStopStartTerminate` |
| A new RPC on `InstanceService`, `ImageService`, `KeyPairService`, or `NodeService` fails to build until it is served. | `server` embeds the four `Unsafe...ServiceServer` types |
| Only `internal/grpcapi` calls `SyncNode` and `ReportInstanceStatus`. Placement runs in the store transaction that writes it and sets `assigned_node` only while it is empty. | `TestArchitectureRules` and the `storetest` placement cases |
| A node-driven instance (non-empty zone) is converged by its node, never by the reconciler. | `storetest` `nodeDrivenNotUnconverged` |
| `CreateInstance` asks the storage service about volumes only for a request id the store has not seen. | `TestCreateInstancePlacesAndIsIdempotent` |
| Create, Update, and Delete take `request_id` and return an `Operation`. Reads do not. A report takes `request_id` and returns the resource. | `TestServicesMatchTheResourceAPI` |
| Wire protos come from `github.com/popododo0720/vws-proto`, vendored at `third_party/vws-proto` until that module is public. | `TestVendoredProtoModule` |
| No rights or mark notices outside `LICENSE`. | `TestNoTrademarkNotices` |
| No container or chart deploy manifests. | `TestNoDeployManifests` |
| Code is gofmt and goimports clean and passes vet, staticcheck, errcheck, ineffassign, unused, and misspell. `third_party` is excluded. | `golangci-lint` v2.14.0 in CI |
| `go test ./...` passes. | CI |
| Work on a feature branch. Never commit to `main`. | Judgment |
| Each commit is one logical change and leaves `go test ./...` passing. | Judgment |
| Use pstack. Run `architect` before code that crosses a package boundary, `interrogate` on a contested design, and `correct` when the same mistake comes back. | Judgment |

## Commands

```sh
go test ./...
golangci-lint run ./...
```

The generated stubs live in the vendored `github.com/popododo0720/vws-proto` module under `third_party/vws-proto/gen/go`. This repository does not regenerate them. When that module is public, drop the `replace` and require a tagged release.
