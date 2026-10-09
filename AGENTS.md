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
| `internal/grpcapi` | The gRPC edge. Generated stubs live in `internal/grpcapi/internal/computepb`. |
| `cmd/vws-compute` | Wiring and flags. |

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
| Private keys are never stored. | `store.DesiredChange` has no field for one, `TestSchemaHasNoPrivateKeyColumn`, and `TestOnlyCreateKeyPairResponseCarriesAPrivateKey` |
| Every repository behaves the same. | `storetest.Run`, called from both store test packages |
| A new RPC fails to build until it is served. | `server` embeds `UnsafeComputeServiceServer` |
| Every mutating RPC takes `request_id` and returns an `Operation`. | `TestMutatingRPCsTakeRequestIDAndReturnAnOperation` |
| The proto's first line is the move TODO. | `TestProtoStartsWithMoveTODO` |
| No rights or mark notices outside `LICENSE`. | `TestNoTrademarkNotices` |
| No container or chart deploy manifests. | `TestNoDeployManifests` |
| Code is gofmt and goimports clean and passes vet, staticcheck, errcheck, ineffassign, unused, and misspell. | `golangci-lint` v1.61.0 in CI |
| `go test ./...` passes. | CI |
| Work on a feature branch. Never commit to `main`. | Judgment |
| Each commit is one logical change and leaves `go test ./...` passing. | Judgment |
| Use pstack. Run `architect` before code that crosses a package boundary, `interrogate` on a contested design, and `correct` when the same mistake comes back. | Judgment |

## Commands

```sh
go test ./...
golangci-lint run ./...
```

Regenerating the stubs needs `protoc`, `protoc-gen-go` v1.36.1, `protoc-gen-go-grpc` v1.5.1, and `GOOGLEAPIS` set to a googleapis checkout:

```sh
go generate ./internal/grpcapi/internal/computepb
```
