# AGENTS.md

Rules for anyone changing this repository, human or agent. A rule with an enforcer fails a check when it is broken. A rule marked judgment has no check, so the author and the reviewer carry it.

## Layout

| Package | Owns |
| --- | --- |
| `internal/compute` | Domain types, `Plan`, `Settle`, and request fingerprints. Pure functions only. |
| `internal/store` | Repository interfaces, split by writer. `storetest` is the suite every repository passes. |
| `internal/store/memstore`, `internal/store/kubestore` | The two repositories. `kubestore` keeps each resource in one ConfigMap. `kubetest` starts envtest for tests. |
| `internal/controller` | `Submit`, the only mutation path. |
| `internal/reconciler` | The loop that converges the agent toward desired state. |
| `internal/hypervisor` | The agent contract. `fakeagent` implements it in memory. `kubevirt` implements it with KubeVirt `VirtualMachine` objects. |
| `internal/grpcapi` | The gRPC edge. Wire stubs come from `github.com/popododo0720/vws-proto`. |
| `cmd/vws-compute` | Wiring, flags (`-hypervisor`, `-vm-namespace`, `-image-namespace`, `-storage` is the vws-storage address, `-legacy-image-scope` owns images stored without a project, `-public-images` lists public images), and the `-seed-images` loader. |

## Rules

| Rule | Enforced by |
| --- | --- |
| Only `internal/controller` calls `CommitDesired`, and it never imports `internal/hypervisor`. | `TestArchitectureRules` in `internal/archtest` |
| Only `internal/reconciler` calls `CommitInstanceStatus`, `CommitImageStatus`, `ConvergeMachine`, and `ConvergeImage`. | `TestArchitectureRules` |
| `internal/compute` imports nothing from this module and no gRPC, protobuf, `database/sql`, or SQLite package. | `TestArchitectureRules` |
| `internal/hypervisor` imports nothing from this module. `fakeagent` and `kubevirt` import only `internal/hypervisor`. | `TestArchitectureRules` |
| Kubernetes client packages (`k8s.io`, `sigs.k8s.io`) stay in `internal/hypervisor/kubevirt` and `internal/store/kubestore`. | `TestArchitectureRules` |
| Converging a machine twice, or from a fresh agent, writes the Kubernetes API once. A power flip is one patch. | `TestConvergeTwiceMakesOneObject`, `TestNewAgentDoesNotDuplicate`, `TestPowerFlipsWithOnePatch` (envtest) |
| An image is a CDI `DataVolume` and `DataSource` named by the image ID. Its size is the bound claim's capacity once the `DataVolume` succeeded. Removal fails while a `VirtualMachine` clones from it. | `TestImageIsCreatedOnceAndSizedByTheClaim` and `TestRemoveImageRefusedWhileAMachineClonesIt` (envtest) |
| An image's `DataSource` carries `vws.io/project` and `vws.io/region`, which vws-storage requires to clone it. A `DataSource` without them is labeled with one patch by the sweep. | `TestAuthorizeImages` (envtest) |
| A key pair reaches the guest as NoCloud meta-data `public-keys` (`accessCredentials` from Secret `<instance>-ssh-keys`, propagation `noCloud`), as EC2 delivers it through instance meta-data. User data reaches the guest unchanged; a key alone also renders a `#cloud-config` with `ssh_authorized_keys`. The keys Secret starts with a comment placeholder because CirrOS skips public-keys entry 0. Terminate deletes both Secrets. | `TestRenderUserData`, `TestKeysData`, `TestKeyAndUserDataReachCloudInit` (envtest) |
| Data volumes attach and detach as hot-pluggable PVC volumes with one patch each. A repeat writes nothing. A single-writer claim already attached to another machine is refused permanently. An EC2 device path (`/dev/sdf`) is disk `sdf`, and the attachment survives a restart. | `TestVolumesAttachAndDetachOnARunningMachine`, `TestAttachPatchesOnceAndKeepsCloudInit`, `TestSingleWriterClaimIsRefusedWhileAttachedElsewhere`, `TestEC2DeviceAttachSurvivesRestart` (envtest) |
| A reported volume is `ATTACHED` only once the agent says its hot-plug completed (KubeVirt VMI `volumeStatus` phase `Ready`, or the machine is halted). Before that it is `ATTACHING`, `BUSY` while a transient agent error has the node retrying, and `DETACHING` after it leaves the spec until the machine lets go. A refused volume has no entry. A settled instance costs no agent read and no write. Only `internal/reconciler` calls `MachineVolumes`. | `TestVolumeIsAttachedOnlyAfterHotplugCompletes`, `TestVolumeIsBusyWhileTheNodeRetries`, `TestRefusedVolumeIsNotReported`, `TestHotplugIsPluggedOnlyOnceKubeVirtSaysReady`, `TestReportedAttachStateFollowsTheHotplug` (envtest), `TestReportedVolumeAttachStatesRoundTrip`, `TestArchitectureRules` |
| A machine created with data volumes lists them in the create as the same hot-pluggable entries an attach adds, in one write, so they reach `ATTACHED` after their hot-plug with no update. A data volume that cannot attach yet does not hold the boot back: the machine is created without data volumes, a missing claim is retried and hot-plugged by the next converge, and every refused one is reported and fails like an attach to a running machine. | `TestCreateWithDataVolumesHotplugsThem`, `TestCreateBootsWithoutARefusedDataVolume`, `TestCreateBeforeTheClaimExistsAttachesOnRetry`, `TestCreatedDataVolumeIsReportedAttachedAfterHotplug`, `TestCreateReportsEveryRefusedVolume`, `TestCreatedInstanceLeavesNoRefusedVolumeAttaching`, `TestAttachReportsEveryRefusedVolume` (envtest), `TestEveryRefusedAttachmentIsDroppedFromDesiredState` |
| An instance with a direct interface (subnet `subnet-direct-<name>`, vws-deploy#125) gets every interface in device index order (`nic<N>`) as KubeVirt bridge `interfaces[]` and `networks[]`: the one overlay interface is the pod network on its `ovn.kubernetes.io/logical_switch`, a direct one is Multus network `vws-direct-<name>` in the VM namespace with `<provider>.kubernetes.io/logical_switch` and, for a fixed address, `<provider>.kubernetes.io/ip_address` (provider `vws-direct-<name>.<ns>.ovn`). Without one, only the first interface is wired by subnet, as before. A repeated device index is `INVALID_ARGUMENT`. A second overlay interface and any direct mode but bridge (SR-IOV, macvtap) are permanent errors. | `TestNetworksFollowDeviceIndexOrder`, `TestNetworksRefuseInvalidInterfaces`, `TestDrivenMachineWiresDirectInterfaces`, `TestCreateInstanceRefusesBadNetworkInterfaces`, `TestDirectInterfacesReachTheVirtualMachine` (envtest) |
| Wire types stay under `internal/grpcapi`. | Go `internal/` visibility and `TestArchitectureRules` |
| No `database/sql` or SQLite driver. `kubestore` exports only `Store`, `Repaired`, and `Open`/`New`, and its record structs stay unexported. | `TestArchitectureRules` |
| Production packages do not import `testing`. `storetest` is the one declared exception. | `TestArchitectureRules` |
| An operation changes state only through `compute.Settle`, and terminal states are absorbing. | Unexported fields, `TestOperationStateIsUnexported`, and the `Settle` table in `internal/compute` |
| Transition rules live only in `compute.Plan`. Each intent carries its own `plan` method. | The compiler, because `Intent` is sealed by unexported methods |
| Private keys are never stored. | `store.DesiredChange` has no field for one, `TestSchemaHasNoPrivateKeyColumn`, and `TestComputeMessagesCarryNoPrivateKey` |
| Every repository behaves the same. | `storetest.Run`, called from both store test packages |
| A capture finishes before its source instance is destroyed, and a launch finishes before its image is deregistered. | `TestCaptureBeforeTerminate` and `TestLaunchBeforeDeregister` |
| A new desired generation does not wait out the previous retry. | `TestNewGenerationDoesNotInheritBackoff` |
| A commit rejects a plan whose observed generation or parked bit moved. | `storetest` conflict case and `TestStopReplansWhenTheGenerationParksBeforeCommit` |
| Shutdown cancels an in-flight RPC instead of waiting out its timeout. | `TestStopServerCancelsHandlersAfterGrace` |
| A request id maps to one operation across restarts, and a crash between the request index and the resource write is repaired. | `TestSameRequestIDSameOperationAcrossRestart`, `TestRepairRemovesAnOrphanedIndexAndTheRetrySucceeds`, `TestRetryAdoptsAnOrphanedIndex`, `TestRepairRestoresAMissingIndex` (envtest) |
| A resource keeps its newest 100 operations and every operation settled in the last 24 hours. An older settled one is dropped in the CAS update that commits a new operation, and its request index is deleted after it (Repair removes one a crash leaves). A Pending operation is never dropped. A record over the 1 MiB ConfigMap limit is `LimitExceeded` (gRPC `RESOURCE_EXHAUSTED`, `QUOTA_EXCEEDED`), not retried. | `TestOperationHistoryIsBounded`, `TestPrunedIndexLeftByACrashIsRepaired`, `TestTooLargeRecordFailsOnceWithLimitExceeded` (envtest), `TestPruneNeverDropsAPendingOperation`, `TestRecordOverTheConfigMapLimitIsLimitExceeded` |
| A `CreateImage` request id stored before image scopes (#92) still replays, only for the scope its image holds. A polled image operation names the image in its stored region. | `TestPreUpgradeCreateImageReplaysUnderItsScope`, `TestLegacyFingerprintMatchesThePreScopeRelease`, `TestPolledImageOperationNamesTheImageWhereGetImageFindsIt` |
| A write that loses a resourceVersion race rereads and reapplies its rule. A stale plan conflicts and leaves no index. | `TestCASConflictRetriesOnFreshState` and `TestStaleExpectLosesToACompetingCommit` (envtest) |
| A key name and a controller-driven volume have one holder. Only a holder that exists and no longer qualifies loses it. | `storetest` key pair and volume cases, `TestRepairReleasesAClaimWhoseHolderNeverWasWritten`, `TestKeyPairNameHasOneHolder` (envtest) |
| A key name created again after its delete is a new key pair: a new `key-` id and the new key. Get and List return only that one, and an older create's replay or operation poll returns its own deleted record. Of concurrent re-creates, one wins. | `TestKeyPairReincarnation` (envtest), `TestDeleteKeyPair` |
| Records from the previous release decode. A fixture under `testdata/upgrade/<version>/` is regenerated only at a release bump. | `TestPreviousReleaseRecordsDecode` |
| Startup re-applies converged specs to the agent. | `TestRestartRebuildsTheAgent` |
| A seed file registers each image once. A restart with the same file writes nothing, and a changed path stops startup. | `TestSeedIsIdempotentAcrossRestarts` |
| An image belongs to the project and region that created it. `GetImage`, `ListImages`, and `DeleteImage` from any other project or region see `NOT_FOUND`, as for no image. A record from before scopes takes `-legacy-image-scope`. | `TestImageOfAnotherProjectIsNotFound`, `TestImageScopeRoundTripsAndLegacyRecordsTakeTheDefault` |
| An image in `-public-images` is read and listed by every project in its region, as an AWS public AMI. Update and delete stay with the owner; others see `NOT_FOUND`. A DataSource's authorization labels (`vws.io/project`, `vws.io/region`, `vws.io/public`) come only from `reconciler.ImageAuth` (store record plus public list; no record, deregistered or no project means none), applied by `AuthorizeImages` to every managed DataSource on every sweep, whatever the image state. No other code writes them. The image `Role` grants `list` and `patch` on `datasources` for that. | `TestPublicImageIsReadableEverywhereInItsRegion`, `TestParseConfigPublicImages`, `TestImageAuthIgnoresImageState`, `TestAuthorizeImages`, `TestNewDataSourceCarriesOnlyTheManagedLabel`, `TestParkedImageLosesThePublicLabel`, `TestImageWithoutSourceMachineLosesItsLabels`, `TestImageRoleLetsTheAgentLabelAnExistingDataSource` (envtest) |
| `CreateImage` stores a volume or snapshot source and `GetImage` returns it. A disk path is registered only by `-seed-images`. | `TestCreateImageRoundTrip` and `TestSeededImageLaunchStopStartTerminate` |
| `CreateImage` mints the image id (`img-` and 17 hex digits). The caller's `image_id` is a request field, so another project's id and an id nobody holds get the same response, including `validate_only`. The same request id returns the same operation. A deregistered image is not revived. Seed registration keeps the caller's id. | `TestCreateImageOfAnotherProjectMatchesAnAbsentID` (envtest), `TestPlanCopyAndFileImage`, `TestCreateImageRoundTrip` |
| `CreateImage` copies only a volume or snapshot of the caller's project, a volume in a zone of the caller's region (the region name plus one letter, as AWS names zones: `vws-1a` is in `vws-1`), and a snapshot of the caller's region. Any other source is `NOT_FOUND` (`RESOURCE_NOT_FOUND`), decided from the names alone, so a foreign source that exists and one that does not get the same answer, including `validate_only` and a retry. Nothing is stored. The one exception is a snapshot of another project in the caller's region whose createVolumePermission, read through `WithSnapshotPermissions`, grants the caller's project or `all` (EC2 ModifySnapshotAttribute): it is copied into an image of the caller. An unshared one is the same `NOT_FOUND` as an absent one, a grant that cannot be read is `UNAVAILABLE` (`DEPENDENCY_UNAVAILABLE`), and without `WithSnapshotPermissions` nothing is shared. This service only reads the permission; the snapshot owner (vws-storage) stores it. | `TestCreateImageOfAnotherProjectsSourceIsNotFound` (memstore and envtest), `TestCreateImageFromASharedSnapshot` (memstore and envtest), `TestForeignSnapshotIsNotFoundWithoutSnapshotPermissions`, `TestSourceRefScope`, `TestZoneInRegion` |
| A new RPC on `InstanceService`, `ImageService`, `KeyPairService`, or `OperationLookupService` fails to build until it is served. | `server` embeds the four `Unsafe...ServiceServer` types |
| `LookupOperation` answers a request id with the operation `GetOperation` returns, only for the project its resource proves (a controller-driven instance's project, an image's scope, a key pair's name) and only within 24 hours of the operation's create time. A foreign, expired, or unknown request id is the same `NOT_FOUND` (`RESOURCE_NOT_FOUND`); an unset or non-UUID `request_id` or a parent other than `projects/*` is `INVALID_SPEC`. It is read-only. | `TestLookupOperation` (memstore and envtest) |
| Only `internal/grpcapi` calls `ReportInstanceStatus`. This service places nothing: kube-scheduler does. | `TestArchitectureRules` |
| A controller-driven instance (non-empty zone) is converged by an external controller through `ReportInstanceStatus`, never by the reconciler. Its delete finishes only when the controller reports DELETING. | `storetest` `controllerNotUnconverged`, `deleteWaitsForTheController`, and `TestUnreportedDeleteStaysUntilTheControllerReports` |
| `CreateInstance` asks the storage service about volumes only for a request id the store has not seen. | `TestCreateInstanceIsIdempotent` |
| Create, Update, and Delete take `request_id` and return an `Operation`. Reads do not. A report takes `request_id` and returns the resource. | `TestServicesMatchTheResourceAPI` |
| Wire protos come from `github.com/popododo0720/vws-proto` at a release tag (`vX.Y.Z`) through the module proxy, with no `replace`, no local copy, and no `*.pb.go` in this repository. | `TestVWSProtoIsATag` |
| No rights or mark notices outside `LICENSE`. | `TestNoTrademarkNotices` |
| No container or chart deploy manifests. RBAC `Role` manifests under `deploy/rbac` are allowed, namespaced and without wildcards. The only release step is `.github/workflows/publish.yml`: on a push to main, ko builds the image and pushes `ghcr.io/<owner>/vws/compute:sha-<7-char commit>` with `org.opencontainers.image.revision` (popododo0720/vws-deploy#280). | `TestNoDeployManifests`, `TestRBACManifestsAreNamespaced`; `publish.yml` |
| Code is gofmt and goimports clean and passes vet, staticcheck, errcheck, ineffassign, unused, and misspell. `third_party` is excluded. | `golangci-lint` v2.14.0 in CI |
| `go test ./...` passes. The envtest tests skip unless `KUBEBUILDER_ASSETS` points at `kube-apiserver` and `etcd`. | CI |
| The repo is English only (see "English only"). Hangul is allowed only in files `.github/english-allowlist` lists with a reason. | `scripts/check-english.sh files` (CI `english` job) |
| Work on a feature branch. Never commit to `main`. | Judgment |
| Each commit is one logical change and leaves `go test ./...` passing. | Judgment |
| Use pstack. Run `architect` before code that crosses a package boundary, `interrogate` on a contested design, and `correct` when the same mistake comes back. | Judgment |

## English only

Write everything in this repo in English: README, code comments, docs, AGENTS.md, QUALITY.md, PR and issue templates, commit messages, PR and issue bodies, review comments and `Verified-by` sections. The `english` job in CI runs `scripts/check-english.sh`: `files` fails on a tracked file with Hangul unless `.github/english-allowlist` lists it as `<glob> <reason>` (only test data and user-facing UI strings belong there), and `text` and `commits` reject Hangul in the PR body and the PR's commit messages with no exception. The script is the same file as in vws-storage and vws-network, so it is copied, not edited.

## /correct loop

1. When a bot is corrected (by the user Su Kim, tmp or eggbot, or by a failed `vws verify`), it opens an issue the same day with the `incident` label using the `.github/ISSUE_TEMPLATE/incident.yml` form. It must fill in the correction and "the check that would have caught this".
2. Every few days vws-verify runs a gardener pass. It groups open incidents by cause and promotes repeated ones to a check (test, lint, guard rule, archtest), not a sentence. The check must actually fail on an input that contains the mistake.
3. When the promoting PR is merged, close the incident with its link. An incident that will not be promoted is closed with the reason.
4. The milestone report (to tmp) lists the number of incidents in the period and the checks promoted.
5. The `incident` label is defined in `.github/labels.yml`, copied from vws-deploy. That file is only a definition and does not create the label on GitHub. `scripts/guard.sh correct` runs `scripts/guard-agents.sh correct`, which checks this section and its body, the required fields of the form and the label definition; `repo-guard.yml` runs it on every pull request and `scripts/guard_test.sh` shows it fails on an empty section.

## Insider-risk guardrails

Bots are insider risks too. These rules have no exceptions. Design: `popododo0720/vws-deploy#33`.

1. **Independent verification.** The bot or model that wrote a change does not verify it. Another bot or a person does. This is process, and CI cannot enforce it.
2. **Record.** Every change is an issue, then a PR with `Part of #N`, then commits. The PR body says what changed, how it was verified, and what was not verified. No force-push or history rewrite without the user's approval.
3. **Failing tests.** A new feature is not tested on the happy path alone. It has at least one case each for power loss or restart, idempotent replay, and hostile or boundary input. Otherwise the PR says why one is impossible.
4. **Least privilege.** No push to `main`. Every change is one PR from its own branch off `main`. No stacked PRs and no local merges. Tokens are scoped to this repository. RBAC covers only what the code calls, in a namespaced Role where possible (`vws-deploy` `docs/rbac.md`).
5. **Separate audit.** The author, the verifier, and the merge decision are different. The author never merges its own PR. Merge only when the base is `main` and every check on the head commit is success.
6. **Isolation and stop.** To stop a bot, end its session and revoke its token. Risky work runs only in the box or a throwaway cluster. No access to a production cluster.
7. **Incident disclosure.** A broken rule or an incident is written up in an issue labeled `incident` with cause, impact, fix, and prevention.

Repository items:

- Reboot recovery. A restart resumes from the repository alone: `TestRestartRebuildsTheAgent`, `TestSeedIsIdempotentAcrossRestarts`, and the `storetest` restart cases. An unreported delete stays until the controller reports: `TestUnreportedDeleteStaysUntilTheControllerReports`.
- Idempotent replay. A repeated request id returns the same operation: `TestCreateInstanceIsIdempotent`. Converging twice writes the Kubernetes API once: `TestConvergeTwiceMakesOneObject`.
- Hostile input. A volume or request the storage service rejects makes `CreateInstance` fail and leave no row: `TestCreateInstanceVerifiesVolumes`.
- RBAC. This service calls only the verbs listed in `vws-deploy` `docs/rbac.md` for compute.
- No AWS account ID digits anywhere in the repository.
- Checks: `.github/workflows/repo-guard.yml` runs `scripts/guard-agents.sh` on every pull request (base is `main`, body has `Part of #N` and no closing keyword, this section has items 1 to 7). `.github/workflows/direct-push-watch.yml` reports a commit that reached `main` without a pull request. Detection, not a block. Both files and the script are copied as-is from `vws-deploy`.

### Script push, PR and merge

A deterministic script (the factory orchestrator in `vws-deploy` `docs/factory-orchestrator.md`, the handoff submit) may push a branch, open a pull request and merge one. A script merge happens only when all of these hold:

- every CI check on the head commit is green;
- `verify-gate` passes;
- the `vws-verify` App check run passes;
- the pull request has no conflicts;
- the merge is a squash, made with `--match-head-commit` set to the verified head;
- the pull request is not labeled `blocked`.

`ORCHESTRATE_MERGE=apply` is approved only under these conditions. The lead (dr eggbot) is woken only for a decision or a failure (issue scope, real conflicts, a failed check), not for a routine push, pull request or merge. Guardrails 1, 4 and 2 still hold: the author bot is never the verifier, nothing pushes to main, and every pull request body has `Part of #N` with no closing keyword.

## Commands

```sh
go test ./...
golangci-lint run ./...
```

The generated stubs live in the `github.com/popododo0720/vws-proto` module under `gen/go`. This repository does not regenerate them. To take a new contract, run `go get github.com/popododo0720/vws-proto@vX.Y.Z` and `go mod tidy`.
