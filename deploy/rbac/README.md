# RBAC for vws-compute

Namespace-scoped `Role` and `RoleBinding` objects. No `ClusterRole`, no wildcards.
The client is uncached, so no `watch` is granted. Add `watch` to each get and
list rule if a cached client is introduced.

Names match the flag defaults: store namespace `vws-compute`
(`-namespace`), `vws-vms` (`-vm-namespace`), `vws-images` (`-image-namespace`).
Change the `namespace` fields together with the flags. The service account is
`vws-compute` in the store namespace.

Apply: `kubectl apply -f deploy/rbac/`.

Granted in the VM namespace: `get` on PVCs, because `volumes.go`
`checkAttachable` reads the claim access modes before an attach. vws-storage
creates the volume PVCs; vws-compute only reads them.

`CreateImage` from a volume or a machine clones a PVC in `vws-vms` into an
image DataVolume in `vws-images`. CDI checks that the DataVolume's creator may
create `datavolumes/source` in the source PVC's namespace, so the VM Role
grants exactly that to `vws-compute`.

Cross-namespace clone: VMs clone from a DataSource in `vws-images`, so the VM
namespace `default` service account gets `create` on `datavolumes/source` in
`vws-images` (`vws-compute-image-clone`). CDI `doc/RBAC.md` (PVC Cloning) and
KubeVirt PR #2509 describe the check. Not verified on a real cluster.

Granted in the VM namespace: `get`, `create`, `update`, `delete` on Secrets,
because `cloudinit.go` writes two Secrets per machine before its VM:
`<instance>-cloudinit` holds the cloud-init user data
(`cloudInitNoCloud.secretRef`), and `<instance>-ssh-keys` holds the key pair's
public keys (`accessCredentials`, propagation `noCloud`). Destroy deletes both
after the VM. The Role cannot narrow these to compute's own Secrets
(`resourceNames` cannot limit `create`), so compute touches only Secrets that
carry its managed label. Keep no other Secrets in the VM namespace.

Not granted, because no code calls it: network-attachment-definitions,
`virtualmachines/addvolume` (hotplug), and `create`, `list`, `delete` on PVCs.
