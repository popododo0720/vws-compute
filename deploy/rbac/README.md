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

Not granted, because no code calls it: secrets, network-attachment-definitions,
`virtualmachines/addvolume` (hotplug), and `create`, `list`, `delete` on PVCs.
