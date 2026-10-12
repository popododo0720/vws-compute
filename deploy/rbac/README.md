# RBAC for vws-compute

Namespace-scoped `Role` and `RoleBinding` objects. No `ClusterRole`, no wildcards.
The client is uncached, so no `watch` is granted. Add `watch` to each get and
list rule if a cached client is introduced.

Names match the flag defaults: store namespace `vws-compute`
(`-namespace`), `vws-vms` (`-vm-namespace`), `vws-images` (`-image-namespace`).
Change the `namespace` fields together with the flags. The service account is
`vws-compute` in the store namespace.

Apply: `kubectl apply -f deploy/rbac/`.

Not granted, because no code calls it: secrets, network-attachment-definitions,
`virtualmachines/addvolume` (hotplug, PR #22), and PVCs in the VM namespace.
vws-storage creates the volume PVCs and owns that permission.
