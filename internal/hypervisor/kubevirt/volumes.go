package kubevirt

import (
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

// A data volume is a hot-pluggable PersistentVolumeClaim volume in the
// VirtualMachine template, plus a scsi disk with the same name. KubeVirt
// rejects a change to the volumes of a running VirtualMachine unless the
// volume is marked hotpluggable, so every data volume is, running or not.
// Root and cloud-init volumes are never hot-pluggable, which is how this
// package tells its own volumes from them.

var reservedDisks = []string{"rootdisk", "cloudinitdisk"}

func templateSlice(vm *unstructured.Unstructured, path ...string) []any {
	s, _, _ := unstructured.NestedSlice(vm.Object, path...) //nolint:errcheck // a field of another type reads as empty and is rewritten
	return s
}

// claimOf returns the claim name of a hot-pluggable volume entry.
func claimOf(v any) (name, claim string, ok bool) {
	name, claim, hot, ok := pvcOf(v)
	return name, claim, ok && hot
}

// pvcOf returns the claim of any persistentVolumeClaim volume entry, a
// hot-plugged data volume or a boot-from-claim root disk.
func pvcOf(v any) (name, claim string, hot, ok bool) {
	m, isMap := v.(map[string]any)
	if !isMap {
		return "", "", false, false
	}
	pvc, isMap := m["persistentVolumeClaim"].(map[string]any)
	if !isMap {
		return "", "", false, false
	}
	hot, _ = pvc["hotpluggable"].(bool)  //nolint:errcheck // absent or another type is not hot-pluggable
	name, _ = m["name"].(string)         //nolint:errcheck // an entry of another shape is not a volume of ours
	claim, _ = pvc["claimName"].(string) //nolint:errcheck // an entry of another shape is not a volume of ours
	return name, claim, hot, true
}

func validateVolumes(machine string, vols []hypervisor.Volume) error {
	seen := map[string]bool{}
	for _, v := range vols {
		if errs := validation.IsDNS1123Label(v.Name); len(errs) > 0 || slices.Contains(reservedDisks, v.Name) {
			return hypervisor.Permanent(fmt.Errorf("machine %s: volume name %q is not a valid disk name", machine, v.Name))
		}
		if errs := validation.IsDNS1123Subdomain(v.Claim); len(errs) > 0 {
			return hypervisor.Permanent(fmt.Errorf("machine %s: volume %s claim %q is not a valid object name", machine, v.Name, v.Claim))
		}
		if seen[v.Name] {
			return hypervisor.Permanent(fmt.Errorf("machine %s: volume name %q listed twice", machine, v.Name))
		}
		seen[v.Name] = true
	}
	return nil
}

// volumeChange computes the volumes and disks lists after attaching and
// detaching to match want. changed is false when the VirtualMachine already
// has exactly want.
func volumeChange(vm *unstructured.Unstructured, want []hypervisor.Volume) (volumes, disks []any, changed bool) {
	have := map[string]string{}
	for _, v := range templateSlice(vm, "spec", "template", "spec", "volumes") {
		if name, claim, ok := claimOf(v); ok {
			have[name] = claim
		}
	}
	wantMap := map[string]string{}
	for _, v := range want {
		wantMap[v.Name] = v.Claim
	}
	if len(have) == len(wantMap) {
		same := true
		for n, c := range wantMap {
			if have[n] != c {
				same = false
			}
		}
		if same {
			return nil, nil, false
		}
	}
	for _, v := range templateSlice(vm, "spec", "template", "spec", "volumes") {
		if _, _, ok := claimOf(v); !ok {
			volumes = append(volumes, v)
		}
	}
	for _, d := range templateSlice(vm, "spec", "template", "spec", "domain", "devices", "disks") {
		m, _ := d.(map[string]any)    //nolint:errcheck // a disk of another shape is not a hotplug disk of ours
		name, _ := m["name"].(string) //nolint:errcheck // nor is one without a string name
		if _, hot := have[name]; !hot {
			disks = append(disks, d)
		}
	}
	hotVolumes, hotDisks := dataVolumeEntries(want)
	return append(volumes, hotVolumes...), append(disks, hotDisks...), true
}

// dataVolumeEntries returns the template volume and disk entries of the data
// volumes in want, sorted by name: a hot-pluggable claim volume and a scsi
// disk of the same name for each. Create and attach both use it, so a volume
// declared at create is the same entry an attach would add.
func dataVolumeEntries(want []hypervisor.Volume) (volumes, disks []any) {
	sorted := slices.Clone(want)
	slices.SortFunc(sorted, func(a, b hypervisor.Volume) int {
		switch {
		case a.Name < b.Name:
			return -1
		case a.Name > b.Name:
			return 1
		}
		return 0
	})
	for _, v := range sorted {
		volumes = append(volumes, map[string]any{
			"name":                  v.Name,
			"persistentVolumeClaim": map[string]any{"claimName": v.Claim, "hotpluggable": true},
		})
		disks = append(disks, map[string]any{"name": v.Name, "serial": v.Name, "disk": map[string]any{"bus": "scsi"}})
	}
	return volumes, disks
}

// checkAttachable fails permanently when a volume that is being attached is a
// single-writer claim already used by another VirtualMachine, or, with
// hotplug, a claim whose volumeMode is not Block. A claim that does not exist
// yet is transient: the storage service may still be creating it.
//
// KubeVirt hot-plugs a Block claim as a raw device but expects a Filesystem
// claim to hold a disk.img, which a fresh storage volume does not. That fails
// only inside KubeVirt (the VMI volumeStatus) while the VirtualMachine patch
// succeeds, so the check runs here, before the patch. volumeMode is immutable,
// so the error is permanent. Claims already in the VirtualMachine are not
// checked again, so a Filesystem volume attached before this check stays.
// The boot claim is not hot-plugged and passes hotplug false: a Filesystem
// boot claim holding a disk.img boots.
func (a *Agent) checkAttachable(ctx context.Context, machine string, vm *unstructured.Unstructured, want []hypervisor.Volume, hotplug bool) error {
	have := map[string]bool{}
	for _, v := range templateSlice(vm, "spec", "template", "spec", "volumes") {
		if _, claim, ok := claimOf(v); ok {
			have[claim] = true
		}
	}
	var others *unstructured.UnstructuredList
	for _, v := range want {
		if have[v.Claim] {
			continue
		}
		var pvc corev1.PersistentVolumeClaim
		err := a.c.Get(ctx, client.ObjectKey{Namespace: a.cfg.Namespace, Name: v.Claim}, &pvc)
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("machine %s: claim %s for volume %s does not exist yet", machine, v.Claim, v.Name)
		}
		if err != nil {
			return fmt.Errorf("machine %s: get claim %s: %w", machine, v.Claim, err)
		}
		if hotplug {
			if err := requireBlock(machine, v, &pvc); err != nil {
				return hypervisor.Attachment(v, err)
			}
		}
		if !singleWriter(pvc.Spec.AccessModes) {
			continue
		}
		if others == nil {
			others = new(unstructured.UnstructuredList)
			others.SetGroupVersionKind(vmGVK.GroupVersion().WithKind("VirtualMachineList"))
			if err := a.c.List(ctx, others, client.InNamespace(a.cfg.Namespace)); err != nil {
				return fmt.Errorf("machine %s: list VirtualMachines: %w", machine, err)
			}
		}
		for i := range others.Items {
			o := &others.Items[i]
			if o.GetName() == machine {
				continue
			}
			for _, ov := range templateSlice(o, "spec", "template", "spec", "volumes") {
				// Any claim volume counts, including another machine's
				// boot-from-claim root disk.
				if _, claim, _, ok := pvcOf(ov); ok && claim == v.Claim {
					err := fmt.Errorf("machine %s: claim %s is single-writer and already attached to %s",
						machine, v.Claim, o.GetName())
					if hotplug {
						return hypervisor.Attachment(v, err)
					}
					return hypervisor.Permanent(err)
				}
			}
		}
	}
	return nil
}

// attachableAtCreate checks each data volume of a machine about to be
// created and returns the ones the create should carry. A transient error
// (a claim does not exist yet) returns no volumes and that error: the
// reconciler converges again and update hot-plugs them all then. A
// permanent refusal drops only the refused volume, so the others still go
// into the create, and the first refusal is returned so the reconciler
// fails that attachment and leaves the machine running, as a refused
// attach to a running machine does.
func (a *Agent) attachableAtCreate(
	ctx context.Context, machine string, want []hypervisor.Volume,
) ([]hypervisor.Volume, error) {
	fresh := &unstructured.Unstructured{Object: map[string]any{}}
	var ok []hypervisor.Volume
	var refused error
	for _, v := range want {
		err := a.checkAttachable(ctx, machine, fresh, []hypervisor.Volume{v}, true)
		switch {
		case err == nil:
			ok = append(ok, v)
		case hypervisor.IsPermanent(err):
			if refused == nil {
				refused = err
			}
		default:
			return nil, err
		}
	}
	return ok, refused
}

// singleWriter reports whether a claim can be attached to one machine at a
// time: it offers no shared access mode.
func singleWriter(modes []corev1.PersistentVolumeAccessMode) bool {
	for _, m := range modes {
		if m == corev1.ReadWriteMany || m == corev1.ReadOnlyMany {
			return false
		}
	}
	return true
}

// requireBlock fails permanently unless the claim's volumeMode is Block. An
// unset volumeMode is Filesystem, the Kubernetes default.
func requireBlock(machine string, v hypervisor.Volume, pvc *corev1.PersistentVolumeClaim) error {
	mode := corev1.PersistentVolumeFilesystem
	if pvc.Spec.VolumeMode != nil {
		mode = *pvc.Spec.VolumeMode
	}
	if mode == corev1.PersistentVolumeBlock {
		return nil
	}
	shown := string(mode)
	if pvc.Spec.VolumeMode == nil {
		shown += " (unset, the default)"
	}
	return hypervisor.Permanent(fmt.Errorf(
		"machine %s: volume %s: claim %s has volumeMode %s, but hot-plugging needs volumeMode Block; recreate the volume as a Block volume and attach it again",
		machine, v.Name, v.Claim, shown))
}
