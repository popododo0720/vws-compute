package kubevirt

import (
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"context"

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
	m, isMap := v.(map[string]any)
	if !isMap {
		return "", "", false
	}
	pvc, isMap := m["persistentVolumeClaim"].(map[string]any)
	if !isMap {
		return "", "", false
	}
	if hot, _ := pvc["hotpluggable"].(bool); !hot { //nolint:errcheck // an entry of another shape is not a hotplug volume of ours
		return "", "", false
	}
	name, _ = m["name"].(string)         //nolint:errcheck // an entry of another shape is not a hotplug volume of ours
	claim, _ = pvc["claimName"].(string) //nolint:errcheck // an entry of another shape is not a hotplug volume of ours
	return name, claim, true
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
		name, _ := d.(map[string]any)["name"].(string) //nolint:errcheck // a disk of another shape is not a hotplug disk of ours
		if _, hot := have[name]; !hot {
			disks = append(disks, d)
		}
	}
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
	return volumes, disks, true
}

// checkAttachable fails permanently when a volume that is being attached is a
// single-writer claim already used by another VirtualMachine. A claim that
// does not exist yet is transient: the storage service may still be creating
// it.
func (a *Agent) checkAttachable(ctx context.Context, machine string, vm *unstructured.Unstructured, want []hypervisor.Volume) error {
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
				if _, claim, ok := claimOf(ov); ok && claim == v.Claim {
					return hypervisor.Permanent(fmt.Errorf("machine %s: claim %s is single-writer and already attached to %s", machine, v.Claim, o.GetName()))
				}
			}
		}
	}
	return nil
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
