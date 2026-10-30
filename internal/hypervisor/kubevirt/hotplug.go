package kubevirt

import (
	"context"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

var _ hypervisor.VolumeObserver = (*Agent)(nil)

// volumeReady is the KubeVirt VolumePhase of a volume the guest sees: for a
// hot-plugged volume, the attachment pod mounted it and virt-handler added
// the disk to the domain.
const volumeReady = "Ready"

// MachineVolumes implements hypervisor.VolumeObserver. It only reads.
//
// A data volume is a hot-pluggable volume in the VirtualMachine template. On
// a running machine it is plugged once the VirtualMachineInstance's
// status.volumeStatus entry for it has phase Ready, and plugging until then,
// including while the instance does not exist yet. A hot-plugged entry in
// status.volumeStatus that the template no longer lists is unplugging.
//
// On a stopped machine (runStrategy Halted) every template volume is
// plugged: KubeVirt attaches it at the next boot, as EC2 attaches a volume
// to a stopped instance at once. Nothing is unplugging there.
func (a *Agent) MachineVolumes(ctx context.Context, name string) (map[string]hypervisor.VolumePhase, error) {
	vm := new(unstructured.Unstructured)
	vm.SetGroupVersionKind(vmGVK)
	err := a.c.Get(ctx, client.ObjectKey{Namespace: a.cfg.Namespace, Name: name}, vm)
	if apierrors.IsNotFound(err) {
		return map[string]hypervisor.VolumePhase{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("machine %s: get VirtualMachine: %w", name, err)
	}
	if vm.GetLabels()[LabelManaged] != "true" {
		return nil, hypervisor.Permanent(
			fmt.Errorf("machine %s: a VirtualMachine with that name exists and is not managed here", name))
	}
	listed := map[string]bool{}
	for _, v := range templateSlice(vm, "spec", "template", "spec", "volumes") {
		if n, _, ok := claimOf(v); ok {
			listed[n] = true
		}
	}
	out := make(map[string]hypervisor.VolumePhase, len(listed))
	// A runStrategy of another type reads as not halted.
	run, _, _ := unstructured.NestedString(vm.Object, "spec", "runStrategy") //nolint:errcheck // see above
	if run == runHalted {
		for n := range listed {
			out[n] = hypervisor.VolumePlugged
		}
		return out, nil
	}

	vmi := new(unstructured.Unstructured)
	vmi.SetGroupVersionKind(vmiGVK)
	err = a.c.Get(ctx, client.ObjectKey{Namespace: a.cfg.Namespace, Name: name}, vmi)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("machine %s: get virtual machine instance: %w", name, err)
	}
	phase := map[string]string{}
	if err == nil {
		for _, s := range templateSlice(vmi, "status", "volumeStatus") {
			m, ok := s.(map[string]any)
			if !ok {
				continue
			}
			n, _ := m["name"].(string) //nolint:errcheck // an entry without a string name is not one of ours
			if n == "" || slices.Contains(reservedDisks, n) {
				continue
			}
			p, _ := m["phase"].(string) //nolint:errcheck // a missing phase reads as not ready
			phase[n] = p
			if _, hot := m["hotplugVolume"]; hot && !listed[n] {
				out[n] = hypervisor.VolumeUnplugging
			}
		}
	}
	for n := range listed {
		if phase[n] == volumeReady {
			out[n] = hypervisor.VolumePlugged
		} else {
			out[n] = hypervisor.VolumePlugging
		}
	}
	return out, nil
}
