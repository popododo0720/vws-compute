package kubevirt

import (
	"context"
	"maps"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

// hotplugStatus is one status.volumeStatus entry of a hot-plugged volume,
// in the shape KubeVirt writes it.
func hotplugStatus(name, phase string) map[string]any {
	return map[string]any{
		"name": name, "target": "sda", "phase": phase, "reason": "SuccessfulMount",
		"hotplugVolume": map[string]any{"attachPodName": "hp-volume-" + name, "attachPodUID": "uid-" + name},
	}
}

// setVolumeStatus replaces the VirtualMachineInstance's status.volumeStatus
// and keeps the rest of its status.
func setVolumeStatus(ctx context.Context, t *testing.T, c client.Client, ns, name string, entries ...map[string]any) {
	t.Helper()
	vmi := new(unstructured.Unstructured)
	vmi.SetGroupVersionKind(vmiGVK)
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, vmi); err != nil {
		t.Fatal(err)
	}
	list := make([]any, 0, len(entries)+1)
	// The root disk has an entry too, which is not a data volume.
	list = append(list, map[string]any{"name": "rootdisk", "target": "vda", "phase": "Ready"})
	for _, e := range entries {
		list = append(list, e)
	}
	if err := unstructured.SetNestedSlice(vmi.Object, list, "status", "volumeStatus"); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(ctx, vmi); err != nil {
		t.Fatal(err)
	}
}

func wantPhases(ctx context.Context, t *testing.T, a *Agent, name string, want map[string]hypervisor.VolumePhase) {
	t.Helper()
	got, err := a.MachineVolumes(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(got, want) {
		t.Fatalf("MachineVolumes = %v, want %v", got, want)
	}
}

// TestHotplugIsPluggedOnlyOnceKubeVirtSaysReady: a converged attach only
// means the VirtualMachine lists the volume. The volume is plugged only once
// the VMI's volumeStatus has it Ready; Pending, AttachedToNode and
// MountedToPod are still plugging. After the detach patch it is unplugging
// while the VMI still has it, and gone once KubeVirt drops it. Observing
// never writes.
func TestHotplugIsPluggedOnlyOnceKubeVirtSaysReady(t *testing.T) {
	c, ns, w := newClient(t)
	standardImage(t, c, ns)
	ctx := context.Background()
	a := New(c, Config{Namespace: ns})
	claim(t, c, ns, "data-1", corev1.ReadWriteOnce)
	s := spec("i-0123456789abcdef0", hypervisor.PowerOn)
	wantPhases(ctx, t, a, s.Name, map[string]hypervisor.VolumePhase{})
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	s.Volumes = []hypervisor.Volume{{Name: "data1", Claim: "data-1"}}
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}

	before := w.n.Load()
	plugging := map[string]hypervisor.VolumePhase{"data1": hypervisor.VolumePlugging}
	wantPhases(ctx, t, a, s.Name, plugging)
	for _, p := range []string{"Pending", "Bound", "AttachedToNode", "MountedToPod"} {
		setVolumeStatus(ctx, t, c, ns, s.Name, hotplugStatus("data1", p))
		wantPhases(ctx, t, a, s.Name, plugging)
	}
	setVolumeStatus(ctx, t, c, ns, s.Name, hotplugStatus("data1", "Ready"))
	wantPhases(ctx, t, a, s.Name, map[string]hypervisor.VolumePhase{"data1": hypervisor.VolumePlugged})
	if got := w.n.Load() - before; got != 0 {
		t.Fatalf("observing wrote %d times, want 0", got)
	}

	s.Volumes = nil
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	setVolumeStatus(ctx, t, c, ns, s.Name, hotplugStatus("data1", "Detaching"))
	wantPhases(ctx, t, a, s.Name, map[string]hypervisor.VolumePhase{"data1": hypervisor.VolumeUnplugging})
	setVolumeStatus(ctx, t, c, ns, s.Name)
	wantPhases(ctx, t, a, s.Name, map[string]hypervisor.VolumePhase{})
}

// TestHotplugBeforeTheInstanceExistsIsPlugging: a running machine whose VMI
// is not there yet has its volumes plugging.
func TestHotplugBeforeTheInstanceExistsIsPlugging(t *testing.T) {
	c, ns, _ := newBareClient(t)
	standardImage(t, c, ns)
	ctx := context.Background()
	a := New(c, Config{Namespace: ns})
	claim(t, c, ns, "data-1", corev1.ReadWriteOnce)
	s := spec("i-0123456789abcdef1", hypervisor.PowerOn)
	// The first converge creates the machine, the second attaches.
	for range 2 {
		if err := a.ConvergeMachine(ctx, s); !hypervisor.IsNotReady(err) {
			t.Fatalf("converge = %v, want not ready", err)
		}
		s.Volumes = []hypervisor.Volume{{Name: "data1", Claim: "data-1"}}
	}
	wantPhases(ctx, t, a, s.Name, map[string]hypervisor.VolumePhase{"data1": hypervisor.VolumePlugging})
}

// TestStoppedMachineVolumesArePlugged: a volume attached to a stopped machine
// is in the definition it boots with, as EC2 attaches to a stopped instance
// at once.
func TestStoppedMachineVolumesArePlugged(t *testing.T) {
	c, ns, _ := newClient(t)
	standardImage(t, c, ns)
	ctx := context.Background()
	a := New(c, Config{Namespace: ns})
	claim(t, c, ns, "data-1", corev1.ReadWriteOnce)
	s := spec("i-0123456789abcdef2", hypervisor.PowerOff)
	for range 2 {
		if err := a.ConvergeMachine(ctx, s); err != nil {
			t.Fatal(err)
		}
		s.Volumes = []hypervisor.Volume{{Name: "data1", Claim: "data-1"}}
	}
	wantPhases(ctx, t, a, s.Name, map[string]hypervisor.VolumePhase{"data1": hypervisor.VolumePlugged})
}
