package kubevirt

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

func claim(t *testing.T, c client.Client, ns, name string, mode corev1.PersistentVolumeAccessMode) {
	t.Helper()
	err := c.Create(context.Background(), &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{mode},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

// diskNames lists the template's volume names and disk names.
func diskNames(t *testing.T, c client.Client, ns, name string) (vols, disks []string) {
	t.Helper()
	vm, err := get(t, c, "VirtualMachine", ns, name)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range templateSlice(vm, "spec", "template", "spec", "volumes") {
		vols = append(vols, v.(map[string]any)["name"].(string))
	}
	for _, d := range templateSlice(vm, "spec", "template", "spec", "domain", "devices", "disks") {
		disks = append(disks, d.(map[string]any)["name"].(string))
	}
	return vols, disks
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestVolumesAttachAndDetachOnARunningMachine(t *testing.T) {
	c, ns, w := newClient(t)
	ctx := context.Background()
	a := New(c, Config{Namespace: ns})
	claim(t, c, ns, "data-1", corev1.ReadWriteOnce)
	s := spec("i-0123456789abcdef0", hypervisor.PowerOn)
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}

	s.Volumes = []hypervisor.Volume{{Name: "data1", Claim: "data-1"}}
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	vols, disks := diskNames(t, c, ns, s.Name)
	if !equal(vols, []string{"rootdisk", "data1"}) || !equal(disks, []string{"rootdisk", "data1"}) {
		t.Fatalf("after attach volumes %v disks %v", vols, disks)
	}
	vm, _ := get(t, c, "VirtualMachine", ns, s.Name)
	v := templateSlice(vm, "spec", "template", "spec", "volumes")[1].(map[string]any)["persistentVolumeClaim"].(map[string]any)
	if v["claimName"] != "data-1" || v["hotpluggable"] != true {
		t.Fatalf("volume source = %v", v)
	}
	d := templateSlice(vm, "spec", "template", "spec", "domain", "devices", "disks")[1].(map[string]any)
	if d["disk"].(map[string]any)["bus"] != "scsi" || d["serial"] != "data1" {
		t.Fatalf("disk = %v", d)
	}

	before := w.n.Load()
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	if got := w.n.Load() - before; got != 0 {
		t.Fatalf("repeating the attach wrote %d times, want 0", got)
	}

	s.Volumes = nil
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	vols, disks = diskNames(t, c, ns, s.Name)
	if !equal(vols, []string{"rootdisk"}) || !equal(disks, []string{"rootdisk"}) {
		t.Fatalf("after detach volumes %v disks %v", vols, disks)
	}
	before = w.n.Load()
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	if got := w.n.Load() - before; got != 0 {
		t.Fatalf("repeating the detach wrote %d times, want 0", got)
	}
}

func TestAttachPatchesOnceAndKeepsCloudInit(t *testing.T) {
	c, ns, w := newClient(t)
	ctx := context.Background()
	a := New(c, Config{Namespace: ns})
	claim(t, c, ns, "data-1", corev1.ReadWriteOnce)
	claim(t, c, ns, "data-2", corev1.ReadWriteOnce)
	s := spec("i-0123456789abcdef0", hypervisor.PowerOff)
	s.AuthorizedKeys = []string{"ssh-ed25519 AAAA test"}
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	before := w.n.Load()
	s.Volumes = []hypervisor.Volume{{Name: "b", Claim: "data-2"}, {Name: "a", Claim: "data-1"}}
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	if got := w.n.Load() - before; got != 1 {
		t.Fatalf("attach wrote %d times, want 1 patch", got)
	}
	vols, disks := diskNames(t, c, ns, s.Name)
	want := []string{"rootdisk", "cloudinitdisk", "a", "b"}
	if !equal(vols, want) || !equal(disks, want) {
		t.Fatalf("volumes %v disks %v, want %v", vols, disks, want)
	}

	// Swapping one volume for another is one patch and leaves the rest.
	before = w.n.Load()
	s.Volumes = []hypervisor.Volume{{Name: "a", Claim: "data-1"}}
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	if got := w.n.Load() - before; got != 1 {
		t.Fatalf("detach wrote %d times, want 1 patch", got)
	}
	vols, _ = diskNames(t, c, ns, s.Name)
	if !equal(vols, []string{"rootdisk", "cloudinitdisk", "a"}) {
		t.Fatalf("volumes %v", vols)
	}
}

func TestSingleWriterClaimIsRefusedWhileAttachedElsewhere(t *testing.T) {
	c, ns, _ := newClient(t)
	ctx := context.Background()
	a := New(c, Config{Namespace: ns})
	claim(t, c, ns, "data-1", corev1.ReadWriteOnce)
	one, two := spec("i-0123456789abcdef0", hypervisor.PowerOn), spec("i-0123456789abcdef1", hypervisor.PowerOn)
	one.Volumes = []hypervisor.Volume{{Name: "data", Claim: "data-1"}}
	two.Volumes = one.Volumes
	for _, s := range []hypervisor.MachineSpec{one, two} {
		withoutVolumes := s
		withoutVolumes.Volumes = nil
		if err := a.ConvergeMachine(ctx, withoutVolumes); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.ConvergeMachine(ctx, one); err != nil {
		t.Fatal(err)
	}
	err := a.ConvergeMachine(ctx, two)
	if err == nil || !hypervisor.IsPermanent(err) {
		t.Fatalf("second attach = %v, want a permanent error", err)
	}
	if vols, _ := diskNames(t, c, ns, two.Name); !equal(vols, []string{"rootdisk"}) {
		t.Fatalf("refused attach changed the machine: %v", vols)
	}

	// Detach from the first, and the second may take the claim.
	one.Volumes = nil
	if err := a.ConvergeMachine(ctx, one); err != nil {
		t.Fatal(err)
	}
	if err := a.ConvergeMachine(ctx, two); err != nil {
		t.Fatalf("attach after the other detached: %v", err)
	}
}

func TestSharedClaimMayAttachToTwoMachines(t *testing.T) {
	c, ns, _ := newClient(t)
	ctx := context.Background()
	a := New(c, Config{Namespace: ns})
	claim(t, c, ns, "shared", corev1.ReadWriteMany)
	for _, n := range []string{"i-0123456789abcdef0", "i-0123456789abcdef1"} {
		s := spec(n, hypervisor.PowerOn)
		if err := a.ConvergeMachine(ctx, s); err != nil {
			t.Fatal(err)
		}
		s.Volumes = []hypervisor.Volume{{Name: "data", Claim: "shared"}}
		if err := a.ConvergeMachine(ctx, s); err != nil {
			t.Fatalf("%s: %v", n, err)
		}
	}
}

func TestMissingClaimIsTransientAndBadNamesPermanent(t *testing.T) {
	c, ns, _ := newClient(t)
	ctx := context.Background()
	a := New(c, Config{Namespace: ns})
	s := spec("i-0123456789abcdef0", hypervisor.PowerOn)
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	s.Volumes = []hypervisor.Volume{{Name: "data", Claim: "not-yet"}}
	if err := a.ConvergeMachine(ctx, s); err == nil || hypervisor.IsPermanent(err) {
		t.Fatalf("missing claim = %v, want a transient error", err)
	}
	for _, v := range [][]hypervisor.Volume{
		{{Name: "Bad_Name", Claim: "c"}},
		{{Name: "rootdisk", Claim: "c"}},
		{{Name: "a", Claim: "c"}, {Name: "a", Claim: "d"}},
		{{Name: "a", Claim: "UPPER"}},
	} {
		s.Volumes = v
		if err := a.ConvergeMachine(ctx, s); err == nil || !hypervisor.IsPermanent(err) {
			t.Fatalf("volumes %v = %v, want a permanent error", v, err)
		}
	}
}
