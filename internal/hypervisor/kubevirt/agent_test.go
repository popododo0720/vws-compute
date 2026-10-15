package kubevirt

import (
	"context"
	"sync"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

func spec(name string, p hypervisor.Power) hypervisor.MachineSpec {
	return hypervisor.MachineSpec{Name: name, Image: "ami-0123456789abcdef0", VCPUs: 2, MemoryMiB: 2048, Power: p}
}

// standardImage is the DataSource spec() names. A launch refuses to clone
// it when that object is missing.
func standardImage(t *testing.T, c client.Client, ns string) {
	t.Helper()
	ds := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}}
	ds.SetGroupVersionKind(dsGVK)
	ds.SetNamespace(ns)
	ds.SetName("ami-0123456789abcdef0")
	if err := c.Create(context.Background(), ds); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, c client.Client, kind, ns, name string) (*unstructured.Unstructured, error) {
	t.Helper()
	u := &unstructured.Unstructured{}
	if kind == "VirtualMachineInstance" {
		u.SetGroupVersionKind(vmiGVK)
	} else {
		u.SetGroupVersionKind(vmGVK)
	}
	return u, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, u)
}

func count(t *testing.T, c client.Client, ns string) int {
	t.Helper()
	l := &unstructured.UnstructuredList{}
	l.SetGroupVersionKind(vmGVK.GroupVersion().WithKind("VirtualMachineList"))
	if err := c.List(context.Background(), l, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	return len(l.Items)
}

func runStrategy(t *testing.T, c client.Client, ns, name string) string {
	t.Helper()
	vm, err := get(t, c, "VirtualMachine", ns, name)
	if err != nil {
		t.Fatal(err)
	}
	s, _, _ := unstructured.NestedString(vm.Object, "spec", "runStrategy")
	return s
}

func TestConvergeTwiceMakesOneObject(t *testing.T) {
	c, ns, w := newClient(t)
	standardImage(t, c, ns)
	a := New(c, Config{Namespace: ns})
	s := spec("i-0123456789abcdef0", hypervisor.PowerOn)
	s.AuthorizedKeys = []string{"ssh-ed25519 AAAA test"}
	if err := a.ConvergeMachine(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	before := w.n.Load()
	if err := a.ConvergeMachine(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if got := w.n.Load() - before; got != 0 {
		t.Fatalf("second call wrote %d times, want 0", got)
	}
	if n := count(t, c, ns); n != 1 {
		t.Fatalf("%d VirtualMachines, want 1", n)
	}
	vm, _ := get(t, c, "VirtualMachine", ns, s.Name)
	if got := runStrategy(t, c, ns, s.Name); got != "Always" {
		t.Fatalf("runStrategy %q, want Always", got)
	}
	cores, _, _ := unstructured.NestedInt64(vm.Object, "spec", "template", "spec", "domain", "cpu", "cores")
	mem, _, _ := unstructured.NestedString(vm.Object, "spec", "template", "spec", "domain", "memory", "guest")
	if cores != 2 || mem != "2048Mi" {
		t.Fatalf("cpu %d memory %q, want 2 and 2048Mi", cores, mem)
	}
	vols, _, _ := unstructured.NestedSlice(vm.Object, "spec", "template", "spec", "volumes")
	if len(vols) != 2 {
		t.Fatalf("%d volumes, want root disk and cloud-init", len(vols))
	}
}

func TestPowerFlipsWithOnePatch(t *testing.T) {
	c, ns, w := newClient(t)
	standardImage(t, c, ns)
	a := New(c, Config{Namespace: ns})
	name := "i-0123456789abcdef1"
	ctx := context.Background()
	if err := a.ConvergeMachine(ctx, spec(name, hypervisor.PowerOn)); err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		p    hypervisor.Power
		want string
	}{{hypervisor.PowerOff, "Halted"}, {hypervisor.PowerOn, "Always"}, {hypervisor.PowerOff, "Halted"}} {
		before := w.n.Load()
		if err := a.ConvergeMachine(ctx, spec(name, step.p)); err != nil {
			t.Fatal(err)
		}
		if got := w.n.Load() - before; got != 1 {
			t.Fatalf("to %s: %d writes, want 1", step.want, got)
		}
		if got := runStrategy(t, c, ns, name); got != step.want {
			t.Fatalf("runStrategy %q, want %s", got, step.want)
		}
	}
	if n := count(t, c, ns); n != 1 {
		t.Fatalf("%d VirtualMachines, want 1", n)
	}
}

func TestNewAgentDoesNotDuplicate(t *testing.T) {
	c, ns, w := newClient(t)
	standardImage(t, c, ns)
	s := spec("i-0123456789abcdef2", hypervisor.PowerOn)
	if err := New(c, Config{Namespace: ns}).ConvergeMachine(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	before := w.n.Load()
	// A restarted controller has a fresh agent and the same spec.
	if err := New(c, Config{Namespace: ns}).ConvergeMachine(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if got := w.n.Load() - before; got != 0 {
		t.Fatalf("restart wrote %d times, want 0", got)
	}
	if n := count(t, c, ns); n != 1 {
		t.Fatalf("%d VirtualMachines, want 1", n)
	}
}

func TestConcurrentConvergeMakesOneObject(t *testing.T) {
	c, ns, _ := newClient(t)
	standardImage(t, c, ns)
	s := spec("i-0123456789abcdef3", hypervisor.PowerOn)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- New(c, Config{Namespace: ns}).ConvergeMachine(context.Background(), s)
		}()
	}
	wg.Wait()
	close(errs)
	// A loser of the create race can look for the instance before the
	// stand-in controller has made it; that is the transient not-ready error.
	for err := range errs {
		if err != nil && hypervisor.IsPermanent(err) {
			t.Fatal(err)
		}
	}
	if err := New(c, Config{Namespace: ns}).ConvergeMachine(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if n := count(t, c, ns); n != 1 {
		t.Fatalf("%d VirtualMachines, want 1", n)
	}
}

func TestResizeIsOnePatch(t *testing.T) {
	c, ns, w := newClient(t)
	standardImage(t, c, ns)
	a := New(c, Config{Namespace: ns})
	s := spec("i-0123456789abcdef4", hypervisor.PowerOff)
	if err := a.ConvergeMachine(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	before := w.n.Load()
	s.VCPUs, s.MemoryMiB = 4, 4096
	if err := a.ConvergeMachine(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if got := w.n.Load() - before; got != 1 {
		t.Fatalf("resize wrote %d times, want 1", got)
	}
	vm, _ := get(t, c, "VirtualMachine", ns, s.Name)
	cores, _, _ := unstructured.NestedInt64(vm.Object, "spec", "template", "spec", "domain", "cpu", "cores")
	mem, _, _ := unstructured.NestedString(vm.Object, "spec", "template", "spec", "domain", "memory", "guest")
	if cores != 4 || mem != "4096Mi" {
		t.Fatalf("cpu %d memory %q, want 4 and 4096Mi", cores, mem)
	}
}

// newInstance stands in for the instance KubeVirt starts: Running and Ready.
func newInstance(t *testing.T, c client.Client, ns, name string) {
	t.Helper()
	setVMI(context.Background(), t, c, ns, name, "Running", "True")
}

func TestRebootOncePerEpoch(t *testing.T) {
	c, ns, w := newBareClient(t)
	standardImage(t, c, ns)
	a := New(c, Config{Namespace: ns})
	ctx := context.Background()
	name := "i-0123456789abcdef5"
	s := spec(name, hypervisor.PowerOn)
	newInstance(t, c, ns, name)
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}

	// The reboot deletes the instance, so the machine is not ready until
	// KubeVirt starts a new one.
	s.RebootEpoch = 1
	if err := a.ConvergeMachine(ctx, s); err == nil || hypervisor.IsPermanent(err) {
		t.Fatalf("reboot: got %v, want a transient not-ready error", err)
	}
	if _, err := get(t, c, "VirtualMachineInstance", ns, name); !apierrors.IsNotFound(err) {
		t.Fatalf("running instance survived the reboot: %v", err)
	}
	// KubeVirt starts a new instance. Converging the same epoch leaves it be.
	newInstance(t, c, ns, name)
	before := w.n.Load()
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	if got := w.n.Load() - before; got != 0 {
		t.Fatalf("same epoch wrote %d times, want 0", got)
	}
	if _, err := get(t, c, "VirtualMachineInstance", ns, name); err != nil {
		t.Fatalf("second call rebooted again: %v", err)
	}
}

func TestBootAppliesEpochWithoutReboot(t *testing.T) {
	c, ns, w := newBareClient(t)
	standardImage(t, c, ns)
	a := New(c, Config{Namespace: ns})
	ctx := context.Background()
	name := "i-0123456789abcdef6"
	if err := a.ConvergeMachine(ctx, spec(name, hypervisor.PowerOff)); err != nil {
		t.Fatal(err)
	}
	newInstance(t, c, ns, name) // stands in for the instance KubeVirt starts
	s := spec(name, hypervisor.PowerOn)
	s.RebootEpoch = 7
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	if _, err := get(t, c, "VirtualMachineInstance", ns, name); err != nil {
		t.Fatalf("boot also rebooted: %v", err)
	}
	before := w.n.Load()
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	if got := w.n.Load() - before; got != 0 {
		t.Fatalf("epoch was not recorded: %d writes on repeat", got)
	}
}

func TestAbsentDeletesAndIsIdempotent(t *testing.T) {
	c, ns, _ := newClient(t)
	standardImage(t, c, ns)
	a := New(c, Config{Namespace: ns})
	ctx := context.Background()
	name := "i-0123456789abcdef7"
	if err := a.ConvergeMachine(ctx, spec(name, hypervisor.PowerAbsent)); err != nil {
		t.Fatalf("absent on a missing machine: %v", err)
	}
	if err := a.ConvergeMachine(ctx, spec(name, hypervisor.PowerOn)); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := a.ConvergeMachine(ctx, spec(name, hypervisor.PowerAbsent)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := get(t, c, "VirtualMachine", ns, name); !apierrors.IsNotFound(err) {
		t.Fatalf("machine still exists: %v", err)
	}
}

func TestPermanentErrors(t *testing.T) {
	c, ns, _ := newClient(t)
	standardImage(t, c, ns)
	a := New(c, Config{Namespace: ns})
	ctx := context.Background()

	foreign := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"runStrategy": "Always"}}}
	foreign.SetGroupVersionKind(vmGVK)
	foreign.SetNamespace(ns)
	foreign.SetName("i-0123456789abcdef8")
	if err := c.Create(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	noImage := spec("i-0123456789abcdef9", hypervisor.PowerOn)
	noImage.Image = ""
	for name, s := range map[string]hypervisor.MachineSpec{
		"bad name":         spec("Not_A_Name", hypervisor.PowerOn),
		"bad power":        spec("i-0123456789abcdef9", hypervisor.Power(9)),
		"no image":         noImage,
		"foreign machine":  spec("i-0123456789abcdef8", hypervisor.PowerOn),
		"bad image id":     {Name: "i-0123456789abcdef9", Image: "Not_An_Image", VCPUs: 1, MemoryMiB: 1, Power: hypervisor.PowerOn},
		"no cpu or memory": {Name: "i-0123456789abcdef9", Image: "ami-1", Power: hypervisor.PowerOn},
	} {
		if err := a.ConvergeMachine(ctx, s); !hypervisor.IsPermanent(err) {
			t.Errorf("%s: got %v, want a permanent error", name, err)
		}
	}
	if _, err := get(t, c, "VirtualMachine", ns, "i-0123456789abcdef9"); !apierrors.IsNotFound(err) {
		t.Fatalf("a rejected spec created an object: %v", err)
	}
}
