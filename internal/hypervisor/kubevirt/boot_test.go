package kubevirt

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

func bootSpec(name, claim string) hypervisor.MachineSpec {
	return hypervisor.MachineSpec{Name: name, BootClaim: claim, VCPUs: 2, MemoryMiB: 2048, Power: hypervisor.PowerOn}
}

func TestBootFromClaimUsesThePVCAndReconvergesWithoutWrites(t *testing.T) {
	c, ns, w := newClient(t)
	claim(t, c, ns, "pvc-root", corev1.ReadWriteOnce)
	a := New(c, Config{Namespace: ns, ImageNamespace: "images"})
	s := bootSpec("web", "pvc-root")
	if err := a.ConvergeMachine(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	vm, err := get(t, c, "VirtualMachine", ns, "web")
	if err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedSlice(vm.Object, "spec", "dataVolumeTemplates"); found {
		t.Fatal("a boot-from-claim machine has a dataVolumeTemplate")
	}
	root := templateSlice(vm, "spec", "template", "spec", "volumes")[0].(map[string]any)
	pvc, _ := root["persistentVolumeClaim"].(map[string]any)
	if root["name"] != "rootdisk" || pvc["claimName"] != "pvc-root" || pvc["hotpluggable"] != nil {
		t.Fatalf("root volume = %v", root)
	}
	if rs, _, _ := unstructured.NestedString(vm.Object, "spec", "runStrategy"); rs != runAlways {
		t.Fatalf("runStrategy = %q", rs)
	}

	before := w.n.Load()
	for range 2 {
		if err := a.ConvergeMachine(context.Background(), s); err != nil {
			t.Fatal(err)
		}
	}
	if got := w.n.Load(); got != before {
		t.Fatalf("re-converging wrote %d times", got-before)
	}
}

func TestBootClaimIsImmutable(t *testing.T) {
	c, ns, w := newClient(t)
	claim(t, c, ns, "pvc-a", corev1.ReadWriteOnce)
	claim(t, c, ns, "pvc-b", corev1.ReadWriteOnce)
	a := New(c, Config{Namespace: ns})
	if err := a.ConvergeMachine(context.Background(), bootSpec("web", "pvc-a")); err != nil {
		t.Fatal(err)
	}
	before := w.n.Load()
	err := a.ConvergeMachine(context.Background(), bootSpec("web", "pvc-b"))
	if !hypervisor.IsPermanent(err) {
		t.Fatalf("changing the boot claim = %v, want permanent", err)
	}
	if w.n.Load() != before {
		t.Fatal("a refused boot change wrote")
	}
}

func TestBootClaimMustExistAndBeFree(t *testing.T) {
	c, ns, _ := newClient(t)
	a := New(c, Config{Namespace: ns})

	err := a.ConvergeMachine(context.Background(), bootSpec("web", "pvc-later"))
	if err == nil || hypervisor.IsPermanent(err) {
		t.Fatalf("missing claim = %v, want transient", err)
	}
	if _, err := get(t, c, "VirtualMachine", ns, "web"); err == nil {
		t.Fatal("a VirtualMachine was created without its boot claim")
	}

	claim(t, c, ns, "pvc-later", corev1.ReadWriteOnce)
	if err := a.ConvergeMachine(context.Background(), bootSpec("web", "pvc-later")); err != nil {
		t.Fatalf("after the claim appears: %v", err)
	}
	if err := a.ConvergeMachine(context.Background(), bootSpec("other", "pvc-later")); !hypervisor.IsPermanent(err) {
		t.Fatalf("single-writer boot claim already in use = %v, want permanent", err)
	}

	none := bootSpec("x", "")
	if err := a.ConvergeMachine(context.Background(), none); !hypervisor.IsPermanent(err) {
		t.Fatalf("no image and no claim = %v, want permanent", err)
	}
	both := bootSpec("y", "pvc-later")
	both.Image = "ami-0123456789abcdef0"
	if err := a.ConvergeMachine(context.Background(), both); !hypervisor.IsPermanent(err) {
		t.Fatalf("image and claim = %v, want permanent", err)
	}
}
