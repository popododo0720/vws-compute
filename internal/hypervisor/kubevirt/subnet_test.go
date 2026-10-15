package kubevirt

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

// templateAnnotations is the VM template's pod annotations.
func templateAnnotations(t *testing.T, vm *unstructured.Unstructured) map[string]string {
	t.Helper()
	a, _, err := unstructured.NestedStringMap(vm.Object, "spec", "template", "metadata", "annotations")
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// TestSubnetSetsLogicalSwitch: a machine with a subnet gets the Kube-OVN
// logical_switch annotation on its pod template, so the virt-launcher pod's
// interface joins that VPC subnet. Without a subnet the template has none and
// the pod stays on the cluster default network.
func TestSubnetSetsLogicalSwitch(t *testing.T) {
	ctx := context.Background()
	c, ns, _ := newClient(t)
	standardImage(t, c, ns)
	a := New(c, Config{Namespace: ns})

	s := spec("i-0123456789abcdef0", hypervisor.PowerOn)
	s.Subnet = "subnet-r40"
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	vm, err := get(t, c, "VirtualMachine", ns, s.Name)
	if err != nil {
		t.Fatal(err)
	}
	if got := templateAnnotations(t, vm)[AnnotationLogicalSwitch]; got != "subnet-r40" {
		t.Fatalf("logical_switch = %q, want subnet-r40", got)
	}
	if got, _, _ := unstructured.NestedString(vm.Object, "spec", "template", "metadata", "labels", "vws.io/instance"); got != s.Name {
		t.Fatalf("instance label = %q, want %s", got, s.Name)
	}

	plain := spec("i-0123456789abcdef1", hypervisor.PowerOn)
	if err := a.ConvergeMachine(ctx, plain); err != nil {
		t.Fatal(err)
	}
	vm, err = get(t, c, "VirtualMachine", ns, plain.Name)
	if err != nil {
		t.Fatal(err)
	}
	if got := templateAnnotations(t, vm); len(got) != 0 {
		t.Fatalf("template annotations = %v, want none without a subnet", got)
	}
}

// TestSubnetIsFixedAtCreation: the subnet is read only when the machine is
// created, like its image. A later spec with another subnet, or none, writes
// nothing and leaves the annotation, so a running machine is not moved or
// restarted.
func TestSubnetIsFixedAtCreation(t *testing.T) {
	ctx := context.Background()
	c, ns, w := newClient(t)
	standardImage(t, c, ns)
	a := New(c, Config{Namespace: ns})
	s := spec("i-0123456789abcdef0", hypervisor.PowerOn)
	s.Subnet = "subnet-a"
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	for _, sub := range []string{"subnet-b", ""} {
		before := w.n.Load()
		s.Subnet = sub
		if err := a.ConvergeMachine(ctx, s); err != nil {
			t.Fatal(err)
		}
		if got := w.n.Load() - before; got != 0 {
			t.Fatalf("subnet %q: %d writes, want 0", sub, got)
		}
	}
	vm, err := get(t, c, "VirtualMachine", ns, s.Name)
	if err != nil {
		t.Fatal(err)
	}
	if got := templateAnnotations(t, vm)[AnnotationLogicalSwitch]; got != "subnet-a" {
		t.Fatalf("logical_switch = %q, want subnet-a", got)
	}
}

// TestInvalidSubnetIsPermanent: a subnet that cannot be a Kubernetes object
// name is refused before anything is created.
func TestInvalidSubnetIsPermanent(t *testing.T) {
	c, ns, _ := newClient(t)
	s := spec("i-0123456789abcdef0", hypervisor.PowerOn)
	s.Subnet = "Subnet_R40"
	err := New(c, Config{Namespace: ns}).ConvergeMachine(context.Background(), s)
	if !hypervisor.IsPermanent(err) || !strings.Contains(err.Error(), `subnet "Subnet_R40" is not a valid Kube-OVN Subnet name`) {
		t.Fatalf("err = %v, want permanent invalid subnet", err)
	}
	if n := count(t, c, ns); n != 0 {
		t.Fatalf("%d VirtualMachines, want 0", n)
	}
}

// TestCreateInstanceSubnetReachesLogicalSwitch drives CreateInstance with the
// network interface vws-console-api sends for RunInstances (SubnetId
// subnet-r40) through the gRPC API and the reconciler. The VirtualMachine's
// pod template names Kube-OVN logical switch subnet-r40.
func TestCreateInstanceSubnetReachesLogicalSwitch(t *testing.T) {
	ctx := context.Background()
	c, ns, _ := newClient(t)
	claim(t, c, ns, "pvc-root-1", corev1.ReadWriteOnce)
	_, rec := createUnreconciled(t, New(c, Config{Namespace: ns, ImageNamespace: ns}), "vws.small", "pvc-root-1",
		&computev1.NetworkInterface{DeviceName: "nic0", Subnet: "projects/demo/regions/vws-1/vpcs/vpc-1/subnets/subnet-r40"})
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	vm, err := get(t, c, "VirtualMachine", ns, "web-1")
	if err != nil {
		t.Fatal(err)
	}
	if got := templateAnnotations(t, vm)[AnnotationLogicalSwitch]; got != "subnet-r40" {
		t.Fatalf("logical_switch = %q, want subnet-r40", got)
	}
}
