package kubevirt

import (
	"context"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

func templateList(t *testing.T, vm *unstructured.Unstructured, path ...string) []any {
	t.Helper()
	l, _, err := unstructured.NestedSlice(vm.Object, path...)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func multusName(net any) string {
	s, _, _ := unstructured.NestedString(net.(map[string]any), "multus", "networkName")
	return s
}

func names(l []any) []string {
	var out []string
	for _, x := range l {
		out = append(out, x.(map[string]any)["name"].(string))
	}
	return out
}

// TestDirectInterfacesReachTheVirtualMachine: a machine with a direct
// interface at device index 0 and the overlay at 1 is created with both
// KubeVirt interfaces and networks in that order, the direct one on its
// Multus network, and the Kube-OVN annotations on the pod template. A second
// converge writes nothing.
func TestDirectInterfacesReachTheVirtualMachine(t *testing.T) {
	ctx := context.Background()
	c, ns, w := newClient(t)
	standardImage(t, c, ns)
	a := New(c, Config{Namespace: ns})
	s := spec("i-0123456789abcdef0", hypervisor.PowerOn)
	s.Interfaces = []hypervisor.Interface{
		{Name: "nic0", Direct: "prod", IPv4: "192.168.200.30"},
		{Name: "nic1", Subnet: "subnet-a"},
	}
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	vm, err := get(t, c, "VirtualMachine", ns, s.Name)
	if err != nil {
		t.Fatal(err)
	}
	devs := templateList(t, vm, "spec", "template", "spec", "domain", "devices", "interfaces")
	nets := templateList(t, vm, "spec", "template", "spec", "networks")
	if got := names(devs); !reflect.DeepEqual(got, []string{"nic0", "nic1"}) {
		t.Fatalf("interfaces = %v, want nic0, nic1", got)
	}
	if got := multusName(nets[0]); got != "vws-direct-prod" {
		t.Fatalf("networks[0] = %v, want multus vws-direct-prod", nets[0])
	}
	if _, ok := nets[1].(map[string]any)["pod"]; !ok {
		t.Fatalf("networks[1] = %v, want the pod network", nets[1])
	}
	provider := "vws-direct-prod." + ns + ".ovn"
	ann := templateAnnotations(t, vm)
	if ann[AnnotationLogicalSwitch] != "subnet-a" || ann[provider+".kubernetes.io/ip_address"] != "192.168.200.30" ||
		ann[provider+".kubernetes.io/logical_switch"] != "vws-direct-prod" {
		t.Fatalf("annotations = %v", ann)
	}
	before := w.n.Load()
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	if got := w.n.Load() - before; got != 0 {
		t.Fatalf("second converge: %d writes, want 0", got)
	}
}

// TestUnsupportedInterfacesCreateNothing: an SR-IOV direct interface, or a
// subnet next to an interface list, is a permanent error before anything is
// created.
func TestUnsupportedInterfacesCreateNothing(t *testing.T) {
	c, ns, _ := newClient(t)
	standardImage(t, c, ns)
	a := New(c, Config{Namespace: ns})
	s := spec("i-0123456789abcdef0", hypervisor.PowerOn)
	s.Interfaces = []hypervisor.Interface{{Name: "nic0", Direct: "prod", Mode: "sriov"}}
	err := a.ConvergeMachine(context.Background(), s)
	if !hypervisor.IsPermanent(err) || !strings.Contains(err.Error(), `direct mode "sriov" is not supported`) {
		t.Fatalf("err = %v, want permanent unsupported mode", err)
	}
	s.Interfaces[0].Mode = ""
	s.Subnet = "subnet-a"
	err = a.ConvergeMachine(context.Background(), s)
	if !hypervisor.IsPermanent(err) || !strings.Contains(err.Error(), "are exclusive") {
		t.Fatalf("err = %v, want permanent subnet and interfaces", err)
	}
	if n := count(t, c, ns); n != 0 {
		t.Fatalf("%d VirtualMachines, want 0", n)
	}
}

// TestCreateInstanceDirectNICReachesMultus drives CreateInstance with an
// overlay interface nic1 listed before a direct interface nic0 through the
// gRPC API and the reconciler. The VirtualMachine puts the direct network
// first, by device index.
func TestCreateInstanceDirectNICReachesMultus(t *testing.T) {
	ctx := context.Background()
	c, ns, _ := newClient(t)
	claim(t, c, ns, "pvc-root-1", corev1.ReadWriteOnce)
	const region = "projects/demo/regions/vws-1/vpcs/"
	_, rec := createUnreconciled(t, New(c, Config{Namespace: ns, ImageNamespace: ns}), "vws.small", "pvc-root-1",
		&computev1.NetworkInterface{DeviceName: "nic1", Subnet: region + "vpc-1/subnets/subnet-r40"},
		&computev1.NetworkInterface{DeviceName: "nic0", Subnet: region + "vpc-direct/subnets/subnet-direct-prod"})
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	vm, err := get(t, c, "VirtualMachine", ns, "web-1")
	if err != nil {
		t.Fatal(err)
	}
	nets := templateList(t, vm, "spec", "template", "spec", "networks")
	if got := names(nets); !reflect.DeepEqual(got, []string{"nic0", "nic1"}) {
		t.Fatalf("networks = %v, want nic0, nic1", got)
	}
	if got := multusName(nets[0]); got != "vws-direct-prod" {
		t.Fatalf("networks[0] = %v, want multus vws-direct-prod", nets[0])
	}
	if got := templateAnnotations(t, vm)[AnnotationLogicalSwitch]; got != "subnet-r40" {
		t.Fatalf("logical_switch = %q, want subnet-r40", got)
	}
}
