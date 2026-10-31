package kubevirt

import (
	"reflect"
	"strings"
	"testing"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

// TestNetworksFollowDeviceIndexOrder: a direct interface at device index 0,
// the overlay at 1, and a second direct network with a fixed address at 2
// render as bridge interfaces and networks in that order. The direct ones
// name their NetworkAttachmentDefinition vws-direct-<name>, and Kube-OVN gets
// the overlay logical switch plus each direct provider's logical switch and
// the fixed address under <provider>.kubernetes.io/ip_address.
func TestNetworksFollowDeviceIndexOrder(t *testing.T) {
	devs, nets, ann, err := networks("i-1", "vws-vms", []hypervisor.Interface{
		{Name: "nic0", Direct: "prod"},
		{Name: "nic1", Subnet: "subnet-a"},
		{Name: "nic2", Direct: "lab", Mode: hypervisor.ModeBridge, IPv4: "192.168.200.30"},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantDevs := []any{
		map[string]any{"name": "nic0", "bridge": map[string]any{}},
		map[string]any{"name": "nic1", "bridge": map[string]any{}},
		map[string]any{"name": "nic2", "bridge": map[string]any{}},
	}
	wantNets := []any{
		map[string]any{"name": "nic0", "multus": map[string]any{"networkName": "vws-direct-prod"}},
		map[string]any{"name": "nic1", "pod": map[string]any{}},
		map[string]any{"name": "nic2", "multus": map[string]any{"networkName": "vws-direct-lab"}},
	}
	wantAnn := map[string]any{
		"ovn.kubernetes.io/logical_switch":                         "subnet-a",
		"vws-direct-prod.vws-vms.ovn.kubernetes.io/logical_switch": "vws-direct-prod",
		"vws-direct-lab.vws-vms.ovn.kubernetes.io/logical_switch":  "vws-direct-lab",
		"vws-direct-lab.vws-vms.ovn.kubernetes.io/ip_address":      "192.168.200.30",
	}
	if !reflect.DeepEqual(devs, wantDevs) {
		t.Errorf("interfaces = %v, want %v", devs, wantDevs)
	}
	if !reflect.DeepEqual(nets, wantNets) {
		t.Errorf("networks = %v, want %v", nets, wantNets)
	}
	if !reflect.DeepEqual(ann, wantAnn) {
		t.Errorf("annotations = %v, want %v", ann, wantAnn)
	}
}

// TestNetworksWithoutOverlay: a machine whose only interface is direct has no
// pod network and no overlay logical switch.
func TestNetworksWithoutOverlay(t *testing.T) {
	_, nets, ann, err := networks("i-1", "ns", []hypervisor.Interface{{Name: "nic0", Direct: "prod"}})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"name": "nic0", "multus": map[string]any{"networkName": "vws-direct-prod"}}
	if len(nets) != 1 || !reflect.DeepEqual(nets[0], want) {
		t.Fatalf("networks = %v, want only the direct one", nets)
	}
	if _, ok := ann[AnnotationLogicalSwitch]; ok {
		t.Fatalf("annotations = %v, want no overlay logical switch", ann)
	}
}

// TestNetworksEmptyRendersNothing: no interface list leaves KubeVirt's
// default pod interface alone.
func TestNetworksEmptyRendersNothing(t *testing.T) {
	devs, nets, ann, err := networks("i-1", "ns", nil)
	if err != nil || devs != nil || nets != nil || ann != nil {
		t.Fatalf("networks(nil) = %v, %v, %v, %v; want all nil", devs, nets, ann, err)
	}
}

type iface = hypervisor.Interface

// TestNetworksRefuseInvalidInterfaces: every malformed or unsupported
// interface is a permanent error naming the problem, including the SR-IOV
// and macvtap extension points.
func TestNetworksRefuseInvalidInterfaces(t *testing.T) {
	cases := []struct {
		name   string
		ifaces []iface
		want   string
	}{
		{"sriov", []iface{{Name: "nic0", Direct: "prod", Mode: "sriov"}}, `direct mode "sriov" is not supported`},
		{"macvtap", []iface{{Name: "nic0", Direct: "prod", Mode: "macvtap"}}, `direct mode "macvtap" is not supported`},
		{"two overlays", []iface{{Name: "nic0", Subnet: "a"}, {Name: "nic1", Subnet: "b"}}, "more than one overlay"},
		{"repeated name", []iface{{Name: "nic0", Direct: "a"}, {Name: "nic0", Direct: "b"}}, "not a unique DNS-1123 label"},
		{"bad name", []iface{{Name: "Nic_0", Direct: "a"}}, "not a unique DNS-1123 label"},
		{"subnet and direct", []iface{{Name: "nic0", Subnet: "a", Direct: "b"}}, "exactly one of"},
		{"neither", []iface{{Name: "nic0"}}, "exactly one of"},
		{"bad direct name", []iface{{Name: "nic0", Direct: "Prod"}}, `direct network "Prod" is not a valid name`},
		{"direct twice", []iface{{Name: "nic0", Direct: "a"}, {Name: "nic1", Direct: "a"}}, "attached twice"},
		{"bad subnet", []iface{{Name: "nic0", Subnet: "Subnet_A"}}, "not a valid Kube-OVN Subnet name"},
		{"overlay fixed address", []iface{{Name: "nic0", Subnet: "a", IPv4: "10.0.0.5"}}, "takes no mode or fixed address"},
		{"not an address", []iface{{Name: "nic0", Direct: "a", IPv4: "10.0.0"}}, "is not an IPv4 address"},
		{"IPv6 address", []iface{{Name: "nic0", Direct: "a", IPv4: "fd00::5"}}, "is not an IPv4 address"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, _, err := networks("i-1", "ns", c.ifaces)
			if !hypervisor.IsPermanent(err) || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want permanent %q", err, c.want)
			}
		})
	}
}
