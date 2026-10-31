package reconciler_test

import (
	"reflect"
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

// TestDrivenMachineWiresDirectInterfaces: an instance with a direct interface
// (subnet-direct-<name>) gets every interface in device index order, whatever
// the request order, and no single subnet. A direct interface keeps its fixed
// address; an overlay one is wired by subnet only. A direct-only instance has
// no overlay interface.
func TestDrivenMachineWiresDirectInterfaces(t *testing.T) {
	const vpc = "projects/p/regions/r/vpcs/vpc-1/subnets/"
	const direct = "projects/p/regions/r/vpcs/vpc-direct/subnets/subnet-direct-"
	eachStore(t, func(h *harness) {
		drivenHarness(h)
		withNICs := func(id string, nics ...compute.NetworkInterface) compute.Intent {
			return ok(compute.NewCreateInstance(compute.CreateInstanceParams{
				ID: id, Project: "p", Zone: "z", MachineType: "vws.small", Power: compute.PowerRunning, NICs: nics,
				Volumes: []compute.VolumeAttachment{{
					Volume: ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/root-" + id)), DeviceName: "vda", Boot: true,
				}},
			}))
		}
		h.submit("mixed", withNICs("mixed",
			compute.NetworkInterface{DeviceName: "nic2", Subnet: direct + "lab"},
			compute.NetworkInterface{DeviceName: "nic1", Subnet: vpc + "subnet-a", PrivateIPv4: "10.0.0.5"},
			compute.NetworkInterface{DeviceName: "nic0", Subnet: direct + "prod", PrivateIPv4: "192.168.200.30"}))
		h.submit("only", withNICs("only", compute.NetworkInterface{DeviceName: "nic0", Subnet: direct + "prod"}))
		h.sweep()
		want := map[string][]hypervisor.Interface{
			"mixed": {
				{Name: "nic0", Direct: "prod", IPv4: "192.168.200.30"},
				{Name: "nic1", Subnet: "subnet-a"},
				{Name: "nic2", Direct: "lab"},
			},
			"only": {{Name: "nic0", Direct: "prod"}},
		}
		for name, w := range want {
			m, found := h.agent.Machine(name)
			if !found || m.Subnet != "" || !reflect.DeepEqual(m.Interfaces, w) {
				h.t.Fatalf("machine %s = %+v, %v; want interfaces %+v and no subnet", name, m, found, w)
			}
		}
	})
}
