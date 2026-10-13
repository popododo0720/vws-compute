package reconciler_test

import (
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// TestDrivenMachineJoinsFirstNICSubnet: the agent gets the subnet ID of the
// instance's first network interface, the name vws-network gives the Kube-OVN
// Subnet. A second interface is not wired yet (vws-compute#55), and an
// instance without interfaces gets no subnet.
func TestDrivenMachineJoinsFirstNICSubnet(t *testing.T) {
	const vpc = "projects/p/regions/r/vpcs/vpc-1/subnets/"
	eachStore(t, func(h *harness) {
		drivenHarness(h)
		withNICs := func(id string, nics ...compute.NetworkInterface) compute.Intent {
			return ok(compute.NewCreateInstance(compute.CreateInstanceParams{
				ID: id, Project: "p", Zone: "z", MachineType: "vws.small", Power: compute.PowerRunning, NICs: nics,
				Volumes: []compute.VolumeAttachment{{Volume: ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/root-" + id)), DeviceName: "vda", Boot: true}},
			}))
		}
		h.submit("one", withNICs("one", compute.NetworkInterface{DeviceName: "nic0", Subnet: vpc + "subnet-a"}))
		h.submit("two", withNICs("two",
			compute.NetworkInterface{DeviceName: "nic0", Subnet: vpc + "subnet-b"},
			compute.NetworkInterface{DeviceName: "nic1", Subnet: vpc + "subnet-c"}))
		h.submit("none", withNICs("none"))
		h.sweep()
		for name, want := range map[string]string{"one": "subnet-a", "two": "subnet-b", "none": ""} {
			m, found := h.agent.Machine(name)
			if !found || m.Subnet != want {
				h.t.Fatalf("machine %s = %+v, %v; want subnet %q", name, m, found, want)
			}
		}
	})
}
