package reconciler_test

import (
	"context"
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// TestDrivenReportCarriesMachineAddress: the READY report names the first
// NIC by its device name with the address the agent sees; an instance whose
// address shows only later gets it on a later sweep; one without a NIC gets
// no interface.
func TestDrivenReportCarriesMachineAddress(t *testing.T) {
	const subnet = "projects/p/regions/r/vpcs/vpc-1/subnets/subnet-a"
	eachStore(t, func(h *harness) {
		drivenHarness(h)
		create := func(id string, nics ...compute.NetworkInterface) compute.Intent {
			return ok(compute.NewCreateInstance(compute.CreateInstanceParams{
				ID: id, Project: "p", Zone: "z", MachineType: "vws.small", Power: compute.PowerRunning, NICs: nics,
				Volumes: []compute.VolumeAttachment{{Volume: ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/root-" + id)), DeviceName: "vda", Boot: true}},
			}))
		}
		nic := compute.NetworkInterface{DeviceName: "nic0", Subnet: subnet}
		h.submit("early", create("early", nic))
		h.submit("late", create("late", nic))
		h.submit("bare", create("bare"))
		h.agent.SetIPv4("early", "172.31.76.2")
		h.agent.SetIPv4("bare", "172.31.76.9")
		h.sweep()
		ifaces := func(id string) []compute.ObservedInterface {
			inst, err := h.repo.Instance(context.Background(), ok(compute.ParseInstanceID(id)))
			if err != nil || inst.Status.Report.State != compute.ReportReady {
				h.t.Fatalf("%s = %+v, %v; want READY", id, inst.Status.Report, err)
			}
			return inst.Status.Report.Interfaces
		}
		want := func(ip string) compute.ObservedInterface {
			return compute.ObservedInterface{DeviceName: "nic0", Subnet: subnet, PrivateIPv4: ip}
		}
		if got := ifaces("early"); len(got) != 1 || got[0] != want("172.31.76.2") {
			h.t.Fatalf("early interfaces = %+v", got)
		}
		if got := ifaces("late"); len(got) != 0 {
			h.t.Fatalf("late interfaces before an address = %+v", got)
		}
		if got := ifaces("bare"); len(got) != 0 {
			h.t.Fatalf("an instance without a NIC got %+v", got)
		}
		h.agent.SetIPv4("late", "172.31.76.3")
		h.sweep()
		if got := ifaces("late"); len(got) != 1 || got[0] != want("172.31.76.3") {
			h.t.Fatalf("late interfaces after its address showed = %+v", got)
		}
	})
}
