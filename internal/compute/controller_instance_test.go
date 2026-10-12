package compute_test

import (
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
)

const controllerNode = "host-a"

func controllerDriven(power compute.Power) compute.Instance {
	return compute.Instance{Spec: compute.InstanceSpec{
		ID:         ok(compute.ParseInstanceID("web-1")),
		Generation: 2,
		Project:    "p",
		Zone:       "z",
		Power:      power,
	}}
}

func TestApplyReport(t *testing.T) {
	node := controllerNode
	now := t0
	t.Run("ready at the spec generation converges", func(t *testing.T) {
		st, changed, err := compute.ApplyReport(controllerDriven(compute.PowerRunning),
			compute.InstanceReport{Node: node, Generation: 2, State: compute.ReportReady, Power: compute.PowerRunning}, now)
		if err != nil || !changed || st.Observed.Generation != 2 || st.Observed.Failure != nil {
			t.Fatalf("%+v %v %v", st, changed, err)
		}
	})
	t.Run("reconciling records the report and does not advance", func(t *testing.T) {
		st, changed, err := compute.ApplyReport(controllerDriven(compute.PowerRunning),
			compute.InstanceReport{Node: node, Generation: 2, State: compute.ReportReconciling, Message: "booting"}, now)
		if err != nil || !changed || st.Observed.Generation != 0 || st.Report.Message != "booting" {
			t.Fatalf("%+v %v %v", st, changed, err)
		}
	})
	t.Run("error parks the generation", func(t *testing.T) {
		st, _, err := compute.ApplyReport(controllerDriven(compute.PowerRunning),
			compute.InstanceReport{Node: node, Generation: 2, State: compute.ReportError, Message: "no disk"}, now)
		if err != nil || !st.Observed.Parked(2) || st.Observed.Failure.Message != "no disk" || st.Observed.Failure.Code != compute.CodeInternal {
			t.Fatalf("%+v %v", st, err)
		}
	})
	t.Run("error carries the report code", func(t *testing.T) {
		st, _, err := compute.ApplyReport(controllerDriven(compute.PowerRunning),
			compute.InstanceReport{Node: node, Generation: 2, State: compute.ReportError, Message: "not Block", Code: compute.CodeFailedPrecondition}, now)
		if err != nil || !st.Observed.Parked(2) || st.Observed.Failure.Code != compute.CodeFailedPrecondition {
			t.Fatalf("%+v %v", st, err)
		}
	})
	t.Run("terminated converges only on DELETING", func(t *testing.T) {
		inst := controllerDriven(compute.PowerTerminated)
		st, _, _ := compute.ApplyReport(inst, compute.InstanceReport{Node: node, Generation: 2, State: compute.ReportReady}, now)
		if st.Observed.Generation != 0 {
			t.Fatalf("READY converged a delete: %+v", st)
		}
		st, _, _ = compute.ApplyReport(inst, compute.InstanceReport{Node: node, Generation: 2, State: compute.ReportDeleting}, now)
		inst.Status = st
		if st.Observed.Generation != 2 || !inst.Removed() {
			t.Fatalf("DELETING did not remove: %+v", st)
		}
	})
	t.Run("a report ahead of the spec is aborted", func(t *testing.T) {
		_, _, err := compute.ApplyReport(controllerDriven(compute.PowerRunning),
			compute.InstanceReport{Node: node, Generation: 3, State: compute.ReportReady}, now)
		if compute.CodeOf(err) != compute.CodeAborted {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("a stale report changes nothing", func(t *testing.T) {
		inst := controllerDriven(compute.PowerRunning)
		inst.Status.Observed.Generation = 2
		_, changed, err := compute.ApplyReport(inst, compute.InstanceReport{Node: node, Generation: 1, State: compute.ReportReady}, now)
		if err != nil || changed {
			t.Fatalf("changed %v err %v", changed, err)
		}
	})
	t.Run("a missing state is invalid", func(t *testing.T) {
		_, _, err := compute.ApplyReport(controllerDriven(compute.PowerRunning), compute.InstanceReport{Node: node, Generation: 2}, now)
		if compute.CodeOf(err) != compute.CodeInvalid {
			t.Fatalf("got %v", err)
		}
	})
}

func createParams() compute.CreateInstanceParams {
	return compute.CreateInstanceParams{
		ID: "web-1", Project: "p", Zone: "z", MachineType: "vws.small", Power: compute.PowerRunning,
		Volumes: []compute.VolumeAttachment{{Volume: ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/root")), DeviceName: "vda", Boot: true}},
		NICs:    []compute.NetworkInterface{{DeviceName: "eth0", Subnet: "projects/p/regions/r/vpcs/v/subnets/s"}},
	}
}

func TestCreateInstanceValidation(t *testing.T) {
	mutate := map[string]func(*compute.CreateInstanceParams){
		"no boot volume": func(p *compute.CreateInstanceParams) { p.Volumes[0].Boot = false },
		"two boot volumes": func(p *compute.CreateInstanceParams) {
			p.Volumes = append(p.Volumes, p.Volumes[0])
			p.Volumes[1].DeviceName = "vdb"
		},
		"duplicate device": func(p *compute.CreateInstanceParams) {
			p.Volumes = append(p.Volumes, p.Volumes[0])
			p.Volumes[1].Boot = false
		},
		"volume in another zone": func(p *compute.CreateInstanceParams) {
			p.Volumes[0].Volume = ok(compute.ParseVolumeRef("projects/p/zones/other/volumes/root"))
		},
		"power unspecified":  func(p *compute.CreateInstanceParams) { p.Power = 0 },
		"power terminated":   func(p *compute.CreateInstanceParams) { p.Power = compute.PowerTerminated },
		"bad machine type":   func(p *compute.CreateInstanceParams) { p.MachineType = "N1" },
		"duplicate nic":      func(p *compute.CreateInstanceParams) { p.NICs = append(p.NICs, p.NICs[0]) },
		"user data too long": func(p *compute.CreateInstanceParams) { p.UserData = string(make([]byte, compute.MaxUserDataBytes+1)) },
	}
	for name, m := range mutate {
		t.Run(name, func(t *testing.T) {
			p := createParams()
			m(&p)
			if _, err := compute.NewCreateInstance(p); compute.CodeOf(err) != compute.CodeInvalid {
				t.Fatalf("got %v", err)
			}
		})
	}
	if _, err := compute.NewCreateInstance(createParams()); err != nil {
		t.Fatal(err)
	}
}

func TestCreateInstancePlan(t *testing.T) {
	in := ok(compute.NewCreateInstance(createParams()))
	ch, err := compute.Plan(newView(), envNow(), in)
	if err != nil || !ch.Write {
		t.Fatalf("%+v %v", ch, err)
	}
	spec := ch.Spec.(compute.InstanceSpec)
	if spec.Generation != 1 || !spec.ControllerDriven() || spec.Image.String() != "" || spec.Type.Name() != "vws.small" {
		t.Fatalf("spec %+v", spec)
	}
	existing := compute.Instance{Spec: spec}
	v := newView().with(existing)

	t.Run("same request on a live instance changes nothing", func(t *testing.T) {
		ch, err := compute.Plan(v, envNow(), in)
		if err != nil || ch.Write || ch.Spec.Gen() != 1 {
			t.Fatalf("%+v %v", ch, err)
		}
	})
	t.Run("a different spec is already exists", func(t *testing.T) {
		p := createParams()
		p.UserData = "#!/bin/sh"
		_, err := compute.Plan(v, envNow(), ok(compute.NewCreateInstance(p)))
		if compute.CodeOf(err) != compute.CodeAlreadyExists {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("a terminated instance keeps its id", func(t *testing.T) {
		gone := existing
		gone.Spec.Power = compute.PowerTerminated
		_, err := compute.Plan(newView().with(gone), envNow(), in)
		if compute.CodeOf(err) != compute.CodeAlreadyExists {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("fingerprint tracks every field", func(t *testing.T) {
		base := compute.FingerprintOf(in)
		p := createParams()
		p.NICs[0].SecurityGroups = []string{"projects/p/regions/r/vpcs/v/securityGroups/sg"}
		if compute.FingerprintOf(ok(compute.NewCreateInstance(p))) == base {
			t.Fatal("security group did not change the fingerprint")
		}
		if compute.FingerprintOf(ok(compute.NewCreateInstance(createParams()))) != base {
			t.Fatal("fingerprint is not deterministic")
		}
	})
}
