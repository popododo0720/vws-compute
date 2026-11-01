package compute_test

import (
	"encoding/hex"
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
	t.Run("ready carries an attachment failure without parking", func(t *testing.T) {
		volume, err := compute.ParseVolumeRef("projects/p/zones/z/volumes/data")
		if err != nil {
			t.Fatal(err)
		}
		boom := &compute.AttachmentFailure{
			Failure: compute.Failure{Code: compute.CodeFailedPrecondition, Message: "not Block"},
			Volumes: []compute.VolumeRef{volume},
		}
		st, _, err := compute.ApplyReport(controllerDriven(compute.PowerRunning),
			compute.InstanceReport{Node: node, Generation: 2, State: compute.ReportReady, AttachmentFailure: boom}, now)
		if err != nil || st.Observed.Parked(2) ||
			st.Observed.AttachmentFailure == nil || st.Observed.AttachmentFailure.Message != "not Block" ||
			!st.Observed.AttachmentFailure.Refused(volume) {
			t.Fatalf("%+v %v", st, err)
		}
		inst := controllerDriven(compute.PowerRunning)
		inst.Status = st
		if got := inst.Lifecycle(); got != compute.InstanceRunning {
			t.Fatalf("lifecycle = %d, want running", got)
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
	t.Run("volumes replace the stored ones and are copied", func(t *testing.T) {
		volume := ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/data"))
		vols := []compute.ObservedVolume{
			{Volume: volume, DeviceName: "/dev/sdf", State: compute.AttachAttaching, StateTime: now},
		}
		inst := controllerDriven(compute.PowerRunning)
		inst.Status.Report.Volumes = []compute.ObservedVolume{
			{Volume: volume, DeviceName: "/dev/sdg", State: compute.AttachAttached},
		}
		st, changed, err := compute.ApplyReport(inst,
			compute.InstanceReport{Node: node, Generation: 2, State: compute.ReportReady, Volumes: vols}, now)
		vols[0].State = compute.AttachAttached
		if err != nil || !changed || len(st.Report.Volumes) != 1 ||
			st.Report.Volumes[0].DeviceName != "/dev/sdf" || st.Report.Volumes[0].State != compute.AttachAttaching {
			t.Fatalf("%+v %v %v", st.Report.Volumes, changed, err)
		}
	})
	t.Run("a volume without an attach state is invalid", func(t *testing.T) {
		volume := ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/data"))
		for _, state := range []compute.AttachState{compute.AttachUnspecified, compute.AttachBusy + 1} {
			_, _, err := compute.ApplyReport(controllerDriven(compute.PowerRunning), compute.InstanceReport{
				Node: node, Generation: 2, State: compute.ReportReady,
				Volumes: []compute.ObservedVolume{{Volume: volume, DeviceName: "/dev/sdf", State: state}},
			}, now)
			if compute.CodeOf(err) != compute.CodeInvalid {
				t.Fatalf("state %d: got %v", state, err)
			}
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
		"raw user data too long": func(p *compute.CreateInstanceParams) {
			p.UserDataRaw = string(make([]byte, compute.MaxUserDataBytes+1))
		},
		"user data and raw user data": func(p *compute.CreateInstanceParams) {
			p.UserData = "#cloud-config"
			p.UserDataRaw = "\xff"
		},
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
	p := createParams()
	p.UserDataRaw = string(make([]byte, compute.MaxUserDataBytes))
	if _, err := compute.NewCreateInstance(p); err != nil {
		t.Fatalf("raw user data at the limit: %v", err)
	}
}

// nonUTF8 is user_data_raw that no proto3 string can carry: invalid UTF-8,
// a NUL, and a lone continuation byte.
const nonUTF8 = "\xff\xfe#!/bin/sh\x00\x80echo hi\n"

func TestCreateInstanceUserDataRaw(t *testing.T) {
	p := createParams()
	p.UserDataRaw = nonUTF8
	in := ok(compute.NewCreateInstance(p))
	ch, err := compute.Plan(newView(), envNow(), in)
	if err != nil || !ch.Write {
		t.Fatalf("%+v %v", ch, err)
	}
	spec := ch.Spec.(compute.InstanceSpec)
	if spec.UserDataRaw != nonUTF8 || spec.UserData != "" {
		t.Fatalf("spec user data = %q raw %q, want raw %q", spec.UserData, spec.UserDataRaw, nonUTF8)
	}
	v := newView().with(compute.Instance{Spec: spec})
	if ch, err := compute.Plan(v, envNow(), in); err != nil || ch.Write {
		t.Fatalf("same create on the live instance = %+v, %v; want no write", ch, err)
	}
	other := createParams()
	other.UserDataRaw = nonUTF8 + "x"
	_, err = compute.Plan(v, envNow(), ok(compute.NewCreateInstance(other)))
	if compute.CodeOf(err) != compute.CodeAlreadyExists {
		t.Fatalf("different raw user data on the live instance = %v, want already exists", err)
	}
	_, err = compute.Plan(v, envNow(), ok(compute.NewCreateInstance(createParams())))
	if compute.CodeOf(err) != compute.CodeAlreadyExists {
		t.Fatalf("no raw user data on the live instance = %v, want already exists", err)
	}
}

// TestCreateInstanceFingerprintCoversUserDataRaw pins the encoding: raw user
// data changes the fingerprint, and a create without it keeps the
// fingerprint that main 45461aa persisted, so stored operations still replay.
func TestCreateInstanceFingerprintCoversUserDataRaw(t *testing.T) {
	const (
		plainBefore   = "b8e58edcf9bbab31ffde38cbfe578fba312917f1d41b3d1cf4193f2f2898f2f1"
		labeledBefore = "97c07d2277703c2f44865606124fe8109300303a7762aab82b94d7bf6b3b47e9"
	)
	hexOf := func(p compute.CreateInstanceParams) string {
		fp := compute.FingerprintOf(ok(compute.NewCreateInstance(p)))
		return hex.EncodeToString(fp[:])
	}
	labeled := createParams()
	labeled.UserData = "#cloud-config"
	labeled.Labels = ok(compute.ParseLabels(map[string]string{"app": "web"}))
	if got := hexOf(createParams()); got != plainBefore {
		t.Fatalf("fingerprint without raw user data = %s, want %s", got, plainBefore)
	}
	if got := hexOf(labeled); got != labeledBefore {
		t.Fatalf("labeled fingerprint without raw user data = %s, want %s", got, labeledBefore)
	}
	seen := map[string]string{plainBefore: "none"}
	for _, raw := range []string{nonUTF8, nonUTF8 + "x", "\x00", "#cloud-config"} {
		p := createParams()
		p.UserDataRaw = raw
		got := hexOf(p)
		if prev, dup := seen[got]; dup {
			t.Fatalf("raw user data %q has the fingerprint of %q", raw, prev)
		}
		seen[got] = raw
	}
	// Raw bytes equal to the text user data are still a different request.
	text, raw := createParams(), createParams()
	text.UserData, raw.UserDataRaw = "#cloud-config", "#cloud-config"
	if hexOf(text) == hexOf(raw) {
		t.Fatal("user_data and user_data_raw with the same bytes share a fingerprint")
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
