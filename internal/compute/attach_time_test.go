package compute_test

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
)

func vol(name, device string) compute.VolumeAttachment {
	ref := ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/" + name))
	return compute.VolumeAttachment{Volume: ref, DeviceName: device}
}

func attachTimeOf(t *testing.T, s compute.InstanceSpec, name string) time.Time {
	t.Helper()
	for _, v := range s.Volumes {
		if v.Volume.String() == "projects/p/zones/z/volumes/"+name {
			return v.AttachTime
		}
	}
	t.Fatalf("volume %s not in %+v", name, s.Volumes)
	return time.Time{}
}

// TestAttachTimeIsSetOnceWhenAVolumeEntersTheSpec pins vws-compute#166 AC1:
// create stamps every initial volume, an attach stamps only the new entry, a
// caller-supplied time is ignored, and later updates keep every kept entry's
// time. A device change is a new attachment.
func TestAttachTimeIsSetOnceWhenAVolumeEntersTheSpec(t *testing.T) {
	t1, t2, t3 := t0.Add(time.Minute), t0.Add(2*time.Minute), t0.Add(3*time.Minute)
	at := func(now time.Time) compute.Env { return compute.Env{Now: now, Rand: rand.Reader} }
	forged := time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)

	p := createParams()
	p.Volumes[0].AttachTime = forged
	data := vol("data", "vdb")
	data.AttachTime = forged
	p.Volumes = append(p.Volumes, data)
	in := ok(compute.NewCreateInstance(p))
	ch := ok(compute.Plan(newView(), at(t0), in))
	spec := ch.Spec.(compute.InstanceSpec)
	if got := attachTimeOf(t, spec, "root"); !got.Equal(t0) {
		t.Fatalf("boot attach time = %v, want %v", got, t0)
	}
	if got := attachTimeOf(t, spec, "data"); !got.Equal(t0) {
		t.Fatalf("initial data attach time = %v, want %v", got, t0)
	}
	if ch, err := compute.Plan(newView().with(compute.Instance{Spec: spec}), at(t1), in); err != nil || ch.Write ||
		!attachTimeOf(t, ch.Spec.(compute.InstanceSpec), "root").Equal(t0) {
		t.Fatalf("create replay = %+v %v, want the stored spec", ch, err)
	}

	update := func(cur compute.InstanceSpec, now time.Time, vols ...compute.VolumeAttachment) compute.InstanceSpec {
		t.Helper()
		in := ok(compute.NewUpdateInstanceSpec("web-1", compute.PowerRunning, vols, compute.MetadataPatch{}))
		ch, err := compute.Plan(newView().with(compute.Instance{Spec: cur}), at(now), in)
		if err != nil {
			t.Fatal(err)
		}
		return ch.Spec.(compute.InstanceSpec)
	}
	root := createParams().Volumes[0]
	root.AttachTime = forged
	extra := vol("extra", "vdc")
	extra.AttachTime = forged

	spec = update(spec, t1, root, data, extra)
	if got := attachTimeOf(t, spec, "extra"); !got.Equal(t1) {
		t.Fatalf("attached volume time = %v, want %v", got, t1)
	}
	if !attachTimeOf(t, spec, "root").Equal(t0) || !attachTimeOf(t, spec, "data").Equal(t0) {
		t.Fatalf("kept volumes changed attach time: %+v", spec.Volumes)
	}

	r := update(spec, t2, root, extra, data)
	if !attachTimeOf(t, r, "extra").Equal(t1) || !attachTimeOf(t, r, "data").Equal(t0) {
		t.Fatalf("reordering the same volumes moved attach times: %+v", r.Volumes)
	}
	stop := ok(compute.NewUpdateInstanceSpec("web-1", compute.PowerStopped,
		[]compute.VolumeAttachment{root, data, extra}, compute.MetadataPatch{}))
	stopped := ok(compute.Plan(newView().with(compute.Instance{Spec: spec}), at(t2), stop)).Spec.(compute.InstanceSpec)
	if stopped.Power != compute.PowerStopped || !attachTimeOf(t, stopped, "extra").Equal(t1) ||
		!attachTimeOf(t, stopped, "root").Equal(t0) {
		t.Fatalf("a power change moved attach times: %+v", stopped.Volumes)
	}
	power := ok(compute.NewSetPower("web-1", compute.PowerRunning, compute.MetadataPatch{}))
	started := ok(compute.Plan(newView().with(compute.Instance{Spec: stopped}), at(t3), power)).Spec.(compute.InstanceSpec)
	if !attachTimeOf(t, started, "extra").Equal(t1) {
		t.Fatalf("a start without volumes moved attach times: %+v", started.Volumes)
	}

	moved := vol("extra", "vdd")
	spec = update(started, t3, root, data, moved)
	if got := attachTimeOf(t, spec, "extra"); !got.Equal(t3) {
		t.Fatalf("volume moved to another device keeps time %v, want %v", got, t3)
	}
}

// TestReportedAttachTimeComesFromTheSpec pins vws-compute#166 AC2: the stored
// report's attach time is the spec entry's, whatever the controller sends,
// and it holds through ATTACHED, BUSY and DETACHING, after the entry left
// the spec.
func TestReportedAttachTimeComesFromTheSpec(t *testing.T) {
	forged := time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
	attached := t0.Add(-time.Hour)
	inst := controllerDriven(compute.PowerRunning)
	data := vol("data", "vdb")
	data.AttachTime = attached
	inst.Spec.Volumes = []compute.VolumeAttachment{data}

	report := func(inst compute.Instance, gen int64, state compute.AttachState) compute.InstanceStatus {
		t.Helper()
		st, _, err := compute.ApplyReport(inst, compute.InstanceReport{
			Node: controllerNode, Generation: gen, State: compute.ReportReady, Power: compute.PowerRunning,
			Volumes: []compute.ObservedVolume{{
				Volume: data.Volume, DeviceName: "vdb", State: state, StateTime: t0, AttachTime: forged,
			}},
		}, t0)
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	for _, state := range []compute.AttachState{compute.AttachAttaching, compute.AttachAttached, compute.AttachBusy} {
		inst.Status = report(inst, 2, state)
		if got := inst.Status.Report.Volumes[0].AttachTime; !got.Equal(attached) {
			t.Fatalf("state %d attach time = %v, want %v", state, got, attached)
		}
	}

	inst.Spec.Volumes, inst.Spec.Generation = nil, 3
	inst.Status = report(inst, 3, compute.AttachDetaching)
	if got := inst.Status.Report.Volumes[0].AttachTime; !got.Equal(attached) {
		t.Fatalf("detaching attach time = %v, want %v kept from the stored report", got, attached)
	}

	fresh := controllerDriven(compute.PowerRunning)
	st := report(fresh, 2, compute.AttachDetaching)
	if got := st.Report.Volumes[0].AttachTime; !got.IsZero() {
		t.Fatalf("a volume in neither spec nor report took attach time %v from the caller", got)
	}
}
