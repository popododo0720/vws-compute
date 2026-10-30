package storetest

import (
	"context"
	"crypto/rand"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

const volPrefix = "projects/p/zones/z/volumes/"

// controllerSpec stores machine type n1.medium, which is not in the
// catalog: an instance stored before unknown types were rejected (#45) must
// still round-trip.
func controllerSpec(id string, vol string, gen int64, power compute.Power) compute.InstanceSpec {
	return compute.InstanceSpec{
		ID: ok(compute.ParseInstanceID(id)), Generation: gen, CreatedAt: t0, ChangedAt: t0,
		Type: ok(compute.ParseMachineType("n1.medium")), Power: power,
		Project: "p", Zone: "z", UserData: "#cloud-config",
		Volumes: []compute.VolumeAttachment{{Volume: ok(compute.ParseVolumeRef(volPrefix + vol)), DeviceName: "vda", Boot: true}},
		NICs:    []compute.NetworkInterface{{DeviceName: "eth0", Subnet: "projects/p/regions/r/vpcs/v/subnets/s", SecurityGroups: []string{"projects/p/regions/r/vpcs/v/securityGroups/g"}}},
	}
}

func createIntent(id, vol string) compute.Intent {
	return ok(compute.NewCreateInstance(compute.CreateInstanceParams{
		ID: id, Project: "p", Zone: "z", MachineType: "vws.small", Power: compute.PowerRunning, UserData: "#cloud-config",
		Volumes: []compute.VolumeAttachment{{Volume: ok(compute.ParseVolumeRef(volPrefix + vol)), DeviceName: "vda", Boot: true}},
		NICs:    []compute.NetworkInterface{{DeviceName: "eth0", Subnet: "projects/p/regions/r/vpcs/v/subnets/s"}},
	}))
}

func (f fixture) createControllerDriven(req, id, vol string, at time.Time) (compute.Operation, error) {
	f.t.Helper()
	spec := controllerSpec(id, vol, 1, compute.PowerRunning)
	op, err := compute.NewOperation(compute.Env{Now: at, Rand: rand.Reader}, ok(compute.ParseRequestID(req)),
		createIntent(id, vol), compute.Change{Spec: spec, Write: true})
	if err != nil {
		f.t.Fatal(err)
	}
	return f.s.CommitDesired(context.Background(), store.DesiredChange{Op: op, Spec: spec, Write: true})
}

func (f fixture) mustCreateControllerDriven(req, id, vol string, at time.Time) compute.Operation {
	f.t.Helper()
	op, err := f.createControllerDriven(req, id, vol, at)
	if err != nil {
		f.t.Fatalf("create %s: %v", id, err)
	}
	return op
}

func (f fixture) instance(id string) compute.Instance {
	f.t.Helper()
	return ok(f.s.Instance(context.Background(), ok(compute.ParseInstanceID(id))))
}

func (f fixture) report(id string, rep compute.InstanceReport, now time.Time) (compute.Instance, error) {
	return f.s.ReportInstanceStatus(context.Background(), ok(compute.ParseInstanceID(id)), rep, now)
}

func controllerRoundTrip(f fixture) {
	op := f.mustCreateControllerDriven("req-1", "web-1", "root", t0)
	f.wantState(op, compute.OperationPending)
	got := f.instance("web-1")
	want := controllerSpec("web-1", "root", 1, compute.PowerRunning)
	if got.Spec.Project != "p" || got.Spec.Zone != "z" || got.Spec.UserData != "#cloud-config" || !got.Spec.ControllerDriven() ||
		got.Spec.Type.Name() != "n1.medium" || len(got.Spec.Volumes) != 1 || got.Spec.Volumes[0] != want.Volumes[0] ||
		len(got.Spec.NICs) != 1 || got.Spec.NICs[0].SecurityGroups[0] != want.NICs[0].SecurityGroups[0] ||
		!got.Spec.Image.IsZero() {
		f.t.Fatalf("instance = %+v", got)
	}
	// A second live instance cannot take the volume, and nothing is written.
	_, err := f.createControllerDriven("req-2", "web-2", "root", t0)
	wantCode(f.t, err, compute.CodeFailedPrecondition)
	_, err = f.s.Instance(context.Background(), ok(compute.ParseInstanceID("web-2")))
	wantCode(f.t, err, compute.CodeNotFound)
	if _, err := f.s.OperationByRequest(context.Background(), ok(compute.ParseRequestID("req-2"))); compute.CodeOf(err) != compute.CodeNotFound {
		f.t.Fatalf("failed create left an operation: %v", err)
	}
	// Another volume is fine.
	f.mustCreateControllerDriven("req-3", "web-3", "other", t0)
}

func controllerNotUnconverged(f fixture) {
	f.mustCreateControllerDriven("req-1", "web-1", "root", t0)
	f.mustCommit("req-2", runIntent("vws.small"), instanceSpec(1, 1, compute.PowerRunning), true)
	for name, list := range map[string]func() ([]compute.Ref, error){
		"Unconverged": func() ([]compute.Ref, error) { return f.s.Unconverged(context.Background(), t1) },
		"Lagging":     func() ([]compute.Ref, error) { return f.s.Lagging(context.Background()) },
	} {
		refs, err := list()
		if err != nil || len(refs) != 1 || refs[0].ID != instanceID(1).String() {
			f.t.Fatalf("%s = %v, %v; want only the reconciler's instance", name, refs, err)
		}
	}
}

func reportSettles(f fixture) {
	op := f.mustCreateControllerDriven("req-1", "web-1", "v1", t0)
	f.wantState(op, compute.OperationPending)
	ready := compute.InstanceReport{
		Node: "host-a", Generation: 1, State: compute.ReportReady, Power: compute.PowerRunning,
		Interfaces: []compute.ObservedInterface{{DeviceName: "eth0", PrivateIPv4: "10.0.0.5"}},
		Volumes: []compute.ObservedVolume{{
			Volume: ok(compute.ParseVolumeRef(volPrefix + "v1")), DeviceName: "vda",
			State: compute.AttachAttached, StateTime: t1,
		}},
	}

	if _, err := f.report("web-1", compute.InstanceReport{Generation: 2, State: compute.ReportReady}, t1); compute.CodeOf(err) != compute.CodeAborted {
		f.t.Fatalf("report ahead of spec: %v", err)
	}
	f.wantState(op, compute.OperationPending)

	got, err := f.report("web-1", ready, t1)
	if err != nil {
		f.t.Fatal(err)
	}
	if got.Status.Observed.Generation != 1 || got.Status.Report.Node != "host-a" || len(got.Status.Report.Interfaces) != 1 {
		f.t.Fatalf("after report = %+v", got.Status)
	}
	vols := f.instance("web-1").Status.Report.Volumes
	if len(vols) != 1 || !sameObservedVolume(vols[0], ready.Volumes[0]) {
		f.t.Fatalf("stored volumes = %+v, want %+v", vols, ready.Volumes)
	}
	f.wantState(op, compute.OperationSucceeded)

	// A repeat is harmless, and a later report may come from another host.
	moved := ready
	moved.Node = "host-b"
	if got, err = f.report("web-1", moved, t1.Add(time.Second)); err != nil || got.Status.Report.Node != "host-b" {
		f.t.Fatalf("report from another host = %+v, %v", got.Status, err)
	}
	// The reconciler's status commit does not clobber the report.
	if err := f.s.CommitInstanceStatus(context.Background(), ok(compute.ParseInstanceID("web-1")),
		compute.InstanceStatus{Observed: compute.Observed{Generation: 1}, UpdatedAt: t1.Add(time.Minute)}); err != nil {
		f.t.Fatal(err)
	}
	if after := f.instance("web-1"); after.Status.Report.State != compute.ReportReady {
		f.t.Fatalf("status commit clobbered the report: %+v", after.Status)
	}
	_, err = f.report("nope", ready, t1)
	wantCode(f.t, err, compute.CodeNotFound)
}

func deleteWaitsForTheController(f fixture) {
	f.mustCreateControllerDriven("req-1", "web-1", "v1", t0)
	term := controllerSpec("web-1", "v1", 2, compute.PowerTerminated)
	term.ChangedAt = t1
	op := f.mustCommit("req-del-1", ok(compute.NewTerminateInstance("web-1")), term, true)
	f.wantState(op, compute.OperationPending)
	if f.instance("web-1").Removed() {
		f.t.Fatal("a delete is removed before the controller reports it")
	}
	// The volume is free as soon as the delete is accepted.
	f.mustCreateControllerDriven("req-2", "web-2", "v1", t0)

	if _, err := f.report("web-1", compute.InstanceReport{Generation: 2, State: compute.ReportReady}, t1); err != nil {
		f.t.Fatal(err)
	}
	f.wantState(op, compute.OperationPending)
	if _, err := f.report("web-1", compute.InstanceReport{Generation: 2, State: compute.ReportDeleting}, t1); err != nil {
		f.t.Fatal(err)
	}
	f.wantState(op, compute.OperationSucceeded)
	if !f.instance("web-1").Removed() {
		f.t.Fatal("DELETING did not remove the instance")
	}
}

// withData returns spec at gen with a data volume vol on vdb, backed by
// claim, after the boot entry.
func withData(spec compute.InstanceSpec, gen int64, vol, claim string) compute.InstanceSpec {
	spec.Generation = gen
	spec.Volumes = append(append([]compute.VolumeAttachment(nil), spec.Volumes[:1]...),
		compute.VolumeAttachment{Volume: ok(compute.ParseVolumeRef(volPrefix + vol)), DeviceName: "vdb", Claim: claim})
	return spec
}

func attachIntent(id string, spec compute.InstanceSpec) compute.Intent {
	return ok(compute.NewUpdateInstanceSpec(id, compute.PowerRunning, spec.Volumes, compute.MetadataPatch{}))
}

// attachClaimsVolumes is AttachVolume and DetachVolume through an update: an
// update claims the volumes it adds, one held by another live instance is
// refused, and a detached volume can be attached elsewhere.
func attachClaimsVolumes(f fixture) {
	f.mustCreateControllerDriven("req-1", "web-1", "root-1", t0)
	f.mustCreateControllerDriven("req-2", "web-2", "root-2", t0)

	held := withData(controllerSpec("web-1", "root-1", 1, compute.PowerRunning), 2, "root-2", "pvc-root-2")
	_, err := f.commit("req-3", attachIntent("web-1", held), held, true)
	wantCode(f.t, err, compute.CodeFailedPrecondition)

	data := withData(controllerSpec("web-1", "root-1", 1, compute.PowerRunning), 2, "data", "pvc-data")
	f.mustCommit("req-4", attachIntent("web-1", data), data, true)
	got := f.instance("web-1").Spec
	if len(got.Volumes) != 2 || got.Volumes[1].Claim != "pvc-data" || got.Volumes[0].Volume.String() != volPrefix+"root-1" {
		f.t.Fatalf("after attach = %+v", got.Volumes)
	}

	taken := withData(controllerSpec("web-2", "root-2", 1, compute.PowerRunning), 2, "data", "pvc-data")
	_, err = f.commit("req-5", attachIntent("web-2", taken), taken, true)
	wantCode(f.t, err, compute.CodeFailedPrecondition)

	detached := controllerSpec("web-1", "root-1", 3, compute.PowerRunning)
	f.mustCommit("req-6", attachIntent("web-1", detached), detached, true)
	f.mustCommit("req-7", attachIntent("web-2", taken), taken, true)
	if got := f.instance("web-2").Spec; len(got.Volumes) != 2 || got.Volumes[1].Claim != "pvc-data" {
		f.t.Fatalf("web-2 after takeover = %+v", got.Volumes)
	}
}

// sameObservedVolume compares by instant: a store may return the time in
// another location.
func sameObservedVolume(a, b compute.ObservedVolume) bool {
	return a.Volume == b.Volume && a.DeviceName == b.DeviceName && a.State == b.State && a.StateTime.Equal(b.StateTime)
}
