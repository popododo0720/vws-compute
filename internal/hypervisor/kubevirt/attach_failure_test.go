package kubevirt

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// refusedHotplug is a running instance whose Filesystem data volume KubeVirt
// cannot hot-plug. The attach operation has failed and the instance is still
// running.
type refusedHotplug struct {
	ctx  context.Context
	c    client.Client
	ns   string
	repo *memstore.Store
	rec  *reconciler.Reconciler
	ctl  *controller.Controller
	id   compute.InstanceID
	boot compute.VolumeAttachment
}

func newRefusedHotplug(t *testing.T) *refusedHotplug {
	t.Helper()
	f := &refusedHotplug{ctx: context.Background()}
	f.c, f.ns, _ = newClient(t)
	fs := corev1.PersistentVolumeFilesystem
	claimIn(t, f.c, f.ns, "pvc-root", corev1.ReadWriteOnce, &fs)
	claimIn(t, f.c, f.ns, "pvc-data", corev1.ReadWriteOnce, &fs)

	f.repo = memstore.New()
	f.rec = reconciler.New(f.repo, New(f.c, Config{Namespace: f.ns}), reconciler.WithReports(f.repo))
	f.ctl = controller.New(f.repo)
	id, err := compute.ParseInstanceID("i-0123456789abcdef0")
	if err != nil {
		t.Fatal(err)
	}
	f.id = id
	root, err := compute.ParseVolumeRef("projects/p/zones/z/volumes/root")
	if err != nil {
		t.Fatal(err)
	}
	f.boot = compute.VolumeAttachment{Volume: root, DeviceName: "vda", Boot: true, Claim: "pvc-root"}
	createIntent, err := compute.NewCreateInstance(compute.CreateInstanceParams{
		ID: id.String(), Project: "p", Zone: "z", MachineType: "vws.small", Power: compute.PowerRunning,
		Volumes: []compute.VolumeAttachment{f.boot},
	})
	if err != nil {
		t.Fatal(err)
	}
	create := submit(t, f.ctl, "create", createIntent)
	if err := f.rec.Sweep(f.ctx); err != nil {
		t.Fatal(err)
	}
	if got := op(t, f.repo, create).State(); got != compute.OperationSucceeded {
		t.Fatalf("create operation = %s, want %s", got, compute.OperationSucceeded)
	}

	dataRef, err := compute.ParseVolumeRef("projects/p/zones/z/volumes/data")
	if err != nil {
		t.Fatal(err)
	}
	data := compute.VolumeAttachment{Volume: dataRef, DeviceName: "vdb", Claim: "pvc-data"}
	attachIntent, err := compute.NewUpdateInstanceSpec(
		id.String(), compute.PowerRunning, []compute.VolumeAttachment{f.boot, data}, compute.MetadataPatch{})
	if err != nil {
		t.Fatal(err)
	}
	attach := submit(t, f.ctl, "attach", attachIntent)
	if err := f.rec.Sweep(f.ctx); err != nil {
		t.Fatal(err)
	}

	got := op(t, f.repo, attach)
	if got.State() != compute.OperationFailed {
		t.Fatalf("attach operation = %s, want %s", got.State(), compute.OperationFailed)
	}
	if failure := got.Failure(); failure == nil || !strings.Contains(failure.Message, "volumeMode Filesystem") {
		t.Fatalf("attach failure = %+v, want the volumeMode reason", failure)
	}
	inst, err := f.repo.Instance(f.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if inst.Lifecycle() == compute.InstanceError {
		t.Fatalf("instance is ERROR after a failed hot-plug: %+v", inst.Status.Observed.Failure)
	}
	if inst.Lifecycle() != compute.InstanceRunning {
		t.Fatalf("instance lifecycle = %d, want running", inst.Lifecycle())
	}
	if vols, _ := diskNames(t, f.c, f.ns, id.String()); strings.Contains(strings.Join(vols, ","), "vdb") {
		t.Fatalf("a refused hot-plug reached the VirtualMachine: %v", vols)
	}
	return f
}

// TestHotplugFailureLeavesInstanceRunning: a claim KubeVirt cannot hot-plug
// fails the attach operation, while the instance stays running (EC2 leaves
// the instance alone when an attachment fails).
func TestHotplugFailureLeavesInstanceRunning(t *testing.T) {
	newRefusedHotplug(t)
}

// TestStopAfterRefusedHotplugHaltsTheMachine: the refused volume must not
// stay in desired state and block a later stop. The VirtualMachine halts and
// the lifecycle matches it.
func TestStopAfterRefusedHotplugHaltsTheMachine(t *testing.T) {
	f := newRefusedHotplug(t)
	stop := submit(t, f.ctl, "stop", ok(compute.NewStopInstance(f.id.String())))
	if err := f.rec.Sweep(f.ctx); err != nil {
		t.Fatal(err)
	}
	if got := op(t, f.repo, stop); got.State() != compute.OperationSucceeded {
		t.Fatalf("stop operation = %s, want %s (%+v)", got.State(), compute.OperationSucceeded, got.Failure())
	}
	if got := runStrategy(t, f.c, f.ns, f.id.String()); got != "Halted" {
		t.Fatalf("runStrategy = %q, want Halted", got)
	}
	inst, err := f.repo.Instance(f.ctx, f.id)
	if err != nil {
		t.Fatal(err)
	}
	if inst.Lifecycle() != compute.InstanceStopped {
		t.Fatalf("instance lifecycle = %d, want stopped", inst.Lifecycle())
	}
	if vols, _ := diskNames(t, f.c, f.ns, f.id.String()); strings.Contains(strings.Join(vols, ","), "vdb") {
		t.Fatalf("the refused volume reached the halted VirtualMachine: %v", vols)
	}
}

// TestRebootAfterRefusedHotplugIsApplied: a reboot after the refused attach
// still reaches the VirtualMachine and records the epoch.
func TestRebootAfterRefusedHotplugIsApplied(t *testing.T) {
	f := newRefusedHotplug(t)
	reboot := submit(t, f.ctl, "reboot", ok(compute.NewRebootInstance(f.id.String())))
	if err := f.rec.Sweep(f.ctx); err != nil {
		t.Fatal(err)
	}
	if got := op(t, f.repo, reboot); got.State() != compute.OperationSucceeded {
		t.Fatalf("reboot operation = %s, want %s (%+v)", got.State(), compute.OperationSucceeded, got.Failure())
	}
	vm, err := get(t, f.c, "VirtualMachine", f.ns, f.id.String())
	if err != nil {
		t.Fatal(err)
	}
	if got := vm.GetAnnotations()[AnnotationRebootEpoch]; got != "1" {
		t.Fatalf("applied reboot epoch = %q, want 1", got)
	}
}

func submit(t *testing.T, ctl *controller.Controller, req string, in compute.Intent) compute.Operation {
	t.Helper()
	id, err := compute.ParseRequestID(req)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ctl.Submit(context.Background(), id, in)
	if err != nil {
		t.Fatal(err)
	}
	return r.Operation
}

func op(t *testing.T, repo *memstore.Store, o compute.Operation) compute.Operation {
	t.Helper()
	got, err := repo.Operation(context.Background(), o.ID)
	if err != nil {
		t.Fatal(err)
	}
	return got
}
