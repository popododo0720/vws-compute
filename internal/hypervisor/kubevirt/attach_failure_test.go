package kubevirt

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// TestHotplugFailureLeavesInstanceRunning: a claim KubeVirt cannot hot-plug
// fails the attach operation, while the instance stays running (EC2 leaves
// the instance alone when an attachment fails).
func TestHotplugFailureLeavesInstanceRunning(t *testing.T) {
	ctx := context.Background()
	c, ns, _ := newClient(t)
	fs := corev1.PersistentVolumeFilesystem
	claimIn(t, c, ns, "pvc-root", corev1.ReadWriteOnce, &fs)
	claimIn(t, c, ns, "pvc-data", corev1.ReadWriteOnce, &fs)

	repo := memstore.New()
	rec := reconciler.New(repo, New(c, Config{Namespace: ns}), reconciler.WithReports(repo))
	ctl := controller.New(repo)
	id, err := compute.ParseInstanceID("i-0123456789abcdef0")
	if err != nil {
		t.Fatal(err)
	}
	root, err := compute.ParseVolumeRef("projects/p/zones/z/volumes/root")
	if err != nil {
		t.Fatal(err)
	}
	boot := compute.VolumeAttachment{Volume: root, DeviceName: "vda", Boot: true, Claim: "pvc-root"}
	createIntent, err := compute.NewCreateInstance(compute.CreateInstanceParams{
		ID: id.String(), Project: "p", Zone: "z", MachineType: "vws.small", Power: compute.PowerRunning,
		Volumes: []compute.VolumeAttachment{boot},
	})
	if err != nil {
		t.Fatal(err)
	}
	create := submit(t, ctl, "create", createIntent)
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if got := op(t, repo, create).State(); got != compute.OperationSucceeded {
		t.Fatalf("create operation = %s, want %s", got, compute.OperationSucceeded)
	}

	dataRef, err := compute.ParseVolumeRef("projects/p/zones/z/volumes/data")
	if err != nil {
		t.Fatal(err)
	}
	data := compute.VolumeAttachment{Volume: dataRef, DeviceName: "vdb", Claim: "pvc-data"}
	attachIntent, err := compute.NewUpdateInstanceSpec(
		id.String(), compute.PowerRunning, []compute.VolumeAttachment{boot, data}, compute.MetadataPatch{})
	if err != nil {
		t.Fatal(err)
	}
	attach := submit(t, ctl, "attach", attachIntent)
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}

	got := op(t, repo, attach)
	if got.State() != compute.OperationFailed {
		t.Fatalf("attach operation = %s, want %s", got.State(), compute.OperationFailed)
	}
	if f := got.Failure(); f == nil || !strings.Contains(f.Message, "volumeMode Filesystem") {
		t.Fatalf("attach failure = %+v, want the volumeMode reason", f)
	}
	inst, err := repo.Instance(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if inst.Lifecycle() == compute.InstanceError {
		t.Fatalf("instance is ERROR after a failed hot-plug: %+v", inst.Status.Observed.Failure)
	}
	if inst.Lifecycle() != compute.InstanceRunning {
		t.Fatalf("instance lifecycle = %d, want running", inst.Lifecycle())
	}
	if vols, _ := diskNames(t, c, ns, id.String()); strings.Contains(strings.Join(vols, ","), "vdb") {
		t.Fatalf("a refused hot-plug reached the VirtualMachine: %v", vols)
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
