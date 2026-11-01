package kubevirt

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// TestCreateWithDataVolumesHotplugsThem is compute#142: a machine created
// with data volumes lists them as hot-pluggable volumes, exactly as an
// attach to an existing machine would, so they plug without a later update.
// The create is the only write, and a second converge writes nothing.
func TestCreateWithDataVolumesHotplugsThem(t *testing.T) {
	c, ns, w := newClient(t)
	standardImage(t, c, ns)
	ctx := context.Background()
	a := New(c, Config{Namespace: ns})
	claim(t, c, ns, "data-1", corev1.ReadWriteOnce)
	claim(t, c, ns, "data-2", corev1.ReadWriteOnce)
	s := spec("i-0123456789abcdef0", hypervisor.PowerOn)
	s.Volumes = []hypervisor.Volume{{Name: "data2", Claim: "data-2"}, {Name: "data1", Claim: "data-1"}}
	before := w.n.Load()
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	if got := w.n.Load() - before; got != 1 {
		t.Fatalf("create wrote %d times, want 1", got)
	}
	vols, disks := diskNames(t, c, ns, s.Name)
	if want := []string{"rootdisk", "data1", "data2"}; !equal(vols, want) {
		t.Fatalf("volumes %v, want %v", vols, want)
	}
	if want := []string{"rootdisk", "data1", "data2"}; !equal(disks, want) {
		t.Fatalf("disks %v, want %v", disks, want)
	}
	wantPhases(ctx, t, a, s.Name, map[string]hypervisor.VolumePhase{
		"data1": hypervisor.VolumePlugging, "data2": hypervisor.VolumePlugging,
	})

	before = w.n.Load()
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	if got := w.n.Load() - before; got != 0 {
		t.Fatalf("second converge wrote %d times, want 0", got)
	}

	setVolumeStatus(ctx, t, c, ns, s.Name, hotplugStatus("data1", "Ready"), hotplugStatus("data2", "MountedToPod"))
	wantPhases(ctx, t, a, s.Name, map[string]hypervisor.VolumePhase{
		"data1": hypervisor.VolumePlugged, "data2": hypervisor.VolumePlugging,
	})

	// The same machine without the volumes drops them with the usual detach.
	s.Volumes = nil
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	if vols, _ := diskNames(t, c, ns, s.Name); !equal(vols, []string{"rootdisk"}) {
		t.Fatalf("volumes after detach %v, want only the root disk", vols)
	}
}

// TestCreateBootsWithoutARefusedDataVolume: a data volume the machine cannot
// hot-plug (here a Filesystem claim) does not keep the machine from booting.
// It is created without the volume and the converge fails with an attachment
// error naming it, the same outcome as attaching it after the boot.
func TestCreateBootsWithoutARefusedDataVolume(t *testing.T) {
	c, ns, _ := newClient(t)
	standardImage(t, c, ns)
	ctx := context.Background()
	a := New(c, Config{Namespace: ns})
	fs := corev1.PersistentVolumeFilesystem
	claimIn(t, c, ns, "data-fs", corev1.ReadWriteOnce, &fs)
	s := spec("i-0123456789abcdef1", hypervisor.PowerOn)
	s.Volumes = []hypervisor.Volume{{Name: "data1", Claim: "data-fs"}}
	err := a.ConvergeMachine(ctx, s)
	if hypervisor.CauseOf(err) != hypervisor.CauseAttachment || !hypervisor.IsPermanent(err) {
		t.Fatalf("converge = %v, want a permanent attachment error", err)
	}
	if v, ok := hypervisor.AttachmentVolume(err); !ok || v.Name != "data1" {
		t.Fatalf("attachment error names %v (%t), want data1", v, ok)
	}
	if vols, _ := diskNames(t, c, ns, s.Name); !equal(vols, []string{"rootdisk"}) {
		t.Fatalf("volumes %v, want only the root disk", vols)
	}
	// Without the refused volume the machine converges.
	s.Volumes = nil
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
}

// TestCreateBeforeTheClaimExistsAttachesOnRetry: a data volume whose claim
// the storage service has not made yet does not hold the boot back. The
// converge is transient, so the reconciler retries, and the retry hot-plugs
// the volume once the claim exists.
func TestCreateBeforeTheClaimExistsAttachesOnRetry(t *testing.T) {
	c, ns, _ := newClient(t)
	standardImage(t, c, ns)
	ctx := context.Background()
	a := New(c, Config{Namespace: ns})
	s := spec("i-0123456789abcdef2", hypervisor.PowerOn)
	s.Volumes = []hypervisor.Volume{{Name: "data1", Claim: "data-late"}}
	err := a.ConvergeMachine(ctx, s)
	if err == nil || hypervisor.IsPermanent(err) {
		t.Fatalf("converge = %v, want a transient error", err)
	}
	if vols, _ := diskNames(t, c, ns, s.Name); !equal(vols, []string{"rootdisk"}) {
		t.Fatalf("volumes %v, want only the root disk", vols)
	}
	claim(t, c, ns, "data-late", corev1.ReadWriteOnce)
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	if vols, _ := diskNames(t, c, ns, s.Name); !equal(vols, []string{"rootdisk", "data1"}) {
		t.Fatalf("volumes %v, want the root disk and data1", vols)
	}
}

// TestCreatedDataVolumeIsReportedAttachedAfterHotplug is compute#142 through
// the controller and the reconciler: an instance created with a data volume
// reports it attaching while the hot-plug runs and attached once the VMI's
// volumeStatus has it Ready, with no update in between. envtest runs no
// KubeVirt, so the test writes volumeStatus as virt-handler would.
func TestCreatedDataVolumeIsReportedAttachedAfterHotplug(t *testing.T) {
	ctx := context.Background()
	c, ns, _ := newClient(t)
	claim(t, c, ns, "pvc-root", corev1.ReadWriteOnce)
	claim(t, c, ns, "pvc-data", corev1.ReadWriteOnce)
	repo := memstore.New()
	rec := reconciler.New(repo, New(c, Config{Namespace: ns}), reconciler.WithReports(repo))
	ctl := controller.New(repo)
	id := ok(compute.ParseInstanceID("i-0123456789abcdef4"))
	root := ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/root"))
	boot := compute.VolumeAttachment{Volume: root, DeviceName: "vda", Boot: true, Claim: "pvc-root"}
	dataRef := ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/data"))
	data := compute.VolumeAttachment{Volume: dataRef, DeviceName: "/dev/sdf", Claim: "pvc-data"}
	create := submit(t, ctl, "create", ok(compute.NewCreateInstance(compute.CreateInstanceParams{
		ID: id.String(), Project: "p", Zone: "z", MachineType: "vws.small", Power: compute.PowerRunning,
		Volumes: []compute.VolumeAttachment{boot, data},
	})))
	report := func() compute.Report {
		t.Helper()
		if err := rec.Sweep(ctx); err != nil {
			t.Fatal(err)
		}
		inst, err := repo.Instance(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return inst.Status.Report
	}
	want := func(rep compute.Report, s ...compute.AttachState) {
		t.Helper()
		var got []compute.AttachState
		for _, v := range rep.Volumes {
			got = append(got, v.State)
		}
		if rep.State != compute.ReportReady || !equalStates(got, s) {
			t.Fatalf("report %v with attach states %v, want READY with %v", rep.State, got, s)
		}
	}

	want(report(), compute.AttachAttached, compute.AttachAttaching)
	if got := op(t, repo, create).State(); got != compute.OperationSucceeded {
		t.Fatalf("create operation = %s, want succeeded", got)
	}
	setVolumeStatus(ctx, t, c, ns, id.String(), hotplugStatus("sdf", "MountedToPod"))
	want(report(), compute.AttachAttached, compute.AttachAttaching)
	setVolumeStatus(ctx, t, c, ns, id.String(), hotplugStatus("sdf", "Ready"))
	want(report(), compute.AttachAttached, compute.AttachAttached)
}

// TestCreateKeepsTheAttachableVolumesBesideARefusedOne: one refused data
// volume does not drop the others from the create. The valid one is
// hot-plugged with the machine, and the converge still fails with an
// attachment error naming only the refused one.
func TestCreateKeepsTheAttachableVolumesBesideARefusedOne(t *testing.T) {
	c, ns, _ := newClient(t)
	standardImage(t, c, ns)
	ctx := context.Background()
	a := New(c, Config{Namespace: ns})
	fs := corev1.PersistentVolumeFilesystem
	claimIn(t, c, ns, "data-fs", corev1.ReadWriteOnce, &fs)
	claim(t, c, ns, "data-ok", corev1.ReadWriteOnce)
	s := spec("i-0123456789abcdef3", hypervisor.PowerOn)
	s.Volumes = []hypervisor.Volume{{Name: "data1", Claim: "data-fs"}, {Name: "data2", Claim: "data-ok"}}
	err := a.ConvergeMachine(ctx, s)
	if hypervisor.CauseOf(err) != hypervisor.CauseAttachment || !hypervisor.IsPermanent(err) {
		t.Fatalf("converge = %v, want a permanent attachment error", err)
	}
	if v, ok := hypervisor.AttachmentVolume(err); !ok || v.Name != "data1" {
		t.Fatalf("attachment error names %v (%t), want data1", v, ok)
	}
	if vols, _ := diskNames(t, c, ns, s.Name); !equal(vols, []string{"rootdisk", "data2"}) {
		t.Fatalf("volumes %v, want the root disk and data2", vols)
	}
	setVolumeStatus(ctx, t, c, ns, s.Name, hotplugStatus("data2", "Ready"))
	wantPhases(ctx, t, a, s.Name, map[string]hypervisor.VolumePhase{"data2": hypervisor.VolumePlugged})
}

// TestCreatedInstanceAttachesTheValidVolumeBesideARefusedOne is the
// multi-volume #142 case through the controller and the reconciler: a
// CreateInstance with /dev/sdf on a Filesystem claim and /dev/sdg on a Block
// claim reports /dev/sdg attached once its hot-plug is Ready, though /dev/sdf
// was refused and the generation settled.
func TestCreatedInstanceAttachesTheValidVolumeBesideARefusedOne(t *testing.T) {
	ctx := context.Background()
	c, ns, _ := newClient(t)
	claim(t, c, ns, "pvc-root", corev1.ReadWriteOnce)
	fs := corev1.PersistentVolumeFilesystem
	claimIn(t, c, ns, "pvc-fs", corev1.ReadWriteOnce, &fs)
	claim(t, c, ns, "pvc-ok", corev1.ReadWriteOnce)
	repo := memstore.New()
	rec := reconciler.New(repo, New(c, Config{Namespace: ns}), reconciler.WithReports(repo))
	ctl := controller.New(repo)
	id := ok(compute.ParseInstanceID("i-0123456789abcdef5"))
	root := ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/root"))
	fsRef := ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/fs"))
	okRef := ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/ok"))
	submit(t, ctl, "create", ok(compute.NewCreateInstance(compute.CreateInstanceParams{
		ID: id.String(), Project: "p", Zone: "z", MachineType: "vws.small", Power: compute.PowerRunning,
		Volumes: []compute.VolumeAttachment{
			{Volume: root, DeviceName: "vda", Boot: true, Claim: "pvc-root"},
			{Volume: fsRef, DeviceName: "/dev/sdf", Claim: "pvc-fs"},
			{Volume: okRef, DeviceName: "/dev/sdg", Claim: "pvc-ok"},
		},
	})))
	stateOf := func(ref compute.VolumeRef) (compute.ReportState, compute.AttachState) {
		t.Helper()
		if err := rec.Sweep(ctx); err != nil {
			t.Fatal(err)
		}
		inst, err := repo.Instance(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range inst.Status.Report.Volumes {
			if v.Volume == ref {
				return inst.Status.Report.State, v.State
			}
		}
		return inst.Status.Report.State, compute.AttachUnspecified
	}
	if st, as := stateOf(okRef); st != compute.ReportReady || as != compute.AttachAttaching {
		t.Fatalf("before hot-plug: instance %v, /dev/sdg %v, want READY and attaching", st, as)
	}
	setVolumeStatus(ctx, t, c, ns, id.String(), hotplugStatus("sdg", "Ready"))
	for range 3 {
		if st, as := stateOf(okRef); st != compute.ReportReady || as != compute.AttachAttached {
			t.Fatalf("after hot-plug: instance %v, /dev/sdg %v, want READY and attached", st, as)
		}
	}
}
