package kubevirt

import (
	"context"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// launchFixture is a controller, a memstore, and a reconciler over one
// envtest namespace, with the root claim made.
type launchFixture struct {
	t    *testing.T
	ctx  context.Context
	c    client.Client
	ns   string
	w    *writes
	repo *memstore.Store
	ctl  *controller.Controller
	rec  *reconciler.Reconciler
	id   compute.InstanceID
}

func newLaunchFixture(t *testing.T, id string) *launchFixture {
	t.Helper()
	c, ns, w := newClient(t)
	claim(t, c, ns, "pvc-root", corev1.ReadWriteOnce)
	repo := memstore.New()
	f := &launchFixture{
		t: t, ctx: context.Background(), c: c, ns: ns, w: w, repo: repo,
		ctl: controller.New(repo), id: ok(compute.ParseInstanceID(id)),
	}
	f.restart()
	return f
}

// restart is a new reconciler and agent over the same store and cluster.
func (f *launchFixture) restart() {
	f.rec = reconciler.New(f.repo, New(f.c, Config{Namespace: f.ns}), reconciler.WithReports(f.repo))
}

func (f *launchFixture) create(req string, power compute.Power, data ...compute.VolumeAttachment) compute.Operation {
	f.t.Helper()
	root := compute.VolumeAttachment{
		Volume:     ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/root")),
		DeviceName: "vda", Boot: true, Claim: "pvc-root",
	}
	return submit(f.t, f.ctl, req, ok(compute.NewCreateInstance(compute.CreateInstanceParams{
		ID: f.id.String(), Project: "p", Zone: "z", MachineType: "vws.small", Power: power,
		Volumes: append([]compute.VolumeAttachment{root}, data...),
	})))
}

func (f *launchFixture) sweep() compute.Instance {
	f.t.Helper()
	if err := f.rec.Sweep(f.ctx); err != nil {
		f.t.Fatal(err)
	}
	return ok(f.repo.Instance(f.ctx, f.id))
}

// wantFailedLaunch checks that create failed, the instance is in error with
// every refused volume named and none reported, and the VirtualMachine is
// halted and lists the root disk plus the disks in kept.
func (f *launchFixture) wantFailedLaunch(
	create compute.Operation, inst compute.Instance, kept []string, refused ...compute.VolumeRef,
) {
	f.t.Helper()
	got := op(f.t, f.repo, create)
	if got.State() != compute.OperationFailed || got.Failure().Code != compute.CodeFailedPrecondition {
		f.t.Fatalf("create operation = %s %+v, want failed with FailedPrecondition", got.State(), got.Failure())
	}
	if lc := inst.Lifecycle(); lc != compute.InstanceError || inst.Status.Report.State != compute.ReportError {
		f.t.Fatalf("instance lifecycle %d, report %v, want error", lc, inst.Status.Report.State)
	}
	for _, v := range refused {
		if !inst.Status.Observed.AttachmentFailure.Refused(v) {
			f.t.Fatalf("attachment failure = %+v, want %s refused", inst.Status.Observed.AttachmentFailure, v)
		}
	}
	for _, v := range inst.Status.Report.Volumes {
		if slices.Contains(refused, v.Volume) || v.State == compute.AttachAttaching {
			f.t.Fatalf("volume %s reported %v: %+v", v.Volume, v.State, inst.Status.Report.Volumes)
		}
	}
	if got := runStrategy(f.t, f.c, f.ns, f.id.String()); got != "Halted" {
		f.t.Fatalf("runStrategy %q, want Halted", got)
	}
	if vols, _ := diskNames(f.t, f.c, f.ns, f.id.String()); !equal(vols, append([]string{"rootdisk"}, kept...)) {
		f.t.Fatalf("VirtualMachine volumes %v, want the root disk and %v", vols, kept)
	}
}

// TestRefusedDataVolumeFailsTheLaunch: CreateInstance with /dev/sdf and
// /dev/sdh on Filesystem claims and /dev/sdg on a Block claim fails the
// launch, as EC2 terminates an instance whose launch-time block device
// fails. The create operation fails, the instance is in error rather than
// running, and the VirtualMachine is halted with /dev/sdg only. Sweeps, a
// restart, and a replay of the create change nothing. A start then runs the
// instance with /dev/sdg and drops the refused volumes from desired state.
func TestRefusedDataVolumeFailsTheLaunch(t *testing.T) {
	f := newLaunchFixture(t, "i-0123456789abcdef5")
	fs := corev1.PersistentVolumeFilesystem
	claimIn(t, f.c, f.ns, "pvc-fs1", corev1.ReadWriteOnce, &fs)
	claimIn(t, f.c, f.ns, "pvc-fs2", corev1.ReadWriteOnce, &fs)
	claim(t, f.c, f.ns, "pvc-ok", corev1.ReadWriteOnce)
	fs1 := ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/fs1"))
	fs2 := ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/fs2"))
	okRef := ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/ok"))
	okEntry := compute.VolumeAttachment{Volume: okRef, DeviceName: "/dev/sdg", Claim: "pvc-ok"}
	data := []compute.VolumeAttachment{
		{Volume: fs1, DeviceName: "/dev/sdf", Claim: "pvc-fs1"},
		okEntry,
		{Volume: fs2, DeviceName: "/dev/sdh", Claim: "pvc-fs2"},
	}
	create := f.create("create", compute.PowerRunning, data...)
	failed := f.sweep()
	f.wantFailedLaunch(create, failed, []string{"sdg"}, fs1, fs2)

	before := f.w.n.Load()
	f.sweep()
	f.restart()
	inst := f.sweep()
	if n := f.w.n.Load() - before; n != 0 {
		t.Fatalf("sweeps after the failed launch wrote the API %d times, want 0", n)
	}
	if !inst.Status.UpdatedAt.Equal(failed.Status.UpdatedAt) {
		t.Fatalf("sweeps after the failed launch rewrote its status: %+v", inst.Status)
	}
	if replay := f.create("create", compute.PowerRunning, data...); replay.ID != create.ID {
		t.Fatalf("replayed create = %s, want operation %s", replay.ID, create.ID)
	}

	startIntent := ok(compute.NewSetPower(f.id.String(), compute.PowerRunning, compute.MetadataPatch{}))
	start := submit(t, f.ctl, "start", startIntent)
	inst = f.sweep()
	if got := op(t, f.repo, start); got.State() != compute.OperationSucceeded {
		t.Fatalf("start operation = %s %+v, want succeeded", got.State(), got.Failure())
	}
	if lc := inst.Lifecycle(); lc != compute.InstanceRunning {
		t.Fatalf("lifecycle after start = %d, want running", lc)
	}
	for _, v := range inst.Spec.Volumes {
		if v.Volume == fs1 || v.Volume == fs2 {
			t.Fatalf("refused volume %s still desired after start: %+v", v.Volume, inst.Spec.Volumes)
		}
	}
	if !slices.Contains(inst.Spec.Volumes, okEntry) {
		t.Fatalf("/dev/sdg dropped by start: %+v", inst.Spec.Volumes)
	}
	if got := runStrategy(t, f.c, f.ns, f.id.String()); got != "Always" {
		t.Fatalf("runStrategy after start %q, want Always", got)
	}
}

// TestRefusalAfterAMissingClaimStillFailsTheLaunch: a data volume whose
// claim does not exist yet boots the machine without it and keeps the
// launch pending. When the claim then shows up as one the machine cannot
// attach, the launch fails as if it had been refused at the first converge.
func TestRefusalAfterAMissingClaimStillFailsTheLaunch(t *testing.T) {
	f := newLaunchFixture(t, "i-0123456789abcdef6")
	late := ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/late"))
	create := f.create("create", compute.PowerRunning,
		compute.VolumeAttachment{Volume: late, DeviceName: "/dev/sdf", Claim: "pvc-late"})
	if err := f.rec.Sweep(f.ctx); err == nil {
		t.Fatal("sweep with a missing claim succeeded, want a transient error")
	}
	if inst := ok(f.repo.Instance(f.ctx, f.id)); inst.Lifecycle() != compute.InstancePending {
		t.Fatalf("lifecycle with a missing claim = %d, want pending", inst.Lifecycle())
	}
	fs := corev1.PersistentVolumeFilesystem
	claimIn(t, f.c, f.ns, "pvc-late", corev1.ReadWriteOnce, &fs)
	f.wantFailedLaunch(create, f.sweep(), nil, late)
}

// TestStoppedLaunchWithARefusedVolumeFails: a create that asks for a stopped
// instance fails its launch the same way, and the machine stays halted.
func TestStoppedLaunchWithARefusedVolumeFails(t *testing.T) {
	f := newLaunchFixture(t, "i-0123456789abcdef7")
	fs := corev1.PersistentVolumeFilesystem
	claimIn(t, f.c, f.ns, "pvc-fs", corev1.ReadWriteOnce, &fs)
	ref := ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/fs"))
	create := f.create("create", compute.PowerStopped,
		compute.VolumeAttachment{Volume: ref, DeviceName: "/dev/sdf", Claim: "pvc-fs"})
	f.wantFailedLaunch(create, f.sweep(), nil, ref)
}
