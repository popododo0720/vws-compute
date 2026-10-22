package kubevirt

import (
	"context"
	"net"
	"slices"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// TestEC2DeviceAttachSurvivesRestart replays the vws-test nightly
// (vws-compute#110): run an instance, attach a data volume the way
// console-api does for "ec2 attach-volume --device /dev/sdf", then restart
// compute (a new agent and reconciler on the same store, Restore, Sweep).
// The instance stays READY and running with the same ID, and the volume
// stays in its spec and hot-plugged on the VirtualMachine as disk "sdf".
// On main the agent refused "/dev/sdf" as a disk name permanently, so the
// instance went to ERROR, which console-api shows as terminated or
// InternalError.
func TestEC2DeviceAttachSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	c, ns, _ := newClient(t)
	claim(t, c, ns, "pvc-root", corev1.ReadWriteOnce)
	claim(t, c, ns, "pvc-data", corev1.ReadWriteOnce)
	const vols = "projects/demo/zones/z1/volumes/"
	storage := mapStorage{vols + "root": "pvc-root", vols + "data": "pvc-data"}
	repo := memstore.New()

	// start runs one compute process on repo and returns its client.
	start := func() (computev1.InstanceServiceClient, *reconciler.Reconciler) {
		rec := reconciler.New(repo, New(c, Config{Namespace: ns, ImageNamespace: ns}),
			reconciler.WithReports(repo), reconciler.WithClaims(grpcapi.Claims(storage)))
		srv := grpc.NewServer()
		grpcapi.Register(srv, controller.New(repo), repo, grpcapi.WithVolumes(storage))
		lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = srv.Serve(lis) }()
		t.Cleanup(srv.Stop)
		cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cc.Close() })
		return computev1.NewInstanceServiceClient(cc), rec
	}
	ins, rec := start()

	boot := &computev1.VolumeAttachment{Volume: vols + "root", DeviceName: "/dev/sda1", Boot: true}
	if _, err := ins.CreateInstance(ctx, &computev1.CreateInstanceRequest{
		Parent: "projects/demo/zones/z1", InstanceId: "i-0123456789abcdef0",
		RequestId: "71717171-7171-4171-8171-717171717171",
		Instance: &computev1.Instance{Spec: &computev1.InstanceSpec{
			MachineType: "vws.small", Power: computev1.InstancePower_INSTANCE_POWER_RUNNING,
			Volumes: []*computev1.VolumeAttachment{boot},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	const name = "projects/demo/zones/z1/instances/i-0123456789abcdef0"
	if _, err := ins.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
		RequestId: "72727272-7272-4272-8272-727272727272",
		Instance: &computev1.Instance{Name: name, Spec: &computev1.InstanceSpec{
			Power:   computev1.InstancePower_INSTANCE_POWER_RUNNING,
			Volumes: []*computev1.VolumeAttachment{boot, {Volume: vols + "data", DeviceName: "/dev/sdf"}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}

	// Restart: a fresh agent and reconciler over the same store and cluster.
	ins, rec = start()
	if err := rec.Restore(ctx); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}

	got, err := ins.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if s := got.GetStatus().GetObserved().GetState(); s != commonv1.ReconciliationState_RECONCILIATION_STATE_READY ||
		got.GetStatus().GetPower() != computev1.InstancePower_INSTANCE_POWER_RUNNING {
		t.Fatalf("after restart: observed %v power %v (%v), want READY and RUNNING",
			s, got.GetStatus().GetPower(), got.GetStatus().GetObserved())
	}
	if n := len(got.GetSpec().GetVolumes()); n != 2 {
		t.Fatalf("after restart: %d volume attachments, want 2: %v", n, got.GetSpec().GetVolumes())
	}
	if v, _ := diskNames(t, c, ns, "i-0123456789abcdef0"); !slices.Contains(v, "sdf") {
		t.Fatalf("after restart: VirtualMachine volumes = %v, want sdf hot-plugged", v)
	}
}
