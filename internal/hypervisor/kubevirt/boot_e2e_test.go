package kubevirt

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// claimStorage answers every volume with one claim, as vws-storage reports
// VolumeStatus.claim_name.
type claimStorage struct{ claim string }

func (s claimStorage) Volume(context.Context, string) (grpcapi.VolumeInfo, error) {
	return grpcapi.VolumeInfo{ClaimName: s.claim}, nil
}

// TestCreateInstanceBootsFromVolumeOnKubeVirt drives CreateInstance through
// the gRPC API, the controller, and the reconciler with this agent against
// envtest: the instance reaches READY and its VirtualMachine boots from the
// storage claim. envtest runs no KubeVirt controller, so READY here means
// the VirtualMachine object was accepted, not that a guest booted.
func TestCreateInstanceBootsFromVolumeOnKubeVirt(t *testing.T) {
	ctx := context.Background()
	c, ns, _ := newClient(t)
	claim(t, c, ns, "pvc-root-1", corev1.ReadWriteOnce)

	repo := memstore.New()
	rec := reconciler.New(repo, New(c, Config{Namespace: ns, ImageNamespace: ns}), reconciler.WithReports(repo))
	ctl := controller.New(repo)
	srv := grpc.NewServer()
	grpcapi.Register(srv, ctl, repo, grpcapi.WithVolumes(claimStorage{claim: "pvc-root-1"}))
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
	inst := computev1.NewInstanceServiceClient(cc)

	_, err = inst.CreateInstance(ctx, &computev1.CreateInstanceRequest{
		Parent: "projects/demo/zones/z1", InstanceId: "web-1", RequestId: "71717171-7171-4171-8171-717171717171",
		Instance: &computev1.Instance{Spec: &computev1.InstanceSpec{
			MachineType: "vws.small", Power: computev1.InstancePower_INSTANCE_POWER_RUNNING,
			Volumes: []*computev1.VolumeAttachment{{Volume: "projects/demo/zones/z1/volumes/root-1", DeviceName: "vda", Boot: true}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}

	got, err := repo.Instance(ctx, ok(compute.ParseInstanceID("web-1")))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status.Report.State != compute.ReportReady || got.Status.Observed.Generation != 1 {
		t.Fatalf("status = %+v, want READY at generation 1", got.Status)
	}
	vm, err := get(t, c, "VirtualMachine", ns, "web-1")
	if err != nil {
		t.Fatal(err)
	}
	if claim := rootClaim(vm); claim != "pvc-root-1" {
		t.Fatalf("root disk claim = %q, want pvc-root-1", claim)
	}
}

func ok[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
