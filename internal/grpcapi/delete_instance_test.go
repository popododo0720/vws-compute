package grpcapi_test

import (
	"context"
	"net"
	"testing"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

func TestSecondDeleteIsDoneAndAMissingInstanceIsNotFound(t *testing.T) {
	ctx := context.Background()
	const (
		id   = "i-0123456789abcdef0"
		name = zoneParent + "/instances/" + id
		req1 = "11111111-1111-4111-8111-111111111111"
		req2 = "22222222-2222-4222-8222-222222222222"
		req3 = "33333333-3333-4333-8333-333333333333"
	)
	sc := storageClient{vols: map[string]*storagev1.Volume{
		volBase + "vol-root": storageVolume(ready, "", "pvc-root"),
	}}
	lookup := grpcapi.NewStorageVolumes(sc)
	repo := memstore.New()
	agent := fakeagent.New()
	rec := reconciler.New(repo, agent, reconciler.WithReports(repo), reconciler.WithClaims(grpcapi.Claims(lookup)))
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	grpcapi.Register(srv, controller.New(repo), repo, grpcapi.WithVolumes(lookup))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ins := computev1.NewInstanceServiceClient(conn)
	ops := longrunningpb.NewOperationsClient(conn)

	if _, err := ins.CreateInstance(ctx, createReq(id, "44444444-4444-4444-8444-444444444444", "vol-root")); err != nil {
		t.Fatal(err)
	}
	var got *computev1.Instance
	for range 5 {
		if err := rec.Sweep(ctx); err != nil {
			t.Fatal(err)
		}
		got, err = ins.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		if got.GetStatus().GetObserved().GetState() == ready {
			break
		}
	}
	if got.GetStatus().GetObserved().GetState() != ready {
		t.Fatalf("state = %s, want READY", got.GetStatus().GetObserved().GetState())
	}

	del, err := ins.DeleteInstance(ctx, &computev1.DeleteInstanceRequest{Name: name, RequestId: req1})
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if err := rec.Sweep(ctx); err != nil {
			t.Fatal(err)
		}
		cur, err := ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: del.GetName()})
		if err != nil {
			t.Fatal(err)
		}
		del = cur
		if del.GetDone() {
			break
		}
	}
	if !del.GetDone() || del.GetError() != nil {
		t.Fatalf("first delete done=%v err=%v", del.GetDone(), del.GetError())
	}

	op2, err := ins.DeleteInstance(ctx, &computev1.DeleteInstanceRequest{Name: name, RequestId: req2})
	if err != nil {
		t.Fatalf("second delete: %v", err)
	}
	if !op2.GetDone() || op2.GetError() != nil {
		t.Fatalf("second delete done=%v err=%v", op2.GetDone(), op2.GetError())
	}
	stored, err := repo.Instance(ctx, mustInstanceID(t, id))
	if err != nil {
		t.Fatal(err)
	}
	if stored.Spec.Generation != 2 || stored.Spec.Power != compute.PowerTerminated {
		t.Fatalf("stored generation %d power %d, want 2 terminated", stored.Spec.Generation, stored.Spec.Power)
	}

	_, err = ins.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("get after delete: %v", err)
	}
	_, err = ins.DeleteInstance(ctx, &computev1.DeleteInstanceRequest{
		Name: "projects/p/zones/z/instances/i-0123456789abcdef9", RequestId: req3,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing instance: %v", err)
	}

	replay, err := ins.DeleteInstance(ctx, &computev1.DeleteInstanceRequest{Name: name, RequestId: req1})
	if err != nil {
		t.Fatal(err)
	}
	if replay.GetName() != del.GetName() {
		t.Fatalf("replay name %s, want %s", replay.GetName(), del.GetName())
	}
	stored, err = repo.Instance(ctx, mustInstanceID(t, id))
	if err != nil {
		t.Fatal(err)
	}
	if stored.Spec.Generation != 2 {
		t.Fatalf("generation after replay = %d, want 2", stored.Spec.Generation)
	}
}

func mustInstanceID(t *testing.T, id string) compute.InstanceID {
	t.Helper()
	got, err := compute.ParseInstanceID(id)
	if err != nil {
		t.Fatal(err)
	}
	return got
}
