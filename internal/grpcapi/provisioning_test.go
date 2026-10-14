package grpcapi_test

import (
	"context"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

const (
	reconciling = commonv1.ReconciliationState_RECONCILIATION_STATE_RECONCILING
	ready       = commonv1.ReconciliationState_RECONCILIATION_STATE_READY
	failed      = commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR
)

// runInstance is the CreateInstance vws-console-api sends for RunInstances
// right after it asked storage to create the root volume from the image
// (internal/backend/grpcclient/services.go).
func runInstance(id, req, vol string) *computev1.CreateInstanceRequest {
	return &computev1.CreateInstanceRequest{
		Parent: zoneParent, InstanceId: id, RequestId: req,
		Instance: &computev1.Instance{Spec: &computev1.InstanceSpec{
			MachineType:       "vws.small",
			Power:             computev1.InstancePower_INSTANCE_POWER_RUNNING,
			NetworkInterfaces: []*computev1.NetworkInterface{{DeviceName: "nic0", Subnet: consoleSubnet}},
			Volumes:           []*computev1.VolumeAttachment{{Volume: volBase + vol, DeviceName: "boot", Boot: true}},
		}},
	}
}

// TestRunInstancesAcceptsAProvisioningRootVolume is the vws-test failure of
// 2026-10-14 05:39 UTC: RunInstances failed with "volume ... has no claim
// yet" because the root volume storage was still making from the image was
// refused as VOLUME_NOT_PROVISIONED. It is accepted now and waits.
func TestRunInstancesAcceptsAProvisioningRootVolume(t *testing.T) {
	ctx := context.Background()
	sc := storageClient{vols: map[string]*storagev1.Volume{
		volBase + "vol-root": storageVolume(reconciling, "", ""),
	}}
	h := serveRepo(t, memstore.New(), grpcapi.WithVolumes(grpcapi.NewStorageVolumes(sc)))
	if _, err := h.inst.CreateInstance(ctx, runInstance("i-0123456789abcdef0", "81818181-8181-4181-8181-818181818181", "vol-root")); err != nil {
		t.Fatalf("RunInstances with its root volume still provisioning: %v", err)
	}
	got, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: consoleInstance})
	if err != nil {
		t.Fatal(err)
	}
	if st := got.GetStatus().GetObserved().GetState(); st == ready || st == failed {
		t.Fatalf("instance with an unprovisioned root volume is %s", st)
	}
}

// TestProvisioningRootVolumeWaitsThenBoots drives the accepted instance
// through the reconciler: it stays pending while storage provisions, boots
// on the claim storage then reports, and fails with storage's reason if
// storage gives up or the volume goes away.
func TestProvisioningRootVolumeWaitsThenBoots(t *testing.T) {
	ctx := context.Background()
	sc := storageClient{vols: map[string]*storagev1.Volume{
		volBase + "vol-a": storageVolume(reconciling, "", ""),
		volBase + "vol-b": storageVolume(reconciling, "", ""),
		volBase + "vol-c": storageVolume(reconciling, "", ""),
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

	for i, id := range []string{"i-000000000000000a1", "i-000000000000000b1", "i-000000000000000c1"} {
		vol := "vol-" + string("abc"[i])
		req := "9191919" + string(rune('0'+i)) + "-9191-4191-8191-919191919191"
		if _, err := ins.CreateInstance(ctx, runInstance(id, req, vol)); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	state := func(id string) (commonv1.ReconciliationState, string) {
		t.Helper()
		got, err := ins.GetInstance(ctx, &computev1.GetInstanceRequest{Name: zoneParent + "/instances/" + id})
		if err != nil {
			t.Fatal(err)
		}
		return got.GetStatus().GetObserved().GetState(), got.GetStatus().GetObserved().GetMessage()
	}

	// Still provisioning: no machine, nothing READY or ERROR, retried.
	for range 3 {
		_ = rec.Sweep(ctx) //nolint:errcheck // a sweep with pending volumes reports them as transient
	}
	for _, id := range []string{"i-000000000000000a1", "i-000000000000000b1", "i-000000000000000c1"} {
		if st, msg := state(id); st == ready || st == failed {
			t.Fatalf("%s while its root volume provisions = %s %q", id, st, msg)
		}
		if _, ok := agent.Machine(id); ok {
			t.Fatalf("%s got a machine before its root volume had a claim", id)
		}
	}

	// a: storage finishes. b: storage gives up. c: storage removed it.
	sc.vols[volBase+"vol-a"] = storageVolume(ready, "", "pvc-a")
	sc.vols[volBase+"vol-b"] = storageVolume(failed, "image source is not supported", "")
	delete(sc.vols, volBase+"vol-c")
	_ = rec.Sweep(ctx) //nolint:errcheck // b and c fail permanently; the states below are the check

	if st, msg := state("i-000000000000000a1"); st != ready {
		t.Fatalf("a after its claim appeared = %s %q, want READY", st, msg)
	}
	if m, ok := agent.Machine("i-000000000000000a1"); !ok || m.BootClaim != "pvc-a" {
		t.Fatalf("a machine = %+v, %v; want boot claim pvc-a", m, ok)
	}
	if st, msg := state("i-000000000000000b1"); st != failed || !strings.Contains(msg, "image source is not supported") {
		t.Fatalf("b after storage gave up = %s %q, want ERROR with storage's reason", st, msg)
	}
	if st, msg := state("i-000000000000000c1"); st != failed || !strings.Contains(msg, "does not exist") {
		t.Fatalf("c after its volume went away = %s %q, want ERROR", st, msg)
	}
}
