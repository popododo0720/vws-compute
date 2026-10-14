package main

import (
	"context"
	"io"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"
)

const termVolumes = "projects/demo/zones/vws-1a/volumes/"

// fakeVolumes is a vws-storage VolumeService that knows two ready volumes
// and records DeleteVolume calls.
type fakeVolumes struct {
	storagev1.UnimplementedVolumeServiceServer
	mu      sync.Mutex
	deletes []string
}

func (f *fakeVolumes) GetVolume(_ context.Context, r *storagev1.GetVolumeRequest) (*storagev1.Volume, error) {
	return &storagev1.Volume{Name: r.GetName(), Status: &storagev1.VolumeStatus{
		Observed:  &commonv1.ObservedState{State: commonv1.ReconciliationState_RECONCILIATION_STATE_READY},
		ClaimName: "pvc-" + r.GetName()[len(termVolumes):],
	}}, nil
}

func (f *fakeVolumes) DeleteVolume(_ context.Context, r *storagev1.DeleteVolumeRequest) (*longrunningpb.Operation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes = append(f.deletes, r.GetName())
	return &longrunningpb.Operation{Name: "operations/del", Done: true}, nil
}

func (f *fakeVolumes) seen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.deletes)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// TestTerminateDeletesRootVolume runs vws-compute against a fake vws-storage.
// After an instance with a root and a data volume is terminated, compute
// deletes the root volume in storage once (DeleteOnTermination defaults to
// true for the root device) and keeps the data volume. On main the root
// volume was left behind.
func TestTerminateDeletesRootVolume(t *testing.T) {
	vols := &fakeVolumes{}
	slis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ssrv := grpc.NewServer()
	storagev1.RegisterVolumeServiceServer(ssrv, vols)
	go func() { _ = ssrv.Serve(slis) }()
	t.Cleanup(ssrv.Stop)

	addr := freeAddr(t)
	cfg, err := parseConfig([]string{"-listen", addr, "-storage", slis.Addr().String()}, func(string) string { return "" }, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	t.Cleanup(func() { cancel(); <-done })

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	ins := computev1.NewInstanceServiceClient(cc)
	name := "projects/demo/zones/vws-1a/instances/i-0123456789abcdef0"
	eventually(t, "CreateInstance", func() bool {
		_, err := ins.CreateInstance(ctx, &computev1.CreateInstanceRequest{
			Parent: "projects/demo/zones/vws-1a", InstanceId: "i-0123456789abcdef0", RequestId: "a1a1a1a1-a1a1-41a1-81a1-a1a1a1a1a1a1",
			Instance: &computev1.Instance{Spec: &computev1.InstanceSpec{
				MachineType: "vws.small", Power: computev1.InstancePower_INSTANCE_POWER_RUNNING,
				Volumes: []*computev1.VolumeAttachment{
					{Volume: termVolumes + "vol-root", DeviceName: "boot", Boot: true},
					{Volume: termVolumes + "vol-data", DeviceName: "vdb"},
				},
			}},
		})
		return err == nil
	})
	state := func() commonv1.ReconciliationState {
		got, err := ins.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
		if err != nil {
			return commonv1.ReconciliationState_RECONCILIATION_STATE_UNSPECIFIED
		}
		return got.GetStatus().GetObserved().GetState()
	}
	eventually(t, "instance READY", func() bool { return state() == commonv1.ReconciliationState_RECONCILIATION_STATE_READY })
	if got := vols.seen(); len(got) != 0 {
		t.Fatalf("deletes before terminate = %v", got)
	}
	if _, err := ins.DeleteInstance(ctx, &computev1.DeleteInstanceRequest{Name: name, RequestId: "b2b2b2b2-b2b2-42b2-82b2-b2b2b2b2b2b2"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "root volume deleted", func() bool { return len(vols.seen()) > 0 })
	time.Sleep(300 * time.Millisecond) // several more sweeps: the release is recorded once
	if got := vols.seen(); !slices.Equal(got, []string{termVolumes + "vol-root"}) {
		t.Fatalf("DeleteVolume calls = %v, want only the root volume, once", got)
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
