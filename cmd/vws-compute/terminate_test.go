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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

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

func (f *fakeVolumes) DeleteVolume(
	_ context.Context,
	r *storagev1.DeleteVolumeRequest,
) (*longrunningpb.Operation, error) {
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

// TestTerminateHonorsDeleteOnTermination runs vws-compute against a fake
// vws-storage (vws-compute#173). Get and List return each attachment's
// delete_on_termination, unset taking the EC2 default. After terminate,
// compute deletes in storage, once each, exactly the volumes marked true:
// an unset root volume and a data volume set true. A root volume set false
// and data volumes set false or unset stay.
func TestTerminateHonorsDeleteOnTermination(t *testing.T) {
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
	cfg, err := parseConfig(
		[]string{"-listen", addr, "-storage", slis.Addr().String()},
		func(string) string { return "" },
		io.Discard,
	)
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

	type attach struct {
		volume, device string
		boot           bool
		set            *bool
		want           bool
	}
	instances := []struct {
		id, createID, deleteID string
		volumes                []attach
	}{
		{
			id:       "i-0123456789abcdef0",
			createID: "a1a1a1a1-a1a1-41a1-81a1-a1a1a1a1a1a1", deleteID: "b2b2b2b2-b2b2-42b2-82b2-b2b2b2b2b2b2",
			volumes: []attach{
				{volume: "vol-root", device: "boot", boot: true, want: true},
				{volume: "vol-data", device: "vdb", want: false},
				{volume: "vol-data-delete", device: "vdc", set: proto.Bool(true), want: true},
				{volume: "vol-data-keep", device: "vdd", set: proto.Bool(false), want: false},
			},
		},
		{
			id:       "i-0123456789abcdef1",
			createID: "c3c3c3c3-c3c3-43c3-83c3-c3c3c3c3c3c3", deleteID: "d4d4d4d4-d4d4-44d4-84d4-d4d4d4d4d4d4",
			volumes: []attach{
				{volume: "vol-root-keep", device: "boot", boot: true, set: proto.Bool(false), want: false},
			},
		},
	}
	name := func(id string) string { return "projects/demo/zones/vws-1a/instances/" + id }
	wantOn := func(id string) map[string]bool {
		m := map[string]bool{}
		for _, in := range instances {
			if in.id == id {
				for _, v := range in.volumes {
					m[termVolumes+v.volume] = v.want
				}
			}
		}
		return m
	}
	check := func(got *computev1.Instance) {
		t.Helper()
		want := wantOn(got.GetName()[len(name("")):])
		for _, v := range got.GetSpec().GetVolumes() {
			if v.DeleteOnTermination == nil || v.GetDeleteOnTermination() != want[v.GetVolume()] {
				t.Fatalf(
					"%s %s delete_on_termination = %v (set %v), want %v",
					got.GetName(),
					v.GetVolume(),
					v.GetDeleteOnTermination(),
					v.DeleteOnTermination != nil,
					want[v.GetVolume()],
				)
			}
		}
		if len(got.GetSpec().GetVolumes()) != len(want) {
			t.Fatalf("%s volumes = %v", got.GetName(), got.GetSpec().GetVolumes())
		}
	}
	state := func(id string) commonv1.ReconciliationState {
		got, err := ins.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name(id)})
		if err != nil {
			return commonv1.ReconciliationState_RECONCILIATION_STATE_UNSPECIFIED
		}
		return got.GetStatus().GetObserved().GetState()
	}
	for _, in := range instances {
		var spec []*computev1.VolumeAttachment
		for _, v := range in.volumes {
			spec = append(
				spec,
				&computev1.VolumeAttachment{
					Volume:              termVolumes + v.volume,
					DeviceName:          v.device,
					Boot:                v.boot,
					DeleteOnTermination: v.set,
				},
			)
		}
		eventually(t, "CreateInstance", func() bool {
			_, err := ins.CreateInstance(ctx, &computev1.CreateInstanceRequest{
				Parent: "projects/demo/zones/vws-1a", InstanceId: in.id, RequestId: in.createID,
				Instance: &computev1.Instance{Spec: &computev1.InstanceSpec{
					MachineType: "vws.small", Power: computev1.InstancePower_INSTANCE_POWER_RUNNING, Volumes: spec,
				}},
			})
			return err == nil
		})
		eventually(
			t,
			"instance READY",
			func() bool { return state(in.id) == commonv1.ReconciliationState_RECONCILIATION_STATE_READY },
		)
		got, err := ins.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name(in.id)})
		if err != nil {
			t.Fatal(err)
		}
		check(got)
	}
	list, err := ins.ListInstances(ctx, &computev1.ListInstancesRequest{Parent: "projects/demo/zones/vws-1a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.GetInstances()) != len(instances) {
		t.Fatalf("ListInstances = %d instances, want %d", len(list.GetInstances()), len(instances))
	}
	for _, got := range list.GetInstances() {
		check(got)
	}
	if got := vols.seen(); len(got) != 0 {
		t.Fatalf("deletes before terminate = %v", got)
	}
	for _, in := range instances {
		req := &computev1.DeleteInstanceRequest{Name: name(in.id), RequestId: in.deleteID}
		if _, err := ins.DeleteInstance(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	for _, in := range instances {
		eventually(t, in.id+" removed", func() bool {
			_, err := ins.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name(in.id)})
			return status.Code(err) == codes.NotFound
		})
	}
	eventually(t, "volumes deleted", func() bool { return len(vols.seen()) >= 2 })
	time.Sleep(300 * time.Millisecond) // several more sweeps: each release is recorded once
	got := vols.seen()
	slices.Sort(got)
	if want := []string{termVolumes + "vol-data-delete", termVolumes + "vol-root"}; !slices.Equal(got, want) {
		t.Fatalf("DeleteVolume calls = %v, want %v, once each", got, want)
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
