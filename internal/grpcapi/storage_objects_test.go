package grpcapi_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
)

// objectOf is the name the fake storage service reports for a volume or
// snapshot: a hash that shares nothing with the resource id, as vws-storage's
// vol-<hash> and snap-<hash> do.
func objectOf(prefix, ref string) string {
	sum := sha256.Sum256([]byte(ref))
	return prefix + "-" + hex.EncodeToString(sum[:8])
}

func fakeClaim(_ context.Context, volume string) (string, error) {
	return objectOf("vol", volume), nil
}

func fakeSnapshotObject(_ context.Context, snapshot string) (string, error) {
	return objectOf("snap", snapshot), nil
}

// imageLog is a fake agent that remembers every image spec it was given.
type imageLog struct {
	*fakeagent.Agent
	mu    sync.Mutex
	specs []hypervisor.ImageSpec
}

func (a *imageLog) ConvergeImage(ctx context.Context, spec hypervisor.ImageSpec) (int64, error) {
	a.mu.Lock()
	a.specs = append(a.specs, spec)
	a.mu.Unlock()
	return a.Agent.ConvergeImage(ctx, spec)
}

func (a *imageLog) sources() []hypervisor.ImageSpec {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]hypervisor.ImageSpec(nil), a.specs...)
}

// TestCreateImageClonesTheObjectStorageReported is vws-compute#24 through the
// gRPC edge: the volume and snapshot names a caller sends are resource names,
// and the agent clones the claim and VolumeSnapshot storage reports for them.
func TestCreateImageClonesTheObjectStorageReported(t *testing.T) {
	const parent = "projects/demo/regions/demo"
	for _, tc := range []struct {
		name   string
		source func(*computev1.ImageSpec)
		ref    string
		want   func(hypervisor.ImageSpec) (got, want string)
	}{
		{
			name: "volume",
			ref:  "projects/demo/zones/demoa/volumes/root",
			source: func(s *computev1.ImageSpec) {
				s.Source = &computev1.ImageSpec_Volume{Volume: "projects/demo/zones/demoa/volumes/root"}
			},
			want: func(i hypervisor.ImageSpec) (string, string) {
				return i.SourceClaim, objectOf("vol", "projects/demo/zones/demoa/volumes/root")
			},
		},
		{
			name: "snapshot",
			ref:  parent + "/snapshots/snap",
			source: func(s *computev1.ImageSpec) {
				s.Source = &computev1.ImageSpec_Snapshot{Snapshot: parent + "/snapshots/snap"}
			},
			want: func(i hypervisor.ImageSpec) (string, string) {
				return i.SourceVolumeSnapshot, objectOf("snap", parent+"/snapshots/snap")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := serve(t)
			spec := &computev1.ImageSpec{}
			tc.source(spec)
			op, err := h.img.CreateImage(ctx, &computev1.CreateImageRequest{
				Parent: parent, ImageId: "ami-0000000000000aaa1", RequestId: "12121212-1212-4212-8212-121212121212",
				Image: &computev1.Image{Spec: spec},
			})
			if err != nil {
				t.Fatal(err)
			}
			done, err := h.ops.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{
				Name: op.GetName(), Timeout: durationpb.New(10 * time.Second),
			})
			if err != nil || done.GetError() != nil {
				t.Fatalf("operation = %v, %v", done, err)
			}
			specs := h.images.sources()
			if len(specs) != 1 {
				t.Fatalf("agent got %d image specs, want 1: %+v", len(specs), specs)
			}
			if got, want := tc.want(specs[0]); got != want || got == "" {
				t.Fatalf("agent source = %q, want what storage reported, %q", got, want)
			}
			if strings.Contains(specs[0].SourceClaim+specs[0].SourceVolumeSnapshot, "demo") {
				t.Fatalf("the resource name reached the agent: %+v", specs[0])
			}
		})
	}
}

type fakeSnapshots struct {
	storagev1.SnapshotServiceClient
	snap *storagev1.Snapshot
	err  error
}

func (f fakeSnapshots) GetSnapshot(
	context.Context, *storagev1.GetSnapshotRequest, ...grpc.CallOption,
) (*storagev1.Snapshot, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.snap, nil
}

func snapshot(state commonv1.ReconciliationState, object string) *storagev1.Snapshot {
	return &storagev1.Snapshot{Status: &storagev1.SnapshotStatus{
		Observed:           &commonv1.ObservedState{State: state, Message: "copy failed"},
		SnapshotObjectName: object,
	}}
}

func TestSnapshotObjects(t *testing.T) {
	const (
		ready       = commonv1.ReconciliationState_RECONCILIATION_STATE_READY
		reconciling = commonv1.ReconciliationState_RECONCILIATION_STATE_RECONCILING
	)
	deleted := snapshot(ready, "snap-1a2b")
	deleted.DeleteTime = timestamppb.Now()
	transient := errors.New("connection refused")
	for _, tc := range []struct {
		name      string
		client    fakeSnapshots
		want      string
		permanent bool
		wantErr   string
	}{
		{name: "named", client: fakeSnapshots{snap: snapshot(ready, "snap-1a2b")}, want: "snap-1a2b"},
		{name: "being created has no name yet", client: fakeSnapshots{snap: snapshot(reconciling, "")}},
		{
			name:      "not found",
			client:    fakeSnapshots{err: status.Error(codes.NotFound, "no snapshot")},
			permanent: true, wantErr: "does not exist",
		},
		{name: "delete time set", client: fakeSnapshots{snap: deleted}, permanent: true, wantErr: "being deleted"},
		{
			name:      "deleting state",
			client:    fakeSnapshots{snap: snapshot(commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING, "snap-1a2b")},
			permanent: true, wantErr: "being deleted",
		},
		{
			name:      "error state",
			client:    fakeSnapshots{snap: snapshot(commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR, "snap-1a2b")},
			permanent: true, wantErr: "failed in storage: copy failed",
		},
		{name: "storage unreachable", client: fakeSnapshots{err: transient}, wantErr: "connection refused"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolve := grpcapi.SnapshotObjects(grpcapi.NewStorageSnapshots(tc.client))
			got, err := resolve(context.Background(), "projects/p/regions/r/snapshots/s")
			if tc.wantErr == "" {
				if err != nil || got != tc.want {
					t.Fatalf("got %q, %v, want %q", got, err, tc.want)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) || hypervisor.IsPermanent(err) != tc.permanent {
				t.Fatalf("got %q, %v, want permanent=%v error containing %q", got, err, tc.permanent, tc.wantErr)
			}
			if got != "" {
				t.Fatalf("got name %q with an error", got)
			}
		})
	}
}
