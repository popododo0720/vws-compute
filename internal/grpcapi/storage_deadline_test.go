package grpcapi_test

import (
	"context"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc"

	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-compute/internal/grpcapi"
)

// hangingStorage blocks every call until its context ends, as a storage
// service that accepted the connection and never answers.
type hangingStorage struct {
	storagev1.VolumeServiceClient
	storagev1.SnapshotServiceClient
}

func hang(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func (hangingStorage) GetVolume(
	ctx context.Context, _ *storagev1.GetVolumeRequest, _ ...grpc.CallOption,
) (*storagev1.Volume, error) {
	return &storagev1.Volume{}, hang(ctx)
}

func (hangingStorage) DeleteVolume(
	ctx context.Context, _ *storagev1.DeleteVolumeRequest, _ ...grpc.CallOption,
) (*longrunningpb.Operation, error) {
	return &longrunningpb.Operation{}, hang(ctx)
}

func (hangingStorage) GetSnapshot(
	ctx context.Context, _ *storagev1.GetSnapshotRequest, _ ...grpc.CallOption,
) (*storagev1.Snapshot, error) {
	return &storagev1.Snapshot{}, hang(ctx)
}

// TestStorageCallsHaveADeadline: a storage service that never answers fails
// each lookup within a bounded time instead of stalling the reconciler sweep
// that called it. A call without a deadline returns at once with no error,
// which the test reports as a missing deadline.
func TestStorageCallsHaveADeadline(t *testing.T) {
	s := hangingStorage{}
	vols := grpcapi.NewStorageVolumes(s)
	calls := map[string]func(context.Context) error{
		"GetVolume": func(ctx context.Context) error {
			_, err := vols.Volume(ctx, "projects/p/zones/z/volumes/v")
			return err
		},
		"DeleteVolume": func(ctx context.Context) error {
			return vols.(grpcapi.VolumeDeleter).DeleteVolume(ctx, "projects/p/zones/z/volumes/v")
		},
		"GetSnapshot": func(ctx context.Context) error {
			_, err := grpcapi.NewStorageSnapshots(s).Snapshot(ctx, "projects/p/regions/r/snapshots/s")
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			start := time.Now()
			err := call(context.Background())
			if err == nil {
				t.Fatal("the call carried no deadline")
			}
			if d := time.Since(start); d > time.Minute {
				t.Fatalf("the call took %s", d)
			}
		})
	}
}
