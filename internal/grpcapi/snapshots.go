package grpcapi

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

// SnapshotPermissions reads a snapshot's createVolumePermission, which EC2
// sets with ModifySnapshotAttribute. The snapshot owner keeps it with the
// snapshot, so this service only reads it.
type SnapshotPermissions interface {
	// CanCreateVolume reports whether project may create a volume or image
	// from snapshot: its owner added project to the snapshot's
	// createVolumePermission or added the group "all". A snapshot that does
	// not exist is false with no error, so an unshared snapshot and an absent
	// one look the same. An error means the owner of the permission could not
	// answer.
	CanCreateVolume(ctx context.Context, snapshot, project string) (bool, error)
}

// ErrSnapshotNotFound is what a SnapshotLookup returns for a snapshot the
// storage service does not have.
var ErrSnapshotNotFound = errors.New("snapshot not found")

// SnapshotInfo is what an image copy needs to know about a snapshot.
type SnapshotInfo struct {
	// ObjectName is the VolumeSnapshot behind the snapshot, from
	// SnapshotStatus.snapshot_object_name. Empty until storage has created it.
	ObjectName string
	// Deleting is true once the snapshot is being removed.
	Deleting bool
	// State and Message are the snapshot's status.observed state and message.
	State   commonv1.ReconciliationState
	Message string
}

// SnapshotLookup reads snapshots from the storage service. Any error other
// than ErrSnapshotNotFound means the service could not answer.
type SnapshotLookup interface {
	Snapshot(ctx context.Context, name string) (SnapshotInfo, error)
}

type storageSnapshots struct {
	c storagev1.SnapshotServiceClient
}

// NewStorageSnapshots looks snapshots up through the storage service's gRPC API.
func NewStorageSnapshots(c storagev1.SnapshotServiceClient) SnapshotLookup {
	return storageSnapshots{c}
}

func (s storageSnapshots) Snapshot(ctx context.Context, name string) (SnapshotInfo, error) {
	v, err := s.c.GetSnapshot(ctx, &storagev1.GetSnapshotRequest{Name: name})
	if status.Code(err) == codes.NotFound {
		return SnapshotInfo{}, ErrSnapshotNotFound
	}
	if err != nil {
		return SnapshotInfo{}, fmt.Errorf("get snapshot %s: %w", name, err)
	}
	state := v.GetStatus().GetObserved().GetState()
	return SnapshotInfo{
		ObjectName: v.GetStatus().GetSnapshotObjectName(),
		Deleting:   v.GetDeleteTime() != nil || state == commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING,
		State:      state,
		Message:    v.GetStatus().GetObserved().GetMessage(),
	}, nil
}

// SnapshotObjects resolves, through l, the VolumeSnapshot an image copy of a
// snapshot clones from. It is a reconciler.SnapshotFunc. "" with no error
// means storage has not created the object yet, so the image waits. A snapshot
// storage removed, is removing, or gave up on (ERROR) will never have one and
// is hypervisor.Permanent.
func SnapshotObjects(l SnapshotLookup) func(ctx context.Context, snapshot string) (string, error) {
	return func(ctx context.Context, snapshot string) (string, error) {
		info, err := l.Snapshot(ctx, snapshot)
		switch {
		case errors.Is(err, ErrSnapshotNotFound):
			return "", hypervisor.Permanent(fmt.Errorf("snapshot %s does not exist", snapshot))
		case err != nil:
			return "", err
		case info.Deleting:
			return "", hypervisor.Permanent(fmt.Errorf("snapshot %s is being deleted", snapshot))
		case info.State == commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR:
			return "", hypervisor.Permanent(fmt.Errorf("snapshot %s failed in storage: %s", snapshot, info.Message))
		}
		return info.ObjectName, nil
	}
}
