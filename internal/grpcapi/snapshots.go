package grpcapi

import "context"

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
