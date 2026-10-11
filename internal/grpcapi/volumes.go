package grpcapi

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"
)

// ErrVolumeNotFound is what a VolumeLookup returns for a volume the storage
// service does not have.
var ErrVolumeNotFound = errors.New("volume not found")

// VolumeInfo is what CreateInstance needs to know about a volume.
type VolumeInfo struct {
	// Instance is the instance the storage service says holds the volume.
	Instance string
	// Deleting is true once the volume is being removed.
	Deleting bool
}

// VolumeLookup reads volumes from the storage service. Any error other than
// ErrVolumeNotFound means the service could not answer.
type VolumeLookup interface {
	Volume(ctx context.Context, name string) (VolumeInfo, error)
}

type storageVolumes struct{ c storagev1.VolumeServiceClient }

// NewStorageVolumes looks volumes up through the storage service's gRPC API.
func NewStorageVolumes(c storagev1.VolumeServiceClient) VolumeLookup { return storageVolumes{c} }

func (s storageVolumes) Volume(ctx context.Context, name string) (VolumeInfo, error) {
	v, err := s.c.GetVolume(ctx, &storagev1.GetVolumeRequest{Name: name})
	if status.Code(err) == codes.NotFound {
		return VolumeInfo{}, ErrVolumeNotFound
	}
	if err != nil {
		return VolumeInfo{}, err
	}
	return VolumeInfo{
		Instance: v.GetStatus().GetInstance(),
		Deleting: v.GetDeleteTime() != nil ||
			v.GetStatus().GetObserved().GetState() == commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING,
	}, nil
}

// DialStorage connects lazily to the storage service at addr and returns a
// lookup over it and a function that closes the connection.
func DialStorage(addr string) (VolumeLookup, func(), error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return NewStorageVolumes(storagev1.NewVolumeServiceClient(conn)), func() { _ = conn.Close() }, nil
}
