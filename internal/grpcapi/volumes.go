package grpcapi

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
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
	// ClaimName is the PersistentVolumeClaim behind the volume, from
	// VolumeStatus.claim_name. Empty until storage has provisioned it.
	ClaimName string
	// State and Message are the volume's status.observed state and message
	// as storage reports them. A volume storage gave up on is
	// RECONCILIATION_STATE_ERROR, with the reason in Message.
	State   commonv1.ReconciliationState
	Message string
	// Reason is the READY condition's reason token. Claims reads it for an
	// ERROR volume and does not parse Message.
	Reason string
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
		return VolumeInfo{}, fmt.Errorf("get volume %s: %w", name, err)
	}
	return VolumeInfo{
		Instance: v.GetStatus().GetInstance(),
		Deleting: v.GetDeleteTime() != nil ||
			v.GetStatus().GetObserved().GetState() == commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING,
		ClaimName: v.GetStatus().GetClaimName(),
		State:     v.GetStatus().GetObserved().GetState(),
		Message:   v.GetStatus().GetObserved().GetMessage(),
		Reason:    readyReason(v.GetStatus().GetObserved().GetConditions()),
	}, nil
}

func readyReason(conds []*commonv1.Condition) string {
	for _, c := range conds {
		if c.GetType() == commonv1.ConditionType_CONDITION_TYPE_READY {
			return c.GetReason()
		}
	}
	return ""
}

// DialStorage connects lazily to the storage service at addr and returns a
// lookup over it and a function that closes the connection.
func DialStorage(addr string) (VolumeLookup, func() error, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("dial storage %s: %w", addr, err)
	}
	return NewStorageVolumes(storagev1.NewVolumeServiceClient(conn)), conn.Close, nil
}

// Claims resolves, through l, the claim of a volume that was still
// provisioning when its instance was accepted (see WithPendingClaims). It is
// a reconciler.ClaimFunc. Still provisioning is "" and no error, so the
// instance waits. A volume storage removed, is removing, or gave up on
// (ERROR) will never get a claim. An ERROR volume whose READY reason is
// ImageNotFound fails as that cause; any other such volume is
// hypervisor.Permanent. The volume message is not parsed.
func Claims(l VolumeLookup) func(ctx context.Context, volume string) (string, error) {
	return func(ctx context.Context, volume string) (string, error) {
		info, err := l.Volume(ctx, volume)
		switch {
		case errors.Is(err, ErrVolumeNotFound):
			return "", hypervisor.Permanent(fmt.Errorf("volume %s does not exist", volume))
		case err != nil:
			return "", err
		case info.Deleting:
			return "", hypervisor.Permanent(fmt.Errorf("volume %s is being deleted", volume))
		case info.State == commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR:
			err := fmt.Errorf("volume %s failed in storage: %s", volume, info.Message)
			if compute.ParseLaunchReason(info.Reason) == compute.LaunchImageNotFound {
				return "", hypervisor.ImageNotFound(err)
			}
			return "", hypervisor.Permanent(err)
		}
		return info.ClaimName, nil
	}
}

// DeleteVolume asks the storage service to delete the volume. A volume
// already gone (allow_missing) or already being deleted is success; an
// operation storage already finished with an error is returned as one.
// The request carries no request id: storage answers a repeat with the
// delete already in flight, and a fixed id would replay an old failure.
func (s storageVolumes) DeleteVolume(ctx context.Context, name string) error {
	op, err := s.c.DeleteVolume(ctx, &storagev1.DeleteVolumeRequest{Name: name, AllowMissing: true})
	if err != nil {
		return fmt.Errorf("delete volume %s: %w", name, err)
	}
	if e := op.GetError(); e != nil {
		return fmt.Errorf("delete volume %s: %s", name, e.GetMessage())
	}
	return nil
}

// VolumeDeleter is a VolumeLookup that can also delete volumes.
type VolumeDeleter interface {
	DeleteVolume(ctx context.Context, name string) error
}

// DeleteVolumes returns l's DeleteVolume as a reconciler.VolumeDeleter, or
// nil when l cannot delete.
func DeleteVolumes(l VolumeLookup) func(ctx context.Context, volume string) error {
	d, ok := l.(VolumeDeleter)
	if !ok {
		return nil
	}
	return d.DeleteVolume
}
