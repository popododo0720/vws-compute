package grpcapi_test

import (
	"context"
	"testing"

	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// TestUserDataRawIsRefusedUntilStored keeps user_data_raw from being
// dropped silently: compute does not store it yet (#148).
func TestUserDataRawIsRefusedUntilStored(t *testing.T) {
	ctx := context.Background()
	st := &fakeStorage{vols: map[string]grpcapi.VolumeInfo{volBase + "root-1": {ClaimName: "pvc-root-1"}}}
	h := serveRepo(t, memstore.New(), grpcapi.WithVolumes(st))
	req := createReq("web-1", "47474747-4747-4747-8747-474747474747", "root-1")
	req.Instance.Spec.UserData = ""
	req.Instance.Spec.UserDataRaw = []byte{0xff, 0x00}
	_, err := h.inst.CreateInstance(ctx, req)
	wantReason(t, err, "INVALID_SPEC")
}
