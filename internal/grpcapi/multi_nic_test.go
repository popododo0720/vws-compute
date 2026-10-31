package grpcapi_test

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// TestCreateInstanceRefusesBadNetworkInterfaces: two interfaces at one device
// index (nic1 and nic01) and a direct subnet whose network name cannot exist
// are refused as invalid arguments, as EC2 refuses a repeated device index
// with InvalidParameterValue, and no instance is stored. A direct subnet at
// device index 0 next to an overlay one is accepted.
func TestCreateInstanceRefusesBadNetworkInterfaces(t *testing.T) {
	ctx := context.Background()
	st := &fakeStorage{vols: map[string]grpcapi.VolumeInfo{volBase + "root-1": {ClaimName: "pvc-root-1"}}}
	h := serveRepo(t, memstore.New(), grpcapi.WithVolumes(st))
	const overlay = "projects/demo/regions/r/vpcs/v/subnets/subnet-a"
	const direct = "projects/demo/regions/r/vpcs/vpc-direct/subnets/subnet-direct-"
	cases := []struct {
		name   string
		nics   []*computev1.NetworkInterface
		reason string
	}{
		{"repeated device index", []*computev1.NetworkInterface{
			{DeviceName: "nic1", Subnet: overlay}, {DeviceName: "nic01", Subnet: direct + "prod"},
		}, "INVALID_SPEC"},
		{"direct name too long", []*computev1.NetworkInterface{
			{DeviceName: "nic0", Subnet: direct + "abcdefghijklmnop"},
		}, "INVALID_RESOURCE_ID"},
		{"direct name starts with a digit", []*computev1.NetworkInterface{
			{DeviceName: "nic0", Subnet: direct + "0prod"},
		}, "INVALID_RESOURCE_ID"},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := createReq("web-1", fmt.Sprintf("4141414%d-4141-4141-8141-414141414141", i), "root-1")
			req.Instance.Spec.NetworkInterfaces = c.nics
			_, err := h.inst.CreateInstance(ctx, req)
			wantCode(t, err, codes.InvalidArgument)
			wantReason(t, err, c.reason)
			get := &computev1.GetInstanceRequest{Name: zoneParent + "/instances/web-1"}
			if _, err := h.inst.GetInstance(ctx, get); err == nil {
				t.Fatal("a refused create stored an instance")
			}
		})
	}

	req := createReq("web-1", "42424242-4242-4242-8242-424242424242", "root-1")
	req.Instance.Spec.NetworkInterfaces = []*computev1.NetworkInterface{
		{DeviceName: "nic0", Subnet: direct + "prod", PrivateIpv4: "192.168.200.30"},
		{DeviceName: "nic1", Subnet: overlay},
	}
	if _, err := h.inst.CreateInstance(ctx, req); err != nil {
		t.Fatalf("direct at device index 0: %v", err)
	}
}
