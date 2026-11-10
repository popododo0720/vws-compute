package grpcapi

import (
	"context"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
)

func (s *server) RebootInstance(
	ctx context.Context, r *computev1.RebootInstanceRequest,
) (*longrunningpb.Operation, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	project, _, id, err := parseInstanceName(r.GetName())
	if err != nil {
		return nil, err
	}
	in, err := compute.NewRebootInstance(id, r.GetName())
	rc, err := s.submit(ctx, r.GetRequestId(), in, err, check(func() error {
		inst, err := s.storedInstance(ctx, r.GetName())
		if err != nil {
			return err
		}
		if inst.Spec.Power != compute.PowerRunning {
			return toStatus(compute.Errorf(compute.CodeIncorrectState, "instance %s is not running", id))
		}
		return checkEtag(r.GetEtag(), etagOf(inst.Spec.Generation, inst.Meta.Revision))
	}))
	if err != nil {
		return nil, err
	}
	return s.operationPB(ctx, project, r.GetName(), rc.Operation)
}
