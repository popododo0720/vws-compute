package grpcapi

import (
	"context"
	"strings"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

func (s *server) GetKeyPair(ctx context.Context, r *computev1.GetKeyPairRequest) (*computev1.KeyPair, error) {
	k, _, err := s.liveKeyPair(ctx, r.GetName())
	if err != nil {
		return nil, err
	}
	return keyPairPB(k)
}

func (s *server) ListKeyPairs(ctx context.Context, r *computev1.ListKeyPairsRequest) (*computev1.ListKeyPairsResponse, error) {
	project, region, err := parseRegionalParent(r.GetParent())
	if err != nil {
		return nil, err
	}
	prefix := keyPairResourceName(project, region, "")
	var matched []compute.KeyPair
	after := ""
	for {
		page, err := s.read.KeyPairs(ctx, store.KeyPairQuery{PageSize: store.MaxPageSize, After: after})
		if err != nil {
			return nil, toStatus(err)
		}
		for _, k := range page.Items {
			if strings.HasPrefix(k.Spec.Name.String(), prefix) {
				matched = append(matched, k)
			}
		}
		if page.Next == "" {
			break
		}
		after = page.Next
	}
	page, next, err := cutPage(matched, r.GetPageSize(), r.GetPageToken(), func(k compute.KeyPair) string { return k.Spec.Name.String() })
	if err != nil {
		return nil, err
	}
	out := make([]*computev1.KeyPair, 0, len(page))
	for _, k := range page {
		pb, err := keyPairPB(k)
		if err != nil {
			return nil, toStatus(err)
		}
		out = append(out, pb)
	}
	return &computev1.ListKeyPairsResponse{KeyPairs: out, NextPageToken: next}, nil
}

func (s *server) CreateKeyPair(ctx context.Context, r *computev1.CreateKeyPairRequest) (*longrunningpb.Operation, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	project, region, err := parseRegionalParent(r.GetParent())
	if err != nil {
		return nil, err
	}
	if err := validateResourceID(r.GetKeyPairId()); err != nil {
		return nil, err
	}
	name := keyPairResourceName(project, region, r.GetKeyPairId())
	return s.importKeyPair(ctx, project, name, r.GetKeyPair(), r.GetRequestId(), r.GetValidateOnly())
}

func (s *server) UpdateKeyPair(ctx context.Context, r *computev1.UpdateKeyPairRequest) (*longrunningpb.Operation, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	kp := r.GetKeyPair()
	if kp == nil {
		return nil, invalidSpec("key_pair is required")
	}
	project, _, _, err := parseKeyPairName(kp.GetName())
	if err != nil {
		return nil, err
	}
	paths := effectiveMask(r.GetUpdateMask().GetPaths())
	if err := checkMask(paths); err != nil {
		return nil, err
	}
	if err := rejectLabels(kp.GetLabels()); err != nil {
		return nil, err
	}
	existing, name, err := s.liveKeyPair(ctx, kp.GetName())
	if err != nil {
		if r.GetAllowMissing() && statusNotFound(err) {
			return s.importKeyPair(ctx, project, name, kp, r.GetRequestId(), r.GetValidateOnly())
		}
		return nil, err
	}
	if err := checkEtag(kp.GetEtag(), etagOf(existing.Spec.Generation)); err != nil {
		return nil, err
	}
	if maskIncludes(paths, "spec") {
		if kp.GetSpec() == nil || kp.GetSpec().GetPublicKey() == "" {
			return nil, invalidSpec("key_pair.spec.public_key is required")
		}
		parsed, err := compute.ParsePublicKey(kp.GetSpec().GetPublicKey())
		if err != nil {
			return nil, toStatus(err)
		}
		if parsed != existing.Spec.PublicKey {
			return nil, invalidSpec("key_pair.spec.public_key is immutable")
		}
	}
	if r.GetValidateOnly() {
		return doneOperation(project, name, commonv1.OperationVerb_OPERATION_VERB_UPDATE, r.GetRequestId(), nil)
	}
	if op, ok, err := s.replay(ctx, project, r.GetRequestId(), existing.Spec.ID.String()); err != nil || ok {
		return op, err
	}
	pb, err := keyPairPB(existing)
	if err != nil {
		return nil, toStatus(err)
	}
	return doneOperation(project, name, commonv1.OperationVerb_OPERATION_VERB_UPDATE, r.GetRequestId(), pb)
}

func (s *server) DeleteKeyPair(ctx context.Context, r *computev1.DeleteKeyPairRequest) (*longrunningpb.Operation, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	project, _, _, err := parseKeyPairName(r.GetName())
	if err != nil {
		return nil, err
	}
	existing, name, err := s.liveKeyPair(ctx, r.GetName())
	if err != nil {
		if r.GetAllowMissing() && statusNotFound(err) {
			return doneOperation(project, r.GetName(), commonv1.OperationVerb_OPERATION_VERB_DELETE, r.GetRequestId(), nil)
		}
		return nil, err
	}
	if err := checkEtag(r.GetEtag(), etagOf(existing.Spec.Generation)); err != nil {
		return nil, err
	}
	in, err := compute.NewDeleteKeyPair(name)
	rc, err := s.submit(ctx, r.GetRequestId(), in, err)
	if err != nil {
		return nil, err
	}
	return s.operationPB(ctx, project, rc.Operation)
}

func (s *server) ReportKeyPairStatus(ctx context.Context, r *computev1.ReportKeyPairStatusRequest) (*computev1.ReportKeyPairStatusResponse, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	if r.GetStatus() == nil || r.GetStatus().GetObserved() == nil {
		return nil, invalidSpec("status.observed is required")
	}
	k, _, err := s.liveKeyPair(ctx, r.GetName())
	if err != nil {
		return nil, err
	}
	if r.GetStatus().GetObserved().GetGeneration() > k.Spec.Generation {
		return nil, aborted("GENERATION_CONFLICT", "observed generation is ahead of the stored generation")
	}
	pb, err := keyPairPB(k)
	if err != nil {
		return nil, toStatus(err)
	}
	return &computev1.ReportKeyPairStatusResponse{KeyPair: pb}, nil
}

func (s *server) importKeyPair(ctx context.Context, project, name string, kp *computev1.KeyPair, requestID string, validateOnly bool) (*longrunningpb.Operation, error) {
	if kp == nil || kp.GetSpec() == nil || kp.GetSpec().GetPublicKey() == "" {
		return nil, invalidSpec("key_pair.spec.public_key is required")
	}
	if kp.GetName() != "" && kp.GetName() != name {
		return nil, invalidID("key_pair.name does not match parent and key_pair_id")
	}
	if kp.GetUid() != "" || kp.GetGeneration() != 0 || kp.GetEtag() != "" || kp.GetStatus() != nil ||
		kp.GetCreateTime() != nil || kp.GetUpdateTime() != nil || kp.GetDeleteTime() != nil {
		return nil, invalidSpec("output-only fields must be unset")
	}
	if err := rejectLabels(kp.GetLabels()); err != nil {
		return nil, err
	}
	if _, err := compute.ParsePublicKey(kp.GetSpec().GetPublicKey()); err != nil {
		return nil, toStatus(err)
	}
	if validateOnly {
		if _, _, err := s.liveKeyPair(ctx, name); err == nil {
			return nil, alreadyExists("key pair " + name + " already exists")
		} else if !statusNotFound(err) {
			return nil, err
		}
		return doneOperation(project, name, commonv1.OperationVerb_OPERATION_VERB_CREATE, requestID, nil)
	}
	in, err := compute.NewImportKeyPair(name, kp.GetSpec().GetPublicKey())
	rc, err := s.submit(ctx, requestID, in, err)
	if err != nil {
		return nil, err
	}
	return s.operationPB(ctx, project, rc.Operation)
}

func (s *server) liveKeyPair(ctx context.Context, name string) (compute.KeyPair, string, error) {
	if _, _, _, err := parseKeyPairName(name); err != nil {
		return compute.KeyPair{}, "", err
	}
	keyName, err := compute.ParseKeyName(name)
	if err != nil {
		return compute.KeyPair{}, "", toStatus(err)
	}
	k, err := s.read.KeyPairByName(ctx, keyName)
	if err != nil {
		return compute.KeyPair{}, name, toStatus(err)
	}
	if k.Spec.Deleted || k.Spec.Name.String() != name {
		return compute.KeyPair{}, name, notFound("key pair " + name + " not found")
	}
	return k, name, nil
}
