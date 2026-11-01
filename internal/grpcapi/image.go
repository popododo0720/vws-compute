package grpcapi

import (
	"context"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

func (s *server) GetImage(ctx context.Context, r *computev1.GetImageRequest) (*computev1.Image, error) {
	img, name, err := s.readableImage(ctx, r.GetName())
	if err != nil {
		return nil, err
	}
	return imagePB(img, name), nil
}

func (s *server) ListImages(ctx context.Context, r *computev1.ListImagesRequest) (*computev1.ListImagesResponse, error) {
	project, region, err := parseRegionalParent(r.GetParent())
	if err != nil {
		return nil, err
	}
	var matched []compute.Image
	after := ""
	for {
		page, err := s.read.Images(ctx, store.ImageQuery{PageSize: store.MaxPageSize, After: after})
		if err != nil {
			return nil, toStatus(err)
		}
		for _, img := range page.Items {
			if img.Lifecycle() == compute.ImageDeregistered || !s.readable(img, project, region) {
				continue
			}
			matched = append(matched, img)
		}
		if page.Next == "" {
			break
		}
		after = page.Next
	}
	page, next, err := cutPage(matched, r.GetPageSize(), r.GetPageToken(), func(i compute.Image) string {
		return imageResourceName(project, region, i.Spec.ID.String())
	})
	if err != nil {
		return nil, err
	}
	out := make([]*computev1.Image, 0, len(page))
	for _, img := range page {
		out = append(out, imagePB(img, imageResourceName(project, region, img.Spec.ID.String())))
	}
	return &computev1.ListImagesResponse{Images: out, NextPageToken: next}, nil
}

func (s *server) CreateImage(ctx context.Context, r *computev1.CreateImageRequest) (*longrunningpb.Operation, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	project, region, err := parseRegionalParent(r.GetParent())
	if err != nil {
		return nil, err
	}
	name := imageResourceName(project, region, r.GetImageId())
	in, err := copyImageIntent(project, region, r.GetImageId(), r.GetImage())
	if err != nil {
		return nil, err
	}
	if r.GetValidateOnly() {
		return doneOperation(project, name, commonv1.OperationVerb_OPERATION_VERB_CREATE, r.GetRequestId(), nil)
	}
	rc, err := s.submit(ctx, r.GetRequestId(), in, nil, nil)
	if err != nil {
		return nil, err
	}
	return s.operationPB(ctx, project, "", rc.Operation)
}

// copyImageIntent parses a CreateImage request into an intent. It does not
// read the store, so validate_only and the submitting call fail on the same
// inputs and neither can see another project's image.
func copyImageIntent(project, region, imageID string, body *computev1.Image) (compute.Intent, error) {
	if err := validateResourceID(imageID); err != nil {
		return nil, err
	}
	name := imageResourceName(project, region, imageID)
	if err := validateCreateImage(name, body); err != nil {
		return nil, err
	}
	src, err := copySource(body.GetSpec())
	if err != nil {
		return nil, toStatus(err)
	}
	if err := checkSourceScope(project, region, src); err != nil {
		return nil, err
	}
	labels, err := compute.ParseLabels(body.GetLabels())
	if err != nil {
		return nil, toStatus(err)
	}
	scope, err := compute.NewImageScope(project, region)
	if err != nil {
		return nil, toStatus(err)
	}
	in, err := compute.NewCopyImage(
		imageID, scope, src, body.GetSpec().GetFamily(), body.GetSpec().GetDescription(), labels,
	)
	if err != nil {
		return nil, toStatus(err)
	}
	return in, nil
}

func copySource(spec *computev1.ImageSpec) (compute.CopySource, error) {
	if spec.GetVolume() != "" {
		ref, err := compute.ParseVolumeRef(spec.GetVolume())
		if err != nil {
			return nil, err
		}
		return compute.FromVolume{Ref: ref}, nil
	}
	ref, err := compute.ParseSnapshotRef(spec.GetSnapshot())
	if err != nil {
		return nil, err
	}
	return compute.FromSnapshot{Ref: ref}, nil
}

// checkSourceScope fails a copy source the caller cannot see as NOT_FOUND:
// a volume or snapshot of another project, a volume whose zone is not in the
// caller's region, or a snapshot of another region.
// AWS answers InvalidVolume.NotFound and InvalidSnapshot.NotFound for such a
// source. The answer depends on the names alone, so it is the same whether or
// not the source exists and says nothing about another project.
func checkSourceScope(project, region string, src compute.CopySource) error {
	switch s := src.(type) {
	case compute.FromVolume:
		if s.Ref.Project() != project || !compute.ZoneInRegion(s.Ref.Zone(), region) {
			return notFound("volume " + s.Ref.String() + " not found")
		}
	case compute.FromSnapshot:
		if s.Ref.Project() != project || s.Ref.Region() != region {
			return notFound("snapshot " + s.Ref.String() + " not found")
		}
	}
	return nil
}

func (s *server) UpdateImage(ctx context.Context, r *computev1.UpdateImageRequest) (*longrunningpb.Operation, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	body := r.GetImage()
	if body == nil {
		return nil, invalidSpec("image is required")
	}
	project, _, _, err := parseImageName(body.GetName())
	if err != nil {
		return nil, err
	}
	paths := effectiveMask(r.GetUpdateMask().GetPaths())
	if err := checkMask(paths); err != nil {
		return nil, err
	}
	patch, err := metadataPatch(paths, body.GetLabels(), imageDescription(paths, body.GetSpec()), maskIncludes(paths, "spec"))
	if err != nil {
		return nil, toStatus(err)
	}
	existing, name, err := s.liveImage(ctx, body.GetName())
	if err != nil {
		if r.GetAllowMissing() && statusNotFound(err) {
			return nil, dependencyMissing("image " + body.GetName() + " is absent and this process has no volume or snapshot to copy")
		}
		return nil, err
	}
	if maskIncludes(paths, "spec") {
		spec := body.GetSpec()
		if spec == nil {
			return nil, invalidSpec("image.spec is required")
		}
		if spec.GetVolume() != "" || spec.GetSnapshot() != "" {
			return nil, invalidSpec("image.spec.source is immutable")
		}
		if spec.GetFamily() != "" && spec.GetFamily() != existing.Spec.Name.String() {
			return nil, invalidSpec("image.spec.family is immutable")
		}
	}
	in, err := compute.NewUpdateImageMetadata(existing.Spec.ID.String(), patch)
	if r.GetValidateOnly() {
		if err != nil {
			return nil, toStatus(err)
		}
		if err := checkEtag(body.GetEtag(), etagOf(existing.Spec.Generation, existing.Meta.Revision)); err != nil {
			return nil, err
		}
		return doneOperation(project, name, commonv1.OperationVerb_OPERATION_VERB_UPDATE, r.GetRequestId(), nil)
	}
	rc, err := s.submit(ctx, r.GetRequestId(), in, err, check(func() error {
		cur, _, err := s.liveImage(ctx, body.GetName())
		if err != nil {
			return err
		}
		return checkEtag(body.GetEtag(), etagOf(cur.Spec.Generation, cur.Meta.Revision))
	}))
	if err != nil {
		return nil, err
	}
	return s.operationPB(ctx, project, name, rc.Operation)
}

func (s *server) DeleteImage(ctx context.Context, r *computev1.DeleteImageRequest) (*longrunningpb.Operation, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	project, _, id, err := parseImageName(r.GetName())
	if err != nil {
		return nil, err
	}
	if _, _, err = s.liveImage(ctx, r.GetName()); err != nil {
		if r.GetAllowMissing() && statusNotFound(err) {
			return doneOperation(project, r.GetName(), commonv1.OperationVerb_OPERATION_VERB_DELETE, r.GetRequestId(), nil)
		}
		return nil, err
	}
	in, err := compute.NewDeregisterImage(id)
	rc, err := s.submit(ctx, r.GetRequestId(), in, err, check(func() error {
		cur, _, err := s.liveImage(ctx, r.GetName())
		if err != nil {
			return err
		}
		return checkEtag(r.GetEtag(), etagOf(cur.Spec.Generation, cur.Meta.Revision))
	}))
	if err != nil {
		return nil, err
	}
	return s.operationPB(ctx, project, r.GetName(), rc.Operation)
}

func (s *server) ReportImageStatus(ctx context.Context, r *computev1.ReportImageStatusRequest) (*computev1.ReportImageStatusResponse, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	if r.GetStatus() == nil || r.GetStatus().GetObserved() == nil {
		return nil, invalidSpec("status.observed is required")
	}
	img, name, err := s.liveImage(ctx, r.GetName())
	if err != nil {
		return nil, err
	}
	if r.GetStatus().GetObserved().GetGeneration() > img.Spec.Generation {
		return nil, aborted("GENERATION_CONFLICT", "observed generation is ahead of the stored generation")
	}
	return &computev1.ReportImageStatusResponse{Image: imagePB(img, name)}, nil
}

// inScope reports whether img belongs to project and region. An image of no
// project (zero scope) belongs to none.
func inScope(img compute.Image, project, region string) bool {
	sc := img.Spec.Scope
	return !sc.IsZero() && sc.Project() == project && sc.Region() == region
}

// readable reports whether a caller in project and region may read img: it
// owns it, or img is public and in that region.
func (s *server) readable(img compute.Image, project, region string) bool {
	if inScope(img, project, region) {
		return true
	}
	sc := img.Spec.Scope
	return s.public.Has(img.Spec.ID) && !sc.IsZero() && sc.Region() == region
}

// liveImage reads the image the name addresses for its owner, as a change,
// delete, or status report needs. An image of another project or region,
// public or not, answers exactly like a missing one (#92).
func (s *server) liveImage(ctx context.Context, name string) (compute.Image, string, error) {
	return s.lookupImage(ctx, name, inScope)
}

// readableImage reads the image the name addresses for a read, which a
// public image of the same region also allows.
func (s *server) readableImage(ctx context.Context, name string) (compute.Image, string, error) {
	return s.lookupImage(ctx, name, s.readable)
}

func (s *server) lookupImage(ctx context.Context, name string, allowed func(compute.Image, string, string) bool) (compute.Image, string, error) {
	project, region, id, err := parseImageName(name)
	if err != nil {
		return compute.Image{}, "", err
	}
	iid, err := compute.ParseImageID(id)
	if err != nil {
		return compute.Image{}, name, notFound("image " + name + " not found")
	}
	img, err := s.read.Image(ctx, iid)
	if compute.CodeOf(err) == compute.CodeNotFound {
		return compute.Image{}, name, notFound("image " + name + " not found")
	}
	if err != nil {
		return compute.Image{}, name, toStatus(err)
	}
	if img.Lifecycle() == compute.ImageDeregistered || !allowed(img, project, region) {
		return compute.Image{}, name, notFound("image " + name + " not found")
	}
	return img, name, nil
}

func validateCreateImage(name string, img *computev1.Image) error {
	if img == nil || img.GetSpec() == nil {
		return invalidSpec("image.spec is required")
	}
	if img.GetName() != "" && img.GetName() != name {
		return invalidID("image.name does not match parent and image_id")
	}
	if img.GetUid() != "" || img.GetGeneration() != 0 || img.GetEtag() != "" || img.GetStatus() != nil ||
		img.GetCreateTime() != nil || img.GetUpdateTime() != nil || img.GetDeleteTime() != nil {
		return invalidSpec("output-only fields must be unset")
	}
	if _, err := compute.ParseLabels(img.GetLabels()); err != nil {
		return toStatus(err)
	}
	if _, err := compute.ParseDescription(img.GetSpec().GetDescription()); err != nil {
		return toStatus(err)
	}
	spec := img.GetSpec()
	sources := 0
	if spec.GetVolume() != "" {
		sources++
		if _, _, _, err := parseVolumeName(spec.GetVolume()); err != nil {
			return err
		}
	}
	if spec.GetSnapshot() != "" {
		sources++
		if _, _, _, err := parseSnapshotName(spec.GetSnapshot()); err != nil {
			return err
		}
	}
	if sources != 1 {
		return invalidSpec("image.spec requires exactly one of volume or snapshot")
	}
	return nil
}
