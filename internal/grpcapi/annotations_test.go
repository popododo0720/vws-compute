package grpcapi_test

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// A full EC2 tag set: 50 tags of 128-character keys and 256-character
// values, 3 bytes per character. It is about 58 KB, past what 64 labels of
// 63 bytes hold.
func fullTagSet() map[string]string {
	m := map[string]string{}
	for i := range 50 {
		k := fmt.Sprintf("Name %02d ", i)
		m[k+strings.Repeat("€", 128-len(k))] = strings.Repeat("€", 256)
	}
	return m
}

// TestInstanceAnnotationsRoundTrip stores a full EC2 tag set as
// annotations, returns it verbatim on Get and List, replaces it only under
// the annotations mask path, and keeps it across a restart.
func TestInstanceAnnotationsRoundTrip(t *testing.T) {
	t.Run("memstore", func(t *testing.T) {
		repo := memstore.New()
		instanceAnnotationsRoundTrip(t, func() store.Repository { return repo })
	})
	t.Run("envtest", func(t *testing.T) {
		kc, ns := kubetest.Namespace(t)
		instanceAnnotationsRoundTrip(t, func() store.Repository { return kubetest.Open(t, kc, ns) })
	})
}

func instanceAnnotationsRoundTrip(t *testing.T, open func() store.Repository) {
	ctx := context.Background()
	st := &fakeStorage{vols: map[string]grpcapi.VolumeInfo{volBase + "root-1": {ClaimName: "pvc-root-1"}}}
	h := serveRepo(t, open(), grpcapi.WithVolumes(st))
	name := zoneParent + "/instances/web-1"
	tags := fullTagSet()

	create := createReq("web-1", "41414141-4141-4141-8141-414141414141", "root-1")
	create.Instance.Annotations = tags
	op, err := h.inst.CreateInstance(ctx, create)
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(got.GetAnnotations(), tags) || got.GetLabels()["app"] != "web" {
		t.Fatalf(
			"Get annotations = %d entries, labels %v, want the %d tags verbatim",
			len(got.GetAnnotations()),
			got.GetLabels(),
			len(tags),
		)
	}
	list, err := h.inst.ListInstances(ctx, &computev1.ListInstancesRequest{Parent: zoneParent})
	if err != nil || len(list.GetInstances()) != 1 || !maps.Equal(list.GetInstances()[0].GetAnnotations(), tags) {
		t.Fatalf("List = %v, %v, want the tags verbatim", len(list.GetInstances()), err)
	}
	if replay, err := h.inst.CreateInstance(ctx, create); err != nil || replay.GetName() != op.GetName() {
		t.Fatalf("replay = %v, %v, want operation %s", replay.GetName(), err, op.GetName())
	}

	validate := func(
		req string, mask []string, annotations map[string]string, validateOnly bool,
	) (*computev1.Instance, error) {
		t.Helper()
		_, err := h.inst.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
			RequestId:    req,
			ValidateOnly: validateOnly,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: mask},
			Instance: &computev1.Instance{
				Name:        name,
				Labels:      map[string]string{"app": "web"},
				Annotations: annotations,
			},
		})
		if err != nil {
			return nil, err
		}
		return h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
	}
	update := func(req string, mask []string, annotations map[string]string) (*computev1.Instance, error) {
		t.Helper()
		return validate(req, mask, annotations, false)
	}

	kept, err := update("42424242-4242-4242-8242-424242424242", []string{"labels"}, nil)
	if err != nil || !maps.Equal(kept.GetAnnotations(), tags) {
		t.Fatalf("labels-only update: %v, annotations %d entries, want them kept", err, len(kept.GetAnnotations()))
	}

	next := map[string]string{"Name": "web server", "cost:center": "42"}
	replaced, err := update("43434343-4343-4343-8343-434343434343", []string{"annotations"}, next)
	if err != nil || !maps.Equal(replaced.GetAnnotations(), next) {
		t.Fatalf("annotations update: %v, got %v, want %v", err, replaced.GetAnnotations(), next)
	}
	if replaced.GetGeneration() != got.GetGeneration() || replaced.GetEtag() == got.GetEtag() {
		t.Fatalf("generation %d -> %d, etag %s -> %s, want the same generation and a new etag",
			got.GetGeneration(), replaced.GetGeneration(), got.GetEtag(), replaced.GetEtag())
	}
	same, err := update("44444444-4444-4444-8444-444444444444", []string{"annotations"}, next)
	if err != nil || same.GetEtag() != replaced.GetEtag() {
		t.Fatalf("identical annotations: %v, etag %s -> %s, want a no-op", err, replaced.GetEtag(), same.GetEtag())
	}

	for name, c := range map[string]struct {
		mask        []string
		annotations map[string]string
		want        string
	}{
		"reserved key": {[]string{"annotations"}, map[string]string{"aws:x": "y"}, "reserved"},
		"control char": {
			[]string{"annotations"},
			map[string]string{"k": "a\x01"},
			`Value (a\x01) for parameter tagValue is invalid.`,
		},
		"subpath": {[]string{"annotations.Name"}, map[string]string{"Name": "x"}, "cannot be updated"},
	} {
		for _, validateOnly := range []bool{false, true} {
			_, err := validate("45454545-4545-4545-8545-454545454545", c.mask, c.annotations, validateOnly)
			wantCode(t, err, codes.InvalidArgument)
			wantReason(t, err, "INVALID_SPEC")
			if !strings.Contains(status.Convert(err).Message(), c.want) {
				t.Errorf("%s (validate_only %v): message %q, want %q", name, validateOnly, status.Convert(err).Message(), c.want)
			}
		}
	}

	h.stop()
	h = serveRepo(t, open(), grpcapi.WithVolumes(st))
	after, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
	if err != nil || !maps.Equal(after.GetAnnotations(), next) || after.GetEtag() != replaced.GetEtag() {
		t.Fatalf(
			"after restart: %v, annotations %v etag %s, want %v etag %s",
			err,
			after.GetAnnotations(),
			after.GetEtag(),
			next,
			replaced.GetEtag(),
		)
	}
}

// TestCreateInstanceRefusesBadAnnotations answers a reserved key, an
// invalid value, and too many entries with INVALID_SPEC, and stores
// nothing.
func TestCreateInstanceRefusesBadAnnotations(t *testing.T) {
	ctx := context.Background()
	st := &fakeStorage{vols: map[string]grpcapi.VolumeInfo{volBase + "root-1": {ClaimName: "pvc-root-1"}}}
	h := serveRepo(t, memstore.New(), grpcapi.WithVolumes(st))
	tooMany := fullTagSet()
	tooMany["one more"] = ""
	for i, a := range []map[string]string{
		{"aws:cloudformation:stack-name": "s"},
		{"Name": strings.Repeat("v", 257)},
		{"": "v"},
		tooMany,
	} {
		for _, validate := range []bool{true, false} {
			req := createReq("web-1", fmt.Sprintf("4646464%d-4646-4646-8646-464646464646", i), "root-1")
			req.Instance.Annotations = a
			req.ValidateOnly = validate
			_, err := h.inst.CreateInstance(ctx, req)
			wantCode(t, err, codes.InvalidArgument)
			wantReason(t, err, "INVALID_SPEC")
		}
	}
	_, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: zoneParent + "/instances/web-1"})
	wantCode(t, err, codes.NotFound)
	if st.count() != 0 {
		t.Fatalf("storage calls = %d, want none for a refused create", st.count())
	}
}
