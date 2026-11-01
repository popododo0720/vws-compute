package kubestore

import (
	"bytes"
	"encoding/json"
	"slices"
	"strconv"
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
)

func TestFailureReasonRoundTripsAndRejectsAnUnknownByte(t *testing.T) {
	raw, err := json.Marshal(failureTo(&compute.Failure{
		Code: compute.CodeFailedPrecondition, Message: "image missing", Reason: compute.LaunchImageNotFound,
	}))
	if err != nil {
		t.Fatal(err)
	}
	var stored failureJSON
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	got, err := failureFrom(&stored)
	if err != nil || got.Reason != compute.LaunchImageNotFound || got.Message != "image missing" {
		t.Fatalf("round trip = %+v %v", got, err)
	}

	raw, err = json.Marshal(failureTo(&compute.Failure{Code: compute.CodeInternal, Message: "gave up"}))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(`"reason"`)) {
		t.Fatalf("zero reason was stored: %s", raw)
	}
	stored = failureJSON{}
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	got, err = failureFrom(&stored)
	if err != nil || got.Reason != compute.LaunchInternal {
		t.Fatalf("legacy failure = %+v %v", got, err)
	}

	if _, err := failureFrom(&failureJSON{Code: uint8(compute.CodeInternal), Reason: 9}); err == nil {
		t.Fatal("reason 9 was accepted")
	}
}

// TestObservedRoundTripsAnAttachmentFailure: an attachment failure persists
// beside the instance failure, and a record written before the field existed
// decodes with no attachment failure.
func TestObservedRoundTripsAnAttachmentFailure(t *testing.T) {
	volume, err := compute.ParseVolumeRef("projects/p/zones/z/volumes/data")
	if err != nil {
		t.Fatal(err)
	}
	other, err := compute.ParseVolumeRef("projects/p/zones/z/volumes/data2")
	if err != nil {
		t.Fatal(err)
	}
	refused := &compute.AttachmentFailure{
		Failure: compute.Failure{Code: compute.CodeFailedPrecondition, Message: "not Block"},
		Volumes: []compute.VolumeRef{volume, other},
	}
	raw, err := json.Marshal(observedTo(compute.Observed{Generation: 3, AttachmentFailure: refused}))
	if err != nil {
		t.Fatal(err)
	}
	var stored observedJSON
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	got, err := observedFrom(stored)
	if err != nil || got.Generation != 3 || got.AttachmentFailure == nil ||
		got.AttachmentFailure.Failure != refused.Failure ||
		!slices.Equal(got.AttachmentFailure.Volumes, refused.Volumes) {
		t.Fatalf("round trip = %+v %v", got, err)
	}

	// A row written before Volumes existed names its one refused volume
	// under "volume".
	stored = observedJSON{}
	legacy := `{"generation":3,"attachment_failure":{"code":` + strconv.Itoa(int(compute.CodeFailedPrecondition)) +
		`,"message":"not Block","volume":"projects/p/zones/z/volumes/data"}}`
	if err := json.Unmarshal([]byte(legacy), &stored); err != nil {
		t.Fatal(err)
	}
	got, err = observedFrom(stored)
	if err != nil || got.AttachmentFailure == nil ||
		!slices.Equal(got.AttachmentFailure.Volumes, []compute.VolumeRef{volume}) {
		t.Fatalf("legacy row = %+v %v", got, err)
	}

	stored = observedJSON{}
	if err := json.Unmarshal([]byte(`{"generation":2}`), &stored); err != nil {
		t.Fatal(err)
	}
	got, err = observedFrom(stored)
	if err != nil || got.Generation != 2 || got.Failure != nil || got.AttachmentFailure != nil {
		t.Fatalf("old record = %+v %v", got, err)
	}
}
