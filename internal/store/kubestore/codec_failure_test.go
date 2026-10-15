package kubestore

import (
	"bytes"
	"encoding/json"
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
