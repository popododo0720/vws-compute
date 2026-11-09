package kubestore

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// TestInstanceRecordKeepsAttachTimes pins vws-compute#166 AC3: the spec and
// the reported attach times survive a record round trip, and a record written
// before the field existed decodes with both unset and gains no key.
func TestInstanceRecordKeepsAttachTimes(t *testing.T) {
	ref := compute.Ref{Kind: compute.KindInstance, ID: "web-1"}
	specAt := time.Date(2026, 11, 9, 1, 2, 3, 4, time.UTC)
	reportAt := specAt.Add(-time.Hour)
	const root = "projects/p/zones/z/volumes/root"
	in := instanceJSON{
		ID: "web-1", Generation: 1, Type: "vws.small", Power: uint8(compute.PowerRunning), Project: "p", Zone: "z",
		Volumes: []volumeJSON{{Volume: root, Device: "vda", Boot: true, AttachTime: specAt.UnixNano()}},
		Report: reportJSON{Volumes: []observedVolumeJSON{{
			Volume: root, Device: "vda", State: uint8(compute.AttachAttached), AttachTime: reportAt.UnixNano(),
		}}},
	}
	got := decodeRecord(t, ref, recordJSON{Instance: &in})
	if a := got.inst.Spec.Volumes[0].AttachTime; !a.Equal(specAt) {
		t.Fatalf("spec attach time = %v, want %v", a, specAt)
	}
	if a := got.inst.Status.Report.Volumes[0].AttachTime; !a.Equal(reportAt) {
		t.Fatalf("reported attach time = %v, want %v", a, reportAt)
	}
	again := instanceTo(got.inst)
	if again.Volumes[0].AttachTime != specAt.UnixNano() || again.Report.Volumes[0].AttachTime != reportAt.UnixNano() {
		t.Fatalf("re-encoded attach times = %d, %d", again.Volumes[0].AttachTime, again.Report.Volumes[0].AttachTime)
	}

	legacy := `{"instance":{"id":"web-1","generation":1,"created_at":0,"changed_at":0,"type":"vws.small","power":1,` +
		`"project":"p","zone":"z","volumes":[{"volume":"` + root + `","device":"vda","boot":true}],` +
		`"observed":{},"retry":{},"report":{"volumes":[{"volume":"` + root + `","device":"vda","state":2}]},"meta":{}}}`
	var rec recordJSON
	if err := json.Unmarshal([]byte(legacy), &rec); err != nil {
		t.Fatal(err)
	}
	old := decodeRecord(t, ref, rec)
	if !old.inst.Spec.Volumes[0].AttachTime.IsZero() || !old.inst.Status.Report.Volumes[0].AttachTime.IsZero() {
		t.Fatalf("legacy record attach times = %+v, %+v", old.inst.Spec.Volumes, old.inst.Status.Report.Volumes)
	}
	b, err := json.Marshal(instanceTo(old.inst))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "attach_time") {
		t.Fatalf("legacy record gained attach_time on re-encode: %s", b)
	}
}
