package compute_test

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
)

func TestParseLabels(t *testing.T) {
	okLabels := func(m map[string]string) compute.Labels {
		t.Helper()
		l, err := compute.ParseLabels(m)
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	if got := okLabels(nil); got.Len() != 0 || got.Map() != nil {
		t.Fatalf("nil labels = %#v", got.Map())
	}
	if got := okLabels(map[string]string{}); !got.Equal(compute.Labels{}) {
		t.Fatal("empty map is not the zero Labels")
	}
	l := okLabels(map[string]string{"vws-fingerprint": "abc", "team": "infra"})
	if v, ok := l.Get("vws-fingerprint"); !ok || v != "abc" {
		t.Fatalf("vws-fingerprint = %q, %v", v, ok)
	}
	cloned := l.Map()
	cloned["team"] = "other"
	if v, _ := l.Get("team"); v != "infra" {
		t.Fatalf("Map shares storage: team = %q", v)
	}
	cases := []map[string]string{
		{"A": "x"},
		{"1abc": "x"},
		{"has space": "x"},
		{strings.Repeat("a", 64): "x"},
		{"ok": strings.Repeat("b", 64)},
		{"ok": "bad\n"},
		{"ok": "bad\x00"},
	}
	tooMany := map[string]string{}
	for i := 0; i < compute.MaxLabels+1; i++ {
		tooMany[string(rune('a'+i%26))+strings.Repeat("a", i/26)] = "v"
	}
	// The generated keys above can collide. Build 65 distinct keys explicitly.
	tooMany = map[string]string{}
	for i := 0; i < compute.MaxLabels+1; i++ {
		tooMany["k"+strings.Repeat("a", i)] = "v"
	}
	cases = append(cases, tooMany)
	for _, m := range cases {
		if _, err := compute.ParseLabels(m); compute.CodeOf(err) != compute.CodeInvalid {
			t.Errorf("ParseLabels(%v) err = %v, want Invalid", m, err)
		}
	}
}

func TestParseDescription(t *testing.T) {
	d, err := compute.ParseDescription("line\n\tmore")
	if err != nil || d.String() != "line\n\tmore" {
		t.Fatalf("description = %q, %v", d.String(), err)
	}
	if _, err := compute.ParseDescription("bad\x00"); compute.CodeOf(err) != compute.CodeInvalid {
		t.Fatalf("control: %v", err)
	}
	if _, err := compute.ParseDescription(strings.Repeat("a", compute.MaxDescriptionBytes+1)); compute.CodeOf(err) != compute.CodeInvalid {
		t.Fatalf("length: %v", err)
	}
}

func TestFingerprintMetadataIsCanonical(t *testing.T) {
	stop := compute.FingerprintOf(ok(compute.NewStopInstance(instanceID)))
	const wantStop = "bc315f5ae58c5fd47a5370a0049c4763fbbf851dd0cde6c6fa354804591e85e7"
	if got := hex.EncodeToString(stop[:]); got != wantStop {
		t.Fatalf("stop fingerprint = %s, want %s", got, wantStop)
	}
	same := compute.FingerprintOf(ok(compute.NewSetPower(instanceID, compute.PowerStopped, compute.MetadataPatch{})))
	if stop != same {
		t.Fatal("zero metadata patch changes the StopInstance fingerprint")
	}
	bare := compute.FingerprintOf(ok(compute.NewImportKeyPair("k", ed25519Bare, compute.Labels{})))
	again := compute.FingerprintOf(ok(compute.NewImportKeyPair("k", ed25519Bare, compute.Labels{})))
	if bare != again {
		t.Fatal("empty labels are not stable")
	}
	labeled := okLabels(t, map[string]string{"b": "2", "a": "1"})
	flipped := okLabels(t, map[string]string{"a": "1", "b": "2"})
	one := compute.FingerprintOf(ok(compute.NewImportKeyPair("k", ed25519Bare, labeled)))
	two := compute.FingerprintOf(ok(compute.NewImportKeyPair("k", ed25519Bare, flipped)))
	if one != two {
		t.Fatal("label order changes the fingerprint")
	}
	if one == bare {
		t.Fatal("labels do not change the import fingerprint")
	}
	clear := compute.MetadataPatch{}.ReplaceLabels(compute.Labels{})
	keep := compute.MetadataPatch{}
	desc := compute.MetadataPatch{}.SetDescription(ok(compute.ParseDescription("")))
	edit, err := compute.MetadataPatch{}.EditLabels(compute.Labels{}, []string{"a"})
	if err != nil {
		t.Fatal(err)
	}
	setEmpty, err := compute.MetadataPatch{}.EditLabels(okLabels(t, map[string]string{"a": ""}), nil)
	if err != nil {
		t.Fatal(err)
	}
	fps := []compute.Fingerprint{
		fpUpdate(t, keep),
		fpUpdate(t, clear),
		fpUpdate(t, desc),
		fpUpdate(t, edit),
		fpUpdate(t, setEmpty),
	}
	seen := map[compute.Fingerprint]int{}
	for i, fp := range fps {
		if prev, ok := seen[fp]; ok {
			t.Fatalf("patch %d hashes like patch %d", i, prev)
		}
		seen[fp] = i
	}
}

func TestPlanMetadataDoesNotBumpGeneration(t *testing.T) {
	labels := okLabels(t, map[string]string{"vws-fingerprint": "abc"})
	patch := compute.MetadataPatch{}.ReplaceLabels(labels)
	inst := runningInstance()
	ch, err := compute.Plan(newView().with(inst), envNow(), ok(compute.NewUpdateInstanceMetadata(instanceID, patch)))
	if err != nil {
		t.Fatal(err)
	}
	if ch.Write || ch.Spec.Gen() != inst.Spec.Generation || ch.Meta == nil || ch.Meta.Revision != 1 {
		t.Fatalf("change = write %v gen %d meta %#v", ch.Write, ch.Spec.Gen(), ch.Meta)
	}
	if !ch.Meta.Labels.Equal(labels) {
		t.Fatalf("labels = %#v", ch.Meta.Labels.Map())
	}
	op := ok(compute.NewOperation(envNow(), mustRequest(t, "meta"), ok(compute.NewUpdateInstanceMetadata(instanceID, patch)), ch))
	if !op.MetadataOnly {
		t.Fatal("metadata update is not MetadataOnly")
	}
	born := compute.Settle(op, ch.Spec.Gen(), compute.Observed{Generation: 0}, envNow().Now)
	if born.State() != compute.OperationSucceeded {
		t.Fatalf("born %s, want succeeded ahead of observation", born.State())
	}
	if _, err := compute.RestoreOperation(op, compute.OperationPending, nil, time.Time{}); err == nil {
		t.Fatal("a pending metadata-only record was restored")
	}

	img := availableImage()
	desc := compute.MetadataPatch{}.SetDescription(ok(compute.ParseDescription("debian")))
	ich, err := compute.Plan(newView().with(img), envNow(), ok(compute.NewUpdateImageMetadata(imageID, desc)))
	if err != nil {
		t.Fatal(err)
	}
	if ich.Write || ich.Spec.Gen() != 1 || ich.Meta == nil || ich.Meta.Description.String() != "debian" {
		t.Fatalf("image change = %+v meta %#v", ich.Write, ich.Meta)
	}
	merged := compute.Image{Spec: ich.Spec.(compute.ImageSpec), Status: img.Status, Meta: *ich.Meta}
	if !merged.Available() {
		t.Fatal("a description edit made the image unavailable")
	}

	same, err := compute.Plan(newView().with(compute.Instance{Spec: inst.Spec, Meta: *ch.Meta}), envNow(), ok(compute.NewUpdateInstanceMetadata(instanceID, patch)))
	if err != nil {
		t.Fatal(err)
	}
	if same.Meta != nil || same.Write {
		t.Fatalf("identical labels wrote %#v", same.Meta)
	}

	_, err = compute.Plan(newView().with(compute.Instance{Spec: compute.InstanceSpec{ID: inst.Spec.ID, Generation: 1, Power: compute.PowerTerminated}}), envNow(), ok(compute.NewUpdateInstanceMetadata(instanceID, patch)))
	if compute.CodeOf(err) != compute.CodeFailedPrecondition {
		t.Fatalf("terminated: %v", err)
	}
}

func TestSetPowerKeepsLabelsInTheSameCommit(t *testing.T) {
	inst := runningInstance()
	labels := okLabels(t, map[string]string{"team": "infra"})
	in := ok(compute.NewSetPower(instanceID, compute.PowerStopped, compute.MetadataPatch{}.ReplaceLabels(labels)))
	ch, err := compute.Plan(newView().with(inst), envNow(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !ch.Write || ch.Spec.Gen() != 2 || ch.Meta == nil || !ch.Meta.Labels.Equal(labels) {
		t.Fatalf("change write=%v gen=%d meta=%#v", ch.Write, ch.Spec.Gen(), ch.Meta)
	}
	op := ok(compute.NewOperation(envNow(), mustRequest(t, "pow"), in, ch))
	if op.MetadataOnly || op.Kind != compute.OpStopInstance {
		t.Fatalf("op metadata=%v kind=%s", op.MetadataOnly, op.Kind)
	}
}

func okLabels(t *testing.T, m map[string]string) compute.Labels {
	t.Helper()
	l, err := compute.ParseLabels(m)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func fpUpdate(t *testing.T, p compute.MetadataPatch) compute.Fingerprint {
	t.Helper()
	return compute.FingerprintOf(ok(compute.NewUpdateInstanceMetadata(instanceID, p)))
}

func envNow() compute.Env {
	return compute.Env{Now: t0, Rand: rand.Reader}
}
