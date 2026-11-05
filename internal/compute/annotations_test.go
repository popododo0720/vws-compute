package compute_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
)

func TestParseAnnotations(t *testing.T) {
	if got := ok(compute.ParseAnnotations(map[string]string{})); !got.Equal(compute.Annotations{}) || got.Map() != nil {
		t.Fatalf("empty map = %#v, want the zero set", got.Map())
	}
	euro := func(n int) string { return strings.Repeat("€", n) }
	full := map[string]string{"aws:reserved": "x"}
	for i := range compute.MaxAnnotations {
		full[fmt.Sprintf("Name %02d %s", i, euro(120))] = euro(256)
	}
	a := ok(compute.ParseAnnotations(full))
	if a.Len() != compute.MaxAnnotations+1 {
		t.Fatalf("Len = %d", a.Len())
	}
	cp := a.Map()
	cp["mutated"] = "x"
	if a.Len() != compute.MaxAnnotations+1 {
		t.Fatal("Map shares storage")
	}
	for _, m := range []map[string]string{
		{"Name": ""},
		{"kubernetes.io/cluster: x y+=@": "a value with spaces / and : symbols"},
		{euro(128): euro(256)},
	} {
		if _, err := compute.ParseAnnotations(m); err != nil {
			t.Errorf("ParseAnnotations(%q) = %v, want accepted", m, err)
		}
	}

	tooMany := map[string]string{}
	for i := range compute.MaxAnnotations + 1 {
		tooMany[fmt.Sprint("k", i)] = "v"
	}
	cases := map[string]struct {
		m    map[string]string
		want string
	}{
		"empty key":       {map[string]string{"": "v"}, "Value () for parameter tagKey is invalid."},
		"long key":        {map[string]string{euro(129): "v"}, "parameter tagKey is invalid."},
		"long value":      {map[string]string{"k": euro(257)}, "parameter tagValue is invalid."},
		"newline value":   {map[string]string{"k": "a\nb"}, `Value (a\nb) for parameter tagValue is invalid.`},
		"tab key":         {map[string]string{"a\tb": "v"}, `Value (a\tb) for parameter tagKey is invalid.`},
		"C1 control":      {map[string]string{"k": "a\u0085b"}, `Value (a\u0085b) for parameter tagValue is invalid.`},
		"invalid UTF-8":   {map[string]string{"k": "\xff"}, `Value (\xff) for parameter tagValue is invalid.`},
		"51 user entries": {tooMany, "annotations exceed 50 entries"},
	}
	for name, c := range cases {
		_, err := compute.ParseAnnotations(c.m)
		if compute.CodeOf(err) != compute.CodeInvalid || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want Invalid containing %q", name, err, c.want)
		}
	}
}

func TestCreateInstanceRefusesAReservedAnnotation(t *testing.T) {
	p := createParams()
	p.Annotations = ok(compute.ParseAnnotations(map[string]string{"aws:cloudformation:stack-name": "s"}))
	_, err := compute.NewCreateInstance(p)
	if compute.CodeOf(err) != compute.CodeInvalid || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("err = %v, want Invalid naming the reserved prefix", err)
	}
}

func TestCreateInstanceStoresAnnotations(t *testing.T) {
	p := createParams()
	p.Annotations = ok(compute.ParseAnnotations(map[string]string{"Name": "web server", "cost:center": "42"}))
	in := ok(compute.NewCreateInstance(p))
	ch, err := compute.Plan(newView(), envNow(), in)
	if err != nil {
		t.Fatal(err)
	}
	if ch.Meta == nil || !ch.Meta.Annotations.Equal(p.Annotations) || ch.Meta.Revision != 0 {
		t.Fatalf("meta = %#v, want the annotations at revision 0", ch.Meta)
	}

	stored := compute.Instance{Spec: ch.Spec.(compute.InstanceSpec), Meta: *ch.Meta}
	again, err := compute.Plan(newView().with(stored), envNow(), in)
	if err != nil || again.Write {
		t.Fatalf("replay = write %v err %v, want the stored instance", again.Write, err)
	}
	other := createParams()
	other.Annotations = ok(compute.ParseAnnotations(map[string]string{"Name": "db"}))
	if _, err := compute.Plan(newView().with(stored), envNow(), ok(compute.NewCreateInstance(other))); compute.CodeOf(
		err,
	) != compute.CodeAlreadyExists {
		t.Fatalf("different annotations under the same id: %v, want AlreadyExists", err)
	}
	if compute.FingerprintOf(in) == compute.FingerprintOf(ok(compute.NewCreateInstance(other))) {
		t.Fatal("annotations do not reach the fingerprint")
	}
}

// The golden value is the fingerprint of createParams() on the release
// before annotations, so a stored CreateInstance request id still replays.
func TestCreateFingerprintWithoutAnnotationsIsUnchanged(t *testing.T) {
	const golden = "b8e58edcf9bbab31ffde38cbfe578fba312917f1d41b3d1cf4193f2f2898f2f1"
	if got := fmt.Sprintf("%x", compute.FingerprintOf(ok(compute.NewCreateInstance(createParams())))); got != golden {
		t.Fatalf("fingerprint = %s, want %s", got, golden)
	}
}

func TestReplaceAnnotationsKeepsReservedKeys(t *testing.T) {
	stored := runningInstance()
	stored.Meta.Annotations = ok(compute.ParseAnnotations(map[string]string{"aws:origin": "server", "Name": "old"}))
	plan := func(m map[string]string) (compute.Change, error) {
		patch := compute.MetadataPatch{}.ReplaceAnnotations(ok(compute.ParseAnnotations(m)))
		return compute.Plan(newView().with(stored), envNow(), ok(compute.NewUpdateInstanceMetadata(instanceID, patch)))
	}

	ch, err := plan(map[string]string{"Name": "new"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"aws:origin": "server", "Name": "new"}
	if ch.Write || ch.Meta == nil || ch.Meta.Revision != 1 ||
		!ch.Meta.Annotations.Equal(ok(compute.ParseAnnotations(want))) {
		t.Fatalf("change = write %v meta %#v, want %v at revision 1", ch.Write, ch.Meta, want)
	}

	if ch, err := plan(map[string]string{"aws:origin": "server", "Name": "old"}); err != nil || ch.Meta != nil {
		t.Fatalf("resending the stored set = meta %#v err %v, want a no-op", ch.Meta, err)
	}
	if ch, err := plan(map[string]string{"Name": "old"}); err != nil || ch.Meta != nil {
		t.Fatalf("omitting a reserved key = meta %#v err %v, want a no-op", ch.Meta, err)
	}
	if ch, err := plan(nil); err != nil ||
		!ch.Meta.Annotations.Equal(ok(compute.ParseAnnotations(map[string]string{"aws:origin": "server"}))) {
		t.Fatalf("clearing = meta %#v err %v, want only the reserved key", ch.Meta, err)
	}
	for _, m := range []map[string]string{
		{"aws:origin": "changed"},
		{"aws:new": "x"},
	} {
		if _, err := plan(m); compute.CodeOf(err) != compute.CodeInvalid {
			t.Errorf("plan(%v) = %v, want Invalid", m, err)
		}
	}
}

func TestAnnotationPatchFingerprint(t *testing.T) {
	a := ok(compute.ParseAnnotations(map[string]string{"Name": "web", "Env": "prod"}))
	b := ok(compute.ParseAnnotations(map[string]string{"Env": "prod", "Name": "web"}))
	if fpUpdate(
		t,
		compute.MetadataPatch{}.ReplaceAnnotations(a),
	) != fpUpdate(
		t,
		compute.MetadataPatch{}.ReplaceAnnotations(b),
	) {
		t.Fatal("map order reached the fingerprint")
	}
	if fpUpdate(
		t,
		compute.MetadataPatch{}.ReplaceAnnotations(compute.Annotations{}),
	) == fpUpdate(
		t,
		compute.MetadataPatch{},
	) {
		t.Fatal("clearing annotations fingerprints as the zero patch")
	}
	if (compute.MetadataPatch{}).ReplaceAnnotations(compute.Annotations{}).IsZero() {
		t.Fatal("an annotation replace is the zero patch")
	}
}
