package kubestore_test

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
)

// historyMetrics is what a collector reads from the history instruments,
// keyed by the kind label.
type historyMetrics struct {
	pruned, released map[string]int64
	sizes            map[string]metricdata.HistogramDataPoint[int64]
}

func readHistory(t *testing.T, reader *sdkmetric.ManualReader) historyMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	kindOf := func(name string, set attribute.Set) string {
		t.Helper()
		kind, _ := set.Value("kind")
		want := attribute.NewSet(attribute.String("store", "kubestore"), attribute.String("kind", kind.AsString()))
		switch kind.AsString() {
		case "instance", "image", "keypair":
			if set.Equals(&want) {
				return kind.AsString()
			}
		}
		t.Fatalf("%s point has attributes %v, want only store=kubestore and a resource kind",
			name, set.ToSlice())
		return ""
	}
	h := historyMetrics{
		pruned: map[string]int64{}, released: map[string]int64{},
		sizes: map[string]metricdata.HistogramDataPoint[int64]{},
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range data.DataPoints {
					switch m.Name {
					case "vws.store.pruned_operations":
						h.pruned[kindOf(m.Name, p.Attributes)] += p.Value
					case "vws.store.released_indexes":
						h.released[kindOf(m.Name, p.Attributes)] += p.Value
					}
				}
			case metricdata.Histogram[int64]:
				if m.Name != "vws.store.record_size" {
					continue
				}
				if m.Unit != "By" {
					t.Fatalf("record size unit = %q, want By", m.Unit)
				}
				for _, p := range data.DataPoints {
					h.sizes[kindOf(m.Name, p.Attributes)] = p
				}
			}
		}
	}
	return h
}

func TestPrunedOperationsAndRecordSizeAreCounted(t *testing.T) {
	reader := collector(t)
	raw, ns := kubetest.Namespace(t)
	ctx := context.Background()
	s := kubestore.New(raw, ns)
	const n = 103
	for g := 1; g <= n; g++ {
		if _, err := s.CommitDesired(ctx, hourly(t, g)); err != nil {
			t.Fatalf("commit generation %d: %v", g, err)
		}
	}
	if _, err := s.CommitDesired(ctx, keyPair(t)); err != nil {
		t.Fatal(err)
	}
	h := readHistory(t, reader)
	if h.pruned["instance"] != 3 || h.released["instance"] != 3 {
		t.Fatalf("instance pruned = %d, released = %d, want 3 and 3", h.pruned["instance"], h.released["instance"])
	}
	if h.pruned["keypair"] != 0 || h.released["keypair"] != 0 {
		t.Fatalf("keypair pruned = %d, released = %d, want 0 and 0", h.pruned["keypair"], h.released["keypair"])
	}
	inst := h.sizes["instance"]
	if inst.Count != n || h.sizes["keypair"].Count != 1 {
		t.Fatalf("record size samples: instance %d, keypair %d, want one per commit (%d and 1)",
			inst.Count, h.sizes["keypair"].Count, n)
	}
	size, _ := recordOps(t, raw, ns)
	if mx, ok := inst.Max.Value(); !ok || mx != int64(size) {
		t.Fatalf("largest instance record size = %d, want the stored record's %d bytes", mx, size)
	}
	if last := inst.Bounds[len(inst.Bounds)-1]; last != 1<<20 {
		t.Fatalf("top bucket bound = %v, want the 1 MiB ConfigMap limit", last)
	}

	// A replayed request writes nothing, so it records nothing.
	if _, err := s.CommitDesired(ctx, hourly(t, n)); err != nil {
		t.Fatal(err)
	}
	again := readHistory(t, reader)
	if again.pruned["instance"] != 3 || again.released["instance"] != 3 || again.sizes["instance"].Count != n {
		t.Fatalf("replay changed the metrics: %+v, then %+v", h, again)
	}
}

const deployKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"

// keyPair is the create of one key pair.
func keyPair(t *testing.T) store.DesiredChange {
	t.Helper()
	key := compute.KeyPairSpec{
		ID:         ok(compute.ParseKeyPairID("key-00000000000000001")),
		Generation: 1, CreatedAt: t0, ChangedAt: t0,
		Name:      ok(compute.ParseKeyName("deploy")),
		PublicKey: ok(compute.ParsePublicKey(deployKey)),
	}
	in := ok(compute.NewImportKeyPair("deploy", key.PublicKey.AuthorizedKey(), compute.Labels{}))
	env := compute.Env{Now: t0, Rand: rand.Reader}
	op := ok(compute.NewOperation(env, ok(compute.ParseRequestID("k-1")), in, compute.Change{Spec: key, Write: true}))
	return store.DesiredChange{Op: op, Spec: key, Write: true}
}

func TestFailedIndexReleaseIsNotCountedAsReleased(t *testing.T) {
	reader := collector(t)
	raw, ns := kubetest.Namespace(t)
	ctx := context.Background()
	s := kubestore.New(raw, ns)
	for g := 1; g <= 100; g++ {
		if _, err := s.CommitDesired(ctx, hourly(t, g)); err != nil {
			t.Fatal(err)
		}
	}
	boom := errors.New("apiserver down")
	c := hook(raw, interceptor.Funcs{
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error { return boom },
	})
	if _, err := kubestore.New(c, ns).CommitDesired(ctx, hourly(t, 101)); err != nil {
		t.Fatal(err)
	}
	if h := readHistory(t, reader); h.pruned["instance"] != 1 || h.released["instance"] != 0 {
		t.Fatalf("pruned = %d, released = %d, want 1 and 0: Repair owns the index left behind",
			h.pruned["instance"], h.released["instance"])
	}
}
