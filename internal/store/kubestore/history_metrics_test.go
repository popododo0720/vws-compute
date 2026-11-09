package kubestore_test

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/popododo0720/vws-compute/internal/store/kubestore"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
)

// historyMetrics is what a collector reads from the history instruments.
type historyMetrics struct {
	pruned, released int64
	sizes            metricdata.HistogramDataPoint[int64]
	bounds           []float64
}

func readHistory(t *testing.T, reader *sdkmetric.ManualReader) historyMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	only := attribute.NewSet(attribute.String("store", "kubestore"))
	var h historyMetrics
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range data.DataPoints {
					if !p.Attributes.Equals(&only) {
						t.Fatalf("%s point has attributes %v, want only store=kubestore", m.Name, p.Attributes.ToSlice())
					}
					switch m.Name {
					case "vws.store.pruned_operations":
						h.pruned += p.Value
					case "vws.store.released_indexes":
						h.released += p.Value
					}
				}
			case metricdata.Histogram[int64]:
				if m.Name != "vws.store.record_size" {
					continue
				}
				if m.Unit != "By" {
					t.Fatalf("record size unit = %q, want By", m.Unit)
				}
				if len(data.DataPoints) != 1 || !data.DataPoints[0].Attributes.Equals(&only) {
					t.Fatalf("record size points = %+v, want one store=kubestore point", data.DataPoints)
				}
				h.sizes = data.DataPoints[0]
				h.bounds = data.DataPoints[0].Bounds
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
	h := readHistory(t, reader)
	if h.pruned != 3 || h.released != 3 {
		t.Fatalf("pruned = %d, released = %d, want 3 and 3", h.pruned, h.released)
	}
	if h.sizes.Count != n {
		t.Fatalf("record size samples = %d, want one per commit (%d)", h.sizes.Count, n)
	}
	size, _ := recordOps(t, raw, ns)
	if mx, ok := h.sizes.Max.Value(); !ok || mx != int64(size) {
		t.Fatalf("largest recorded size = %d, want the stored record's %d bytes", mx, size)
	}
	if last := h.bounds[len(h.bounds)-1]; last != 1<<20 {
		t.Fatalf("top bucket bound = %v, want the 1 MiB ConfigMap limit", last)
	}

	// A replayed request writes nothing, so it records nothing.
	if _, err := s.CommitDesired(ctx, hourly(t, n)); err != nil {
		t.Fatal(err)
	}
	again := readHistory(t, reader)
	if again.pruned != h.pruned || again.released != h.released || again.sizes.Count != h.sizes.Count {
		t.Fatalf("replay changed the metrics: %+v, then %+v", h, again)
	}
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
	if h := readHistory(t, reader); h.pruned != 1 || h.released != 0 {
		t.Fatalf("pruned = %d, released = %d, want 1 and 0: Repair owns the index left behind", h.pruned, h.released)
	}
}
