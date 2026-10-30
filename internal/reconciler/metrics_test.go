package reconciler_test

import (
	"context"
	"io"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/popododo0720/vws-lib/obs"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
)

// TestReconcileMetricsOnEnvtest drives one real sweep through the ConfigMap
// store and checks the OTel instruments a collector would read.
func TestReconcileMetricsOnEnvtest(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	shutdown, err := obs.Setup(ctx, obs.ForService("compute"), obs.WithOverride(func(c *obs.Config) {
		c.MetricReaders = []sdkmetric.Reader{reader}
		c.LogOutput = io.Discard
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	clk := &clock{t: t0}
	repo := kubetest.Store(t)
	observed := make(chan error, 4)
	rec := reconciler.New(repo, fakeagent.New(),
		reconciler.WithClock(clk.Now),
		reconciler.WithInterval(time.Hour),
		reconciler.WithSweepObserver(func(ctx context.Context, result string, d time.Duration, err error) {
			obs.M().Reconcile(ctx, "reconciler", result, d)
			if err != nil {
				obs.M().ReconcileError(ctx, "reconciler", compute.CodeOf(err).String())
			}
			observed <- err
		}))
	obs.M().RegisterQueue("reconciler", rec.QueueDepth, rec.QueueOldest)
	ctl := controller.New(repo, controller.WithClock(clk.Now), controller.WithKick(rec.Kick))
	rc, err := ctl.Submit(ctx, ok(compute.ParseRequestID("img-1")),
		ok(compute.NewRegisterImage("debian-13", "https://images.example/debian-13.qcow2")))
	if err != nil {
		t.Fatal(err)
	}
	if rc.Operation.ID.String() == "" {
		t.Fatal("image submit returned no operation")
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- rec.Run(runCtx) }()
	select {
	case err := <-observed:
		if err != nil {
			t.Fatalf("sweep: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no sweep observed")
	}
	cancel()
	<-done

	if depth := rec.QueueDepth(); depth != 1 {
		t.Fatalf("queue depth at the sweep = %d, want 1", depth)
	}
	clk.Advance(time.Second)
	if age := rec.QueueOldest(); age != time.Second {
		t.Fatalf("queue oldest age = %s, want 1s", age)
	}
	if err := rec.Sweep(ctx); err != nil {
		t.Fatalf("empty sweep: %v", err)
	}
	if depth := rec.QueueDepth(); depth != 0 {
		t.Fatalf("queue depth after the empty sweep = %d, want 0", depth)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		obs.MetricReconcileTotal, obs.MetricReconcileDuration, obs.MetricQueueDepth, obs.MetricQueueOldestAge,
	} {
		if _, ok := metricByName(rm, name); !ok {
			t.Errorf("no %s metric", name)
		}
	}
	if !hasPoint(rm, obs.MetricReconcileTotal, "controller", "reconciler", "result", "ok") {
		t.Error("vws.reconcile.total has no controller=reconciler result=ok point")
	}
	assertNoForbiddenLabels(t, rm)
}

func metricByName(rm metricdata.ResourceMetrics, name string) (metricdata.Metrics, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

func hasPoint(rm metricdata.ResourceMetrics, name, k1, v1, k2, v2 string) bool {
	m, ok := metricByName(rm, name)
	if !ok {
		return false
	}
	for _, labels := range dataPointLabels(m.Data) {
		if labels[k1] == v1 && labels[k2] == v2 {
			return true
		}
	}
	return false
}

func dataPointLabels(data metricdata.Aggregation) []map[string]string {
	var out []map[string]string
	switch d := data.(type) {
	case metricdata.Sum[int64]:
		for _, p := range d.DataPoints {
			out = append(out, labels(p.Attributes))
		}
	case metricdata.Sum[float64]:
		for _, p := range d.DataPoints {
			out = append(out, labels(p.Attributes))
		}
	case metricdata.Gauge[int64]:
		for _, p := range d.DataPoints {
			out = append(out, labels(p.Attributes))
		}
	case metricdata.Gauge[float64]:
		for _, p := range d.DataPoints {
			out = append(out, labels(p.Attributes))
		}
	case metricdata.Histogram[int64]:
		for _, p := range d.DataPoints {
			out = append(out, labels(p.Attributes))
		}
	case metricdata.Histogram[float64]:
		for _, p := range d.DataPoints {
			out = append(out, labels(p.Attributes))
		}
	}
	return out
}

func labels(set attribute.Set) map[string]string {
	out := map[string]string{}
	for _, a := range set.ToSlice() {
		out[string(a.Key)] = a.Value.AsString()
	}
	return out
}

// forbiddenLabels are values whose cardinality grows with traffic or data.
// The lib Attrs allowlist already rejects them; this checks the collected
// instruments too, so a service-defined instrument cannot slip one in.
var forbiddenLabels = map[string]bool{
	"resource_id": true,
	"request_id":  true,
	"trace_id":    true,
	"span_id":     true,
	"instance":    true,
	"image":       true,
}

func assertNoForbiddenLabels(t *testing.T, rm metricdata.ResourceMetrics) {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			for _, labels := range dataPointLabels(m.Data) {
				for key := range labels {
					if forbiddenLabels[key] {
						t.Errorf("%s has forbidden label %q", m.Name, key)
					}
				}
			}
		}
	}
}
