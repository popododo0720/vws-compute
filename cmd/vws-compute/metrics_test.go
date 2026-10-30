package main

import (
	"context"
	"io"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/popododo0720/vws-lib/obs"
)

// TestRunRecordsReconcileAndQueueMetrics runs the binary's wiring and reads
// the instruments it registers. Removing the observer or the queue gauge from
// run therefore fails here, not only in the reconciler test.
func TestRunRecordsReconcileAndQueueMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	old := setupObs
	setupObs = func(ctx context.Context, opts ...obs.Option) (func(context.Context) error, error) {
		return obs.Setup(ctx, append(opts, obs.WithOverride(func(c *obs.Config) {
			c.MetricReaders = []sdkmetric.Reader{reader}
			c.LogOutput = io.Discard
		}))...)
	}
	t.Cleanup(func() { setupObs = old })

	addr := freeAddr(t)
	cfg, err := parseConfig([]string{"-listen", addr}, func(string) string { return "" }, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	eventually(t, "reconcile and queue metrics", func() bool {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(ctx, &rm); err != nil {
			return false
		}
		return hasPoint(rm, obs.MetricReconcileTotal, map[string]string{
			"controller": "reconciler", "result": "ok",
		}) && hasPoint(rm, obs.MetricQueueDepth, map[string]string{
			"queue": "reconciler",
		})
	})
}

func hasPoint(rm metricdata.ResourceMetrics, name string, want map[string]string) bool {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			for _, labels := range pointLabels(m.Data) {
				matched := true
				for k, v := range want {
					if labels[k] != v {
						matched = false
						break
					}
				}
				if matched {
					return true
				}
			}
		}
	}
	return false
}

func pointLabels(data metricdata.Aggregation) []map[string]string {
	var out []map[string]string
	switch d := data.(type) {
	case metricdata.Sum[int64]:
		for _, p := range d.DataPoints {
			out = append(out, labelMap(p.Attributes))
		}
	case metricdata.Gauge[int64]:
		for _, p := range d.DataPoints {
			out = append(out, labelMap(p.Attributes))
		}
	}
	return out
}

func labelMap(set attribute.Set) map[string]string {
	out := map[string]string{}
	for _, a := range set.ToSlice() {
		out[string(a.Key)] = a.Value.AsString()
	}
	return out
}
