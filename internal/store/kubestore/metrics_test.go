package kubestore_test

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/popododo0720/vws-lib/obs"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store/kubestore"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
)

// TestCASConflictIsCounted makes one update lose a resourceVersion race and
// checks the vws.store.conflicts point a collector would read.
func TestCASConflictIsCounted(t *testing.T) {
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

	raw, ns := kubetest.Namespace(t)
	spec := runSpec(t, 1, 1)
	seed := kubestore.New(raw, ns)
	if _, err := seed.CommitDesired(ctx, change(t, "r-1", spec)); err != nil {
		t.Fatal(err)
	}

	var once sync.Once
	update := func(ctx context.Context, c client.WithWatch, o client.Object,
		opts ...client.UpdateOption,
	) error {
		once.Do(func() {
			rival := kubestore.New(raw, ns)
			if err := rival.CommitInstanceStatus(ctx, spec.ID, compute.InstanceStatus{
				Observed: compute.Observed{Generation: 1}, Machine: compute.MachineOn, UpdatedAt: t0.Add(time.Second),
			}); err != nil {
				t.Error(err)
			}
		})
		return c.Update(ctx, o, opts...)
	}
	c := hook(raw, interceptor.Funcs{Update: update})
	s := kubestore.New(c, ns)
	if err := s.CommitInstanceStatus(ctx, spec.ID, compute.InstanceStatus{
		Observed: compute.Observed{Generation: 1}, Machine: compute.MachineOff, UpdatedAt: t0.Add(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != obs.MetricStoreConflicts {
				continue
			}
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, p := range sum.DataPoints {
					for _, a := range p.Attributes.ToSlice() {
						if string(a.Key) == obs.LabelStore && a.Value.AsString() == "kubestore" {
							found = true
						}
					}
				}
			}
		}
	}
	if !found {
		t.Error("vws.store.conflicts has no store=kubestore point")
	}
}
