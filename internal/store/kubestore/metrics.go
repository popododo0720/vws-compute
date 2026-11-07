package kubestore

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/popododo0720/vws-lib/obs"
)

// Names of the operation history and record size metrics.
const (
	metricPrunedOperations = "vws.store.pruned_operations"
	metricReleasedIndexes  = "vws.store.released_indexes"
	metricRecordSize       = "vws.store.record_size"
)

const storeLabel = "kubestore"

// recordSizeBuckets end at the ConfigMap limit, so a record nearing it lands
// in a bucket of its own. The SDK defaults stop at 10000 and would put every
// record over 10 kB in +Inf.
var recordSizeBuckets = []float64{
	1 << 10, 4 << 10, 16 << 10, 64 << 10, 128 << 10, 256 << 10,
	512 << 10, 768 << 10, 896 << 10, 960 << 10, maxConfigMapBytes,
}

type meters struct {
	pruned   metric.Int64Counter
	released metric.Int64Counter
	size     metric.Int64Histogram
}

// newMeters binds the instruments to the meter provider installed when the
// store is made, so obs.Setup must run first.
func newMeters() meters {
	m := otel.GetMeterProvider().Meter("github.com/popododo0720/vws-compute/internal/store/kubestore")
	pruned, err := m.Int64Counter(metricPrunedOperations, metric.WithUnit("{operation}"),
		metric.WithDescription("Settled operations dropped from a resource record by a commit."))
	if err != nil {
		otel.Handle(err)
		pruned = noop.Int64Counter{}
	}
	released, err := m.Int64Counter(metricReleasedIndexes, metric.WithUnit("{index}"),
		metric.WithDescription("Request-id indexes deleted after their operation was pruned."))
	if err != nil {
		otel.Handle(err)
		released = noop.Int64Counter{}
	}
	size, err := m.Int64Histogram(metricRecordSize, metric.WithUnit("By"),
		metric.WithDescription("Encoded size of a resource record after a write; the ConfigMap limit is 1 MiB."),
		metric.WithExplicitBucketBoundaries(recordSizeBuckets...))
	if err != nil {
		otel.Handle(err)
		size = noop.Int64Histogram{}
	}
	return meters{pruned: pruned, released: released, size: size}
}

func (m meters) prunedOperations(ctx context.Context, n int) {
	if n > 0 {
		m.pruned.Add(ctx, int64(n), obs.Attrs(obs.LabelStore, storeLabel))
	}
}

func (m meters) releasedIndex(ctx context.Context) {
	m.released.Add(ctx, 1, obs.Attrs(obs.LabelStore, storeLabel))
}

func (m meters) recordSize(ctx context.Context, n int) {
	m.size.Record(ctx, int64(n), obs.Attrs(obs.LabelStore, storeLabel))
}
