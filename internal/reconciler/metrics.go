package reconciler

import (
	"context"
	"time"

	"github.com/popododo0720/vws-lib/obs"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// queue is the controller label on the reconcile instruments and the queue
// gauge name.
const queue = "reconciler"

// Instrument wires the reconciler into the shared OTel instruments. Every
// sweep is reported and the due backlog is exposed as queue gauges. Call it
// once, after New and before Run.
func Instrument(r *Reconciler) {
	r.observe = observeSweep
	obs.M().RegisterQueue(queue, r.QueueDepth, r.QueueOldest)
}

// observeSweep records one reconcile pass. The result labels are the
// reconciler's sweep outcomes, and the code label is the compute error class.
func observeSweep(ctx context.Context, result string, d time.Duration, err error) {
	obs.M().Reconcile(ctx, queue, result, d)
	if err != nil {
		obs.M().ReconcileError(ctx, queue, compute.CodeOf(err).String())
	}
}
