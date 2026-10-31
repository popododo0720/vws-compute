package kubestore

import (
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
)

const (
	// keepOps is how many of a resource's newest operations are always kept.
	keepOps = 100
	// opRetention is how long a settled operation, and so a replay of its
	// request id, is kept at least. EC2 keeps a client token for at least 24
	// hours after the request completes.
	opRetention = 24 * time.Hour
	// maxConfigMapBytes is the Kubernetes limit on a ConfigMap's data.
	maxConfigMapBytes = 1 << 20
)

// prune drops the settled operations of e that are not among its newest
// keepOps and settled before now minus opRetention, and returns them. A
// Pending operation is never dropped. Operations are appended in commit
// order, so the newest are last. prune runs inside the CAS update that
// writes e, so a crash leaves either every dropped operation or none.
func (e *entry) prune(now time.Time) []compute.Operation {
	excess := len(e.ops) - keepOps
	if excess <= 0 {
		return nil
	}
	cutoff := now.Add(-opRetention)
	var dropped []compute.Operation
	kept := make([]compute.Operation, 0, len(e.ops))
	for i, op := range e.ops {
		if i < excess && op.State().Terminal() && op.SettledAt().Before(cutoff) {
			dropped = append(dropped, op)
			continue
		}
		kept = append(kept, op)
	}
	e.ops = kept
	return dropped
}
