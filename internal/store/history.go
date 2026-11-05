package store

import (
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
)

const (
	// KeepOperations is how many of a resource's newest operations are
	// always kept.
	KeepOperations = 100
	// OperationRetention is how long a settled operation, and so a replay of
	// its request id, is kept at least. EC2 keeps a client token for at least
	// 24 hours after the request completes.
	OperationRetention = 24 * time.Hour
)

// PruneOperations splits one resource's operations, oldest first, into the
// ones it keeps and the ones it drops. It drops a settled operation that is
// not among the newest KeepOperations and settled before now minus
// OperationRetention. A Pending operation is never dropped. Every repository
// applies it in the commit that adds an operation, so a request id is
// forgotten at the same point in each.
func PruneOperations(ops []compute.Operation, now time.Time) (kept, dropped []compute.Operation) {
	excess := len(ops) - KeepOperations
	if excess <= 0 {
		return ops, nil
	}
	cutoff := now.Add(-OperationRetention)
	kept = make([]compute.Operation, 0, len(ops))
	for i, op := range ops {
		if i < excess && op.State().Terminal() && op.SettledAt().Before(cutoff) {
			dropped = append(dropped, op)
			continue
		}
		kept = append(kept, op)
	}
	return kept, dropped
}
