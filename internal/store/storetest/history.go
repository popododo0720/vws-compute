package storetest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

const historyLen = store.KeepOperations + 30

// generations commits generations 1 to historyLen of instance n, generation g
// at t0 plus g steps, each with request id h<n>-<g>. Each one supersedes the
// one before it.
func (f fixture) generations(n int, step time.Duration) []compute.Operation {
	f.t.Helper()
	ops := make([]compute.Operation, historyLen+1)
	for g := 1; g <= historyLen; g++ {
		ops[g] = f.mustCommitAt(t0.Add(time.Duration(g)*step), fmt.Sprintf("h%d-%d", n, g), runIntent("vws.small"),
			instanceSpec(n, int64(g), compute.PowerRunning), true)
	}
	return ops
}

func (f fixture) mustCommitAt(
	now time.Time,
	req string,
	in compute.Intent,
	spec compute.Spec,
	write bool,
) compute.Operation {
	f.t.Helper()
	op, err := f.commitAt(now, req, in, spec, write)
	if err != nil {
		f.t.Fatalf("commit %s: %v", req, err)
	}
	return op
}

// wantHeld fails unless op is still answered by its id and its request id.
func (f fixture) wantHeld(op compute.Operation) {
	f.t.Helper()
	ctx := context.Background()
	if got, err := f.s.Operation(ctx, op.ID); err != nil || got.ID != op.ID {
		f.t.Fatalf("operation %s = %v, want it held", op.ID, err)
	}
	if got, err := f.s.OperationByRequest(ctx, op.Request); err != nil || got.ID != op.ID {
		f.t.Fatalf("request %s = %v, %v; want %s", op.Request, got.ID, err, op.ID)
	}
}

// wantPruned fails unless op and its request id are forgotten.
func (f fixture) wantPruned(op compute.Operation) {
	f.t.Helper()
	ctx := context.Background()
	_, err := f.s.Operation(ctx, op.ID)
	wantCode(f.t, err, compute.CodeNotFound)
	_, err = f.s.OperationByRequest(ctx, op.Request)
	wantCode(f.t, err, compute.CodeNotFound)
}

// historyIsBounded commits generations an hour apart, so every superseded
// operation outside the newest KeepOperations settled more than
// OperationRetention ago and is dropped with its request id.
func historyIsBounded(f fixture) {
	ops := f.generations(1, time.Hour)
	for g := 1; g <= historyLen-store.KeepOperations; g++ {
		f.wantPruned(ops[g])
	}
	for g := historyLen - store.KeepOperations + 1; g <= historyLen; g++ {
		f.wantHeld(ops[g])
	}
	f.wantState(ops[historyLen], compute.OperationPending)
	// A replay of a held request id answers with its own operation and
	// drops nothing.
	oldest := historyLen - store.KeepOperations + 1
	at := t0.Add(time.Duration(historyLen+1) * time.Hour)
	for _, g := range []int{oldest, historyLen} {
		spec := instanceSpec(1, int64(g), compute.PowerRunning)
		again := f.mustCommitAt(at, fmt.Sprintf("h1-%d", g), runIntent("vws.small"), spec, true)
		if again.ID != ops[g].ID {
			f.t.Fatalf("replay of h1-%d = %s, want %s", g, again.ID, ops[g].ID)
		}
	}
	f.wantHeld(ops[oldest])
	// A pruned request id is forgotten, so its retry is a new request. Its
	// plan for generation 1 is stale and leaves no operation behind.
	_, err := f.commitAt(at, "h1-1", runIntent("vws.small"), instanceSpec(1, 1, compute.PowerRunning), true)
	if !errors.Is(err, store.ErrConflict) {
		f.t.Fatalf("retry of pruned h1-1 = %v, want ErrConflict", err)
	}
	f.wantPruned(ops[1])
}

// recentOperationsKept commits generations a minute apart: past
// KeepOperations, but all settled within OperationRetention, so none is
// dropped and every request id still replays.
func recentOperationsKept(f fixture) {
	ops := f.generations(1, time.Minute)
	for g := 1; g <= historyLen; g++ {
		f.wantHeld(ops[g])
	}
}

// pendingNeverPruned keeps one operation Pending, however old, while more
// than KeepOperations newer ones target the same generation. Once the agent
// observes it, the operations settle and are dropped only after
// OperationRetention.
func pendingNeverPruned(f fixture) {
	run := f.mustCommitAt(t0, "p-0", runIntent("vws.small"), instanceSpec(1, 1, compute.PowerRunning), true)
	ops := []compute.Operation{run}
	for i := 1; i <= historyLen; i++ {
		ops = append(ops, f.mustCommitAt(t0.Add(time.Duration(i)*time.Hour), fmt.Sprintf("p-%d", i), startIntent(1),
			instanceSpec(1, 1, compute.PowerRunning), false))
	}
	for _, op := range ops {
		f.wantHeld(op)
	}
	f.wantState(run, compute.OperationPending)

	seen := t0.Add(time.Duration(historyLen+1) * time.Hour)
	f.observeInstance(1, compute.InstanceStatus{Observed: compute.Observed{Generation: 1}, UpdatedAt: seen})
	f.wantState(run, compute.OperationSucceeded)
	soon := f.mustCommitAt(
		seen.Add(time.Hour),
		"p-soon",
		startIntent(1),
		instanceSpec(1, 1, compute.PowerRunning),
		false,
	)
	for _, op := range ops {
		f.wantHeld(op)
	}
	late := f.mustCommitAt(seen.Add(store.OperationRetention+time.Hour), "p-late", startIntent(1),
		instanceSpec(1, 1, compute.PowerRunning), false)
	all := append(ops, soon, late)
	cut := len(all) - store.KeepOperations
	for _, op := range all[:cut] {
		f.wantPruned(op)
	}
	for _, op := range all[cut:] {
		f.wantHeld(op)
	}
}
