package store_test

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

const (
	keep      = store.KeepOperations
	retention = store.OperationRetention
)

var pruneT0 = time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)

// historyOf builds n operations on one instance, created an hour apart. The
// operation of generation g settles when generation g+1 is committed, as a
// CommitDesired supersedes it. The last one is still Pending.
func historyOf(t *testing.T, n int) []compute.Operation {
	t.Helper()
	in, err := compute.NewRunInstance("img-00000000000000001", "vws.small", "")
	if err != nil {
		t.Fatal(err)
	}
	id, err := compute.ParseInstanceID("i-00000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	ops := make([]compute.Operation, 0, n)
	for g := 1; g <= n; g++ {
		req, err := compute.ParseRequestID(fmt.Sprintf("h-%d", g))
		if err != nil {
			t.Fatal(err)
		}
		at := pruneT0.Add(time.Duration(g) * time.Hour)
		spec := compute.InstanceSpec{ID: id, Generation: int64(g), CreatedAt: pruneT0, ChangedAt: at}
		env := compute.Env{Now: at, Rand: rand.Reader}
		op, err := compute.NewOperation(env, req, in, compute.Change{Spec: spec, Write: true})
		if err != nil {
			t.Fatal(err)
		}
		for i := range ops {
			ops[i] = compute.Settle(ops[i], int64(g), compute.Observed{}, at)
		}
		ops = append(ops, op)
	}
	return ops
}

func TestPruneKeepsTheNewestAndTheRecent(t *testing.T) {
	for _, tc := range []struct {
		name     string
		n        int
		now      time.Time
		wantKept int
	}{
		{"at the limit", keep, pruneT0.Add(1000 * time.Hour), keep},
		{"beyond the limit, all old", keep + 50, pruneT0.Add(1000 * time.Hour), keep},
		{"beyond the limit, all within retention", keep + 50, pruneT0.Add(retention), keep + 50},
		// Generation g settles at hour g+1; the cutoff is hour 30.
		{"beyond the limit, some within retention", keep + 50, pruneT0.Add(30*time.Hour + retention), keep + 22},
		{"empty", 0, pruneT0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			all := historyOf(t, tc.n)
			kept, dropped := store.PruneOperations(all, tc.now)
			if len(kept) != tc.wantKept || len(dropped)+len(kept) != tc.n {
				t.Fatalf("kept %d, dropped %d of %d; want %d kept", len(kept), len(dropped), tc.n, tc.wantKept)
			}
			// The kept operations are the newest, in order.
			for i, op := range kept {
				if op.ID != all[tc.n-tc.wantKept+i].ID {
					t.Fatalf("kept[%d] = %s, want %s", i, op.ID, all[tc.n-tc.wantKept+i].ID)
				}
			}
			// Pruning again drops nothing: prune is idempotent.
			if _, again := store.PruneOperations(kept, tc.now); len(again) != 0 {
				t.Fatalf("second prune dropped %d", len(again))
			}
		})
	}
}

func TestPruneNeverDropsAPendingOperation(t *testing.T) {
	ops := historyOf(t, keep+50)
	// The oldest operation is still Pending, as an operation whose
	// generation the agent never observed and no later spec superseded.
	pending := historyOf(t, 1)[0]
	if pending.State().Terminal() {
		t.Fatal("fixture operation is settled")
	}
	ops[0] = pending
	kept, dropped := store.PruneOperations(ops, pruneT0.Add(1000*time.Hour))
	if len(kept) != keep+1 || len(dropped) != 49 {
		t.Fatalf("kept %d, dropped %d; want %d kept and 49 dropped", len(kept), len(dropped), keep+1)
	}
	if kept[0].ID != pending.ID {
		t.Fatalf("pending operation %s was dropped", pending.ID)
	}
	for _, op := range dropped {
		if !op.State().Terminal() {
			t.Fatalf("dropped a %v operation", op.State())
		}
	}
}
