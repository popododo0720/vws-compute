package kubestore

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
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

func TestRecordOverTheConfigMapLimitIsLimitExceeded(t *testing.T) {
	e := &entry{ref: compute.Ref{Kind: compute.KindInstance, ID: "i-00000000000000001"}, exists: true}
	// Recent operations are never pruned, so a burst can still outgrow the
	// limit. Settled operations encode to a few hundred bytes each.
	e.ops = historyOf(t, 6000)
	_, err := e.configMap("ns")
	if compute.CodeOf(err) != compute.CodeLimitExceeded {
		t.Fatalf("configMap = %v, want LimitExceeded", err)
	}
}
