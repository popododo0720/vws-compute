package kubestore_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
)

// hourly is change for generation gen of instance 1, made gen hours after t0.
func hourly(t *testing.T, gen int) store.DesiredChange {
	t.Helper()
	at := t0.Add(time.Duration(gen) * time.Hour)
	spec := runSpec(t, 1, int64(gen))
	spec.ChangedAt = at
	in := ok(compute.NewRunInstance("img-00000000000000001", "vws.small", ""))
	req := ok(compute.ParseRequestID(fmt.Sprintf("h-%d", gen)))
	env := compute.Env{Now: at, Rand: rand.Reader}
	op := ok(compute.NewOperation(env, req, in, compute.Change{Spec: spec, Write: true}))
	return store.DesiredChange{Op: op, Spec: spec, Write: true}
}

// recordOps returns the size of instance 1's record and its operation count.
func recordOps(t *testing.T, c client.Client, ns string) (int, int) {
	t.Helper()
	cms := configMaps(t, c, ns, "instance")
	if len(cms) != 1 {
		t.Fatalf("instance objects = %d, want 1", len(cms))
	}
	data := cms[0].Data["record"]
	var rec struct {
		Ops []json.RawMessage `json:"ops"`
	}
	if err := json.Unmarshal([]byte(data), &rec); err != nil {
		t.Fatal(err)
	}
	return len(data), len(rec.Ops)
}

func TestOperationHistoryIsBounded(t *testing.T) {
	raw, ns := kubetest.Namespace(t)
	ctx := context.Background()
	s := kubestore.New(raw, ns)
	const n = 130
	ops := make([]compute.Operation, n+1)
	for g := 1; g <= n; g++ {
		op, err := s.CommitDesired(ctx, hourly(t, g))
		if err != nil {
			t.Fatalf("commit generation %d: %v", g, err)
		}
		ops[g] = op
	}
	size, held := recordOps(t, raw, ns)
	t.Logf("record holds %d operations in %d bytes, %d bytes per operation", held, size, size/held)
	if held != 100 {
		t.Fatalf("record holds %d operations, want 100", held)
	}
	if size > 1<<20/4 {
		t.Fatalf("record is %d bytes, want well under the 1 MiB ConfigMap limit", size)
	}
	if got := len(configMaps(t, raw, ns, "request")); got != 100 {
		t.Fatalf("request indexes = %d, want 100", got)
	}
	// The oldest operations are gone, with their request ids.
	if _, err := s.OperationByRequest(ctx, ops[1].Request); compute.CodeOf(err) != compute.CodeNotFound {
		t.Fatalf("pruned request = %v, want NotFound", err)
	}
	if _, err := s.Operation(ctx, ops[1].ID); compute.CodeOf(err) != compute.CodeNotFound {
		t.Fatalf("pruned operation = %v, want NotFound", err)
	}
	// The newest is still Pending, and a replay of a recent request id
	// answers with its own operation and writes nothing.
	last, err := s.OperationByRequest(ctx, ops[n].Request)
	if err != nil || last.ID != ops[n].ID || last.State().Terminal() {
		t.Fatalf("newest = %v %v, %v; want %s Pending", last.ID, last.State(), err, ops[n].ID)
	}
	for _, g := range []int{n - 99, n - 1, n} {
		again, err := s.CommitDesired(ctx, hourly(t, g))
		if err != nil || again.ID != ops[g].ID {
			t.Fatalf("replay of h-%d = %v, %v; want %s", g, again.ID, err, ops[g].ID)
		}
	}
	if _, after := recordOps(t, raw, ns); after != held {
		t.Fatalf("replays changed the record to %d operations", after)
	}
}

func TestPrunedIndexLeftByACrashIsRepaired(t *testing.T) {
	raw, ns := kubetest.Namespace(t)
	ctx := context.Background()
	s := kubestore.New(raw, ns)
	for g := 1; g <= 100; g++ {
		if _, err := s.CommitDesired(ctx, hourly(t, g)); err != nil {
			t.Fatal(err)
		}
	}
	// The process dies after the record dropped h-1 and before its index
	// is deleted.
	boom := errors.New("process died")
	c := hook(raw, interceptor.Funcs{
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error { return boom },
	})
	if _, err := kubestore.New(c, ns).CommitDesired(ctx, hourly(t, 101)); err != nil {
		t.Fatal(err)
	}
	if got := len(configMaps(t, raw, ns, "request")); got != 101 {
		t.Fatalf("request indexes after the crash = %d, want 101", got)
	}
	r, err := kubestore.New(raw, ns).Repair(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if r.OrphanIndexes != 1 || r.RestoredIndexes != 0 {
		t.Fatalf("repair = %+v, want one orphan index", r)
	}
	h1 := ok(compute.ParseRequestID("h-1"))
	if _, err := s.OperationByRequest(ctx, h1); compute.CodeOf(err) != compute.CodeNotFound {
		t.Fatalf("pruned request after repair = %v, want NotFound", err)
	}
}

func TestTooLargeRecordFailsOnceWithLimitExceeded(t *testing.T) {
	raw, ns := kubetest.Namespace(t)
	ctx := context.Background()
	if _, err := kubestore.New(raw, ns).CommitDesired(ctx, hourly(t, 1)); err != nil {
		t.Fatal(err)
	}
	updates := 0
	c := hook(raw, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.UpdateOption) error {
			if strings.HasPrefix(o.GetName(), "vws-instance-") {
				updates++
				return apierrors.NewRequestEntityTooLargeError("limit is 3145728")
			}
			return c.Update(ctx, o, opts...)
		},
	})
	_, err := kubestore.New(c, ns).CommitDesired(ctx, hourly(t, 2))
	if compute.CodeOf(err) != compute.CodeLimitExceeded {
		t.Fatalf("commit = %v, want LimitExceeded", err)
	}
	if updates != 1 {
		t.Fatalf("resource updates = %d, want 1: a too-large write is not retried", updates)
	}
	if got := len(configMaps(t, raw, ns, "request")); got != 1 {
		t.Fatalf("request indexes = %d, want 1: the failed commit gives its index back", got)
	}
}
