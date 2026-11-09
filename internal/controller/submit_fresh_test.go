package controller_test

import (
	"context"
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// TestSubmitFreshRunsOnlyForANewRequest: fresh runs once for a request id
// with no stored operation, its error commits nothing, and a replay of a
// stored request returns the operation without running it.
func TestSubmitFreshRunsOnlyForANewRequest(t *testing.T) {
	ctx := context.Background()
	repo := memstore.New()
	c := newController(repo)
	calls := 0
	refuse := func() (compute.Intent, error) {
		calls++
		return nil, compute.Errorf(compute.CodeNotFound, "refused")
	}
	pass := func() (compute.Intent, error) {
		calls++
		return importKey("deploy"), nil
	}

	_, err := c.SubmitFresh(ctx, req("k1"), importKey("deploy"), refuse)
	wantCode(t, err, compute.CodeNotFound)
	if calls != 1 {
		t.Fatalf("fresh ran %d times for a new request, want 1", calls)
	}
	if _, err := repo.OperationByRequest(ctx, req("k1")); compute.CodeOf(err) != compute.CodeNotFound {
		t.Fatalf("a refused request stored %v", err)
	}

	first, err := c.SubmitFresh(ctx, req("k1"), importKey("deploy"), pass)
	if err != nil || calls != 2 {
		t.Fatalf("SubmitFresh = %v, fresh ran %d times, want 2", err, calls)
	}
	again, err := c.SubmitFresh(ctx, req("k1"), importKey("deploy"), refuse)
	if err != nil || again.Operation.ID != first.Operation.ID {
		t.Fatalf("replay = %v %v, want operation %s", again.Operation.ID, err, first.Operation.ID)
	}
	if calls != 2 {
		t.Fatalf("a replay ran fresh: %d calls", calls)
	}
}
