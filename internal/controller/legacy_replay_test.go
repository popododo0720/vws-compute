package controller_test

import (
	"context"
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// legacyOps answers OperationByRequest with the fingerprint a release before
// image scopes (#92) persisted, as a store written by that release does.
type legacyOps struct {
	store.Repository
}

func (s legacyOps) OperationByRequest(ctx context.Context, r compute.RequestID) (compute.Operation, error) {
	op, err := s.Repository.OperationByRequest(ctx, r)
	if err != nil {
		return op, err
	}
	// The legacy encoding has no scope, so any scope gives the same value.
	fp, _, _ := compute.LegacyFingerprintOf(copyIntent(ok(compute.NewImageScope("a1", "r1"))))
	op.Fingerprint = fp
	return op, nil
}

func copyIntent(scope compute.ImageScope) compute.Intent {
	snap := ok(compute.ParseSnapshotRef("projects/a123456789012/regions/vws-1/snapshots/s"))
	return ok(compute.NewCopyImage("ami-0000000000000aaa1", scope, compute.FromSnapshot{Ref: snap}, "", "", compute.Labels{}))
}

// TestPreUpgradeCreateImageReplaysUnderItsScope pins the review of
// vws-compute#93: a CreateImage request id persisted before image scopes
// replays to its operation for the scope its image holds, and still
// mismatches for any other scope.
func TestPreUpgradeCreateImageReplaysUnderItsScope(t *testing.T) {
	owner := ok(compute.NewImageScope("a123456789012", "vws-1"))
	other := ok(compute.NewImageScope("a999999999999", "vws-1"))
	repo := memstore.New()
	first := submit(t, newController(repo), "create-image", copyIntent(owner))

	c := newController(legacyOps{repo})
	again, err := c.Submit(context.Background(), req("create-image"), copyIntent(owner))
	if err != nil {
		t.Fatalf("pre-upgrade replay: %v", err)
	}
	if again.Operation.ID != first.Operation.ID {
		t.Fatalf("replay = %s, want %s", again.Operation.ID, first.Operation.ID)
	}
	_, err = c.Submit(context.Background(), req("create-image"), copyIntent(other))
	wantCode(t, err, compute.CodeRequestMismatch)
}
