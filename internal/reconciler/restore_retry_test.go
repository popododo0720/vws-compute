package reconciler_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// TestRunRetriesAFailedRestore pins the review of vws-compute#93 (ce6fc94):
// Restore is the only path that re-applies a converged image's DataSource
// labels (such as revoking vws.io/public), so Run retries it until it
// succeeds instead of logging once and moving on.
func TestRunRetriesAFailedRestore(t *testing.T) {
	repo := memstore.New()
	clk := &clock{t: t0}
	h := newHarness(t, repo, fakeagent.New(), clk)
	id := h.running()
	img := h.instance(id).Spec.Image.String()

	agent := fakeagent.New()
	agent.Fail(img, errors.New("apiserver unavailable"), errors.New("apiserver unavailable"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- reconciler.New(repo, agent, reconciler.WithInterval(5*time.Millisecond)).Run(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for !agent.HasImage(img) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if !agent.HasImage(img) {
		t.Fatal("Run never retried the failed Restore, so the image was never re-applied")
	}
}
