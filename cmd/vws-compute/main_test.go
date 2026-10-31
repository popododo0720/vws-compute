package main

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
)

func TestMain(m *testing.M) { os.Exit(kubetest.Run(m)) }

// TestReadyzIsNotReadyBeforeGRPCServes holds startup between observability
// setup and serving, then probes the lib health handler. A handler with no
// checks is ready, so this fails unless run registers the gRPC verdict before
// the store open.
func TestReadyzIsNotReadyBeforeGRPCServes(t *testing.T) {
	healthAddr := freeAddr(t)
	t.Setenv("VWS_HEALTH_ADDR", healthAddr)

	oldOpen := openStore
	blocked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	openStore = func(
		ctx context.Context, namespace, kubeconfig string, scope compute.ImageScope,
	) (store.Repository, error) {
		close(blocked)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return oldOpen(ctx, namespace, kubeconfig, scope)
	}
	t.Cleanup(func() {
		unblock()
		openStore = oldOpen
	})

	cfg, err := parseConfig([]string{"-listen", freeAddr(t)}, func(string) string { return "" }, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	t.Cleanup(func() {
		cancel()
		unblock()
		<-done
	})

	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("run did not reach the store open")
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+healthAddr+"/readyz", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("probe /readyz before serving: %v", err)
	}
	body, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("/readyz before the gRPC server serves = %d %s, want 503", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), `"grpc"`) {
		t.Fatalf("/readyz body = %s, want the grpc check listed", body)
	}
}
