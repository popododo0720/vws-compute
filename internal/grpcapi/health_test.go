package grpcapi_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
)

// serveHealth serves only grpc.health.v1 on loopback and returns a real
// health client for it.
func serveHealth(t *testing.T, interval time.Duration, checks ...grpcapi.Check) (*grpcapi.Health, healthpb.HealthClient) {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	h := grpcapi.NewHealth(srv, interval, checks...)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return h, healthpb.NewHealthClient(cc)
}

func wantStatus(t *testing.T, c healthpb.HealthClient, want healthpb.HealthCheckResponse_ServingStatus) {
	t.Helper()
	for _, svc := range append([]string{""}, grpcapi.ServiceNames()...) {
		resp, err := c.Check(context.Background(), &healthpb.HealthCheckRequest{Service: svc})
		if err != nil {
			t.Fatalf("Check(%q): %v", svc, err)
		}
		if resp.GetStatus() != want {
			t.Fatalf("Check(%q) = %v, want %v", svc, resp.GetStatus(), want)
		}
	}
}

func TestHealthServingFollowsChecks(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	check := func(context.Context) error {
		if down.Load() {
			return errors.New("store unreachable")
		}
		return nil
	}
	h, c := serveHealth(t, time.Hour, check)
	wantStatus(t, c, healthpb.HealthCheckResponse_NOT_SERVING) // before any probe

	if err := h.Probe(context.Background()); err == nil {
		t.Fatal("Probe with a failing check returned nil")
	}
	wantStatus(t, c, healthpb.HealthCheckResponse_NOT_SERVING)

	down.Store(false)
	if err := h.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantStatus(t, c, healthpb.HealthCheckResponse_SERVING)

	down.Store(true)
	_ = h.Probe(context.Background())
	wantStatus(t, c, healthpb.HealthCheckResponse_NOT_SERVING)
}

func TestHealthShutdownIsFinal(t *testing.T) {
	h, c := serveHealth(t, time.Hour)
	if err := h.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantStatus(t, c, healthpb.HealthCheckResponse_SERVING)
	h.Shutdown()
	wantStatus(t, c, healthpb.HealthCheckResponse_NOT_SERVING)
	_ = h.Probe(context.Background()) // a late probe must not revive it
	wantStatus(t, c, healthpb.HealthCheckResponse_NOT_SERVING)
}

func TestHealthUnknownService(t *testing.T) {
	_, c := serveHealth(t, time.Hour)
	if _, err := c.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "no.such.Service"}); err == nil {
		t.Fatal("Check of an unregistered service succeeded")
	}
}

// TestHealthRunRechecks watches the overall status while Run re-probes: a
// dependency that recovers turns SERVING without a restart, and one that
// fails turns NOT_SERVING again.
func TestHealthRunRechecks(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	h, c := serveHealth(t, 10*time.Millisecond, func(context.Context) error {
		if down.Load() {
			return errors.New("down")
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go h.Run(ctx)
	w, err := c.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	next := func(want healthpb.HealthCheckResponse_ServingStatus) {
		t.Helper()
		for {
			resp, err := w.Recv()
			if err != nil {
				t.Fatalf("waiting for %v: %v", want, err)
			}
			if resp.GetStatus() == want {
				return
			}
		}
	}
	next(healthpb.HealthCheckResponse_NOT_SERVING)
	down.Store(false)
	next(healthpb.HealthCheckResponse_SERVING)
	down.Store(true)
	next(healthpb.HealthCheckResponse_NOT_SERVING)
}

// TestHealthConfigMapStore checks readiness against a real apiserver.
func TestHealthConfigMapStore(t *testing.T) {
	s := kubetest.Store(t)
	h, c := serveHealth(t, 5*time.Second, s.Ping)
	if err := h.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantStatus(t, c, healthpb.HealthCheckResponse_SERVING)
}
