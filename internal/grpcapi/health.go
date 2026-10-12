package grpcapi

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Check is one readiness dependency. It returns nil when the dependency
// answers.
type Check func(ctx context.Context) error

// ServiceNames are the services Register adds. Health reports each of them
// and the overall "" service with the same status.
func ServiceNames() []string {
	return []string{
		computev1.InstanceService_ServiceDesc.ServiceName,
		computev1.ImageService_ServiceDesc.ServiceName,
		computev1.KeyPairService_ServiceDesc.ServiceName,
		longrunningpb.Operations_ServiceDesc.ServiceName,
	}
}

// Health is the grpc.health.v1 server. Every service starts NOT_SERVING and
// becomes SERVING only after all checks pass. Liveness is the process
// answering Check at all; readiness is the status it answers.
type Health struct {
	srv      *health.Server
	checks   []Check
	interval time.Duration
	timeout  time.Duration
}

// NewHealth registers grpc.health.v1 on s with every service NOT_SERVING.
func NewHealth(s grpc.ServiceRegistrar, interval time.Duration, checks ...Check) *Health {
	h := &Health{srv: health.NewServer(), checks: checks, interval: interval, timeout: interval}
	h.set(healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(s, h.srv)
	return h
}

func (h *Health) set(st healthpb.HealthCheckResponse_ServingStatus) {
	h.srv.SetServingStatus("", st)
	for _, n := range ServiceNames() {
		h.srv.SetServingStatus(n, st)
	}
}

// Probe runs every check once and sets the status from the result.
func (h *Health) Probe(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()
	var errs []error
	for _, c := range h.checks {
		if err := c(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	err := errors.Join(errs...)
	if err != nil {
		h.set(healthpb.HealthCheckResponse_NOT_SERVING)
		return err
	}
	h.set(healthpb.HealthCheckResponse_SERVING)
	return nil
}

// Run probes now and then every interval until ctx ends. It logs only
// transitions, so a steady failure does not flood the log.
func (h *Health) Run(ctx context.Context) {
	t := time.NewTicker(h.interval)
	defer t.Stop()
	var failing bool
	for {
		err := h.Probe(ctx)
		switch {
		case err != nil && !failing && ctx.Err() == nil:
			slog.Warn("health: not serving", "err", err)
		case err == nil && failing:
			slog.Info("health: serving")
		}
		failing = err != nil
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Shutdown sets every service NOT_SERVING for good, so load balancers stop
// sending work before GracefulStop. Later probes cannot undo it.
func (h *Health) Shutdown() { h.srv.Shutdown() }
