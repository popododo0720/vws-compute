// Command vws-compute serves the vws.compute.v1 resource services over gRPC.
//
// It runs the reconciler against the in-memory fake hypervisor agent, or
// against KubeVirt and CDI with -hypervisor=kubevirt. With
// -namespace set, desired state, observed status, and operations persist as
// ConfigMaps in that Kubernetes namespace, and replicas elect a leader on a
// Lease there. Every replica serves gRPC. Only the leader repairs the
// namespace and runs the reconciler, which first re-applies the stored
// specs, so the fake agent is rebuilt after a restart.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/popododo0720/vws-lib/obs"
	obshealth "github.com/popododo0720/vws-lib/obs/health"

	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/hypervisor/kubevirt"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

func main() {
	cfg, err := parseConfig(os.Args[1:], os.Getenv, os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "vws-compute:", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "vws-compute:", err)
		os.Exit(1)
	}
}

// setupObs is the observability setup run calls. Tests replace it to install
// a manual metric reader through the same path.
var setupObs = obs.Setup

// openStore is the store open run calls. Tests replace it to hold startup
// between observability setup and serving.
var openStore = open

func run(ctx context.Context, cfg config) error {
	shutdownObs, err := setupObs(ctx, obs.ForService(grpcapi.ServiceName))
	if err != nil {
		return fmt.Errorf("observability: %w", err)
	}
	// obs.Setup serves /healthz and /readyz on VWS_HEALTH_ADDR. A handler
	// with no checks is ready, so register the gRPC verdict before the store,
	// the storage dial, and the reconciler can hold startup: /readyz must
	// answer 503 until grpc.health.v1 is SERVING. The telemetry backend is
	// never a readiness check.
	var hl atomic.Pointer[grpcapi.Health]
	obshealth.Default().Register("grpc", func(context.Context) error {
		if h := hl.Load(); h != nil && h.Serving() {
			return nil
		}
		return errGRPCNotServing
	})
	defer func() {
		shut, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := shutdownObs(shut); err != nil {
			slog.WarnContext(ctx, "observability shutdown", slog.String(obs.FieldErr, err.Error()))
		}
	}()
	listen, namespace, seedImages, storage := cfg.listen, cfg.namespace, cfg.seedImages, cfg.storage
	seed, err := readSeed(seedImages)
	if err != nil {
		return err
	}
	repo, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		if err := repo.Close(); err != nil {
			slog.WarnContext(ctx, "close store", slog.String(obs.FieldErr, err.Error()))
		}
	}()

	agent, err := newAgent(cfg)
	if err != nil {
		return err
	}
	opts := []grpcapi.Option{grpcapi.WithPublicImages(cfg.publicImages)}
	var lookup grpcapi.Storage
	if storage != "" {
		// The connection is lazy: the storage service may come up after this
		// process, and CreateInstance answers UNAVAILABLE until it does.
		l, closeStorage, err := grpcapi.DialStorage(storage)
		if err != nil {
			return err
		}
		defer func() {
			if err := closeStorage(); err != nil {
				slog.WarnContext(ctx, "close storage connection", slog.String(obs.FieldErr, err.Error()))
			}
		}()
		lookup = l
		// The reconciler resolves a claim storage has not reported yet, so a
		// root volume still being made from its image leaves the instance
		// pending instead of failing the create.
		opts = append(opts, grpcapi.WithVolumes(lookup))
	}
	recOpts := []reconciler.Option{
		reconciler.WithReports(repo),
		reconciler.WithPublicImages(cfg.publicImages),
	}
	if lookup != nil {
		recOpts = append(recOpts,
			reconciler.WithClaims(grpcapi.Claims(lookup)),
			reconciler.WithSnapshotObjects(grpcapi.SnapshotObjects(lookup)),
		)
		if del := grpcapi.DeleteVolumes(lookup); del != nil {
			// Root volumes go with their instance (DeleteOnTermination).
			recOpts = append(recOpts, reconciler.WithVolumeDeleter(del))
		}
	}
	rec := reconciler.New(repo, agent, recOpts...)
	reconciler.Instrument(rec)
	ctl := controller.New(repo, controller.WithKick(rec.Kick))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	reconciled := make(chan error, 1)
	identity, err := replicaIdentity()
	if err != nil {
		return err
	}
	go func() { reconciled <- recovered(func() error { return lead(ctx, repo, identity, rec.Run) }) }()
	if err := applySeed(ctx, ctl, repo, seed); err != nil {
		return stopReconciler(cancel, reconciled, err)
	}

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", listen)
	if err != nil {
		return stopReconciler(cancel, reconciled, err)
	}
	srv := grpc.NewServer(append(grpcapi.ServerOptions(), grpc.WaitForHandlers(true))...)
	grpcapi.Register(srv, ctl, repo, opts...)
	h := grpcapi.NewHealth(srv, healthInterval, healthChecks(repo, agent)...)
	hl.Store(h)
	go h.Run(ctx)
	served := make(chan error, 1)
	go func() { served <- srv.Serve(lis) }()
	slog.InfoContext(ctx, "serving vws.compute.v1", slog.String("addr", lis.Addr().String()), slog.String("namespace", namespace))

	select {
	case <-ctx.Done():
		// NOT_SERVING first, so probes and balancers drain before the
		// listener closes.
		h.Shutdown()
		stopServer(srv, shutdownGrace)
		err = <-served
	case err = <-served:
	}
	cancel()
	if rerr := <-reconciled; !errors.Is(rerr, context.Canceled) {
		err = errors.Join(err, rerr)
	}
	return err
}

// recovered runs f and turns a panic into an error, so a reconciler bug
// stops the process through the normal shutdown path with the cause
// reported, instead of crashing past deferred cleanup.
func recovered(f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	return f()
}

// shutdownGrace is how long Stop waits for in-flight RPCs before it cancels
// them. WaitOperation may block for up to a minute, and GracefulStop does
// not cancel handlers.
const shutdownGrace = 2 * time.Second

// errGRPCNotServing is the readiness verdict before the first probe passes.
var errGRPCNotServing = errors.New("grpc health is not serving")

// stopReconciler cancels the reconciler and waits for it. A cancellation
// is the shutdown we just asked for, so it is not reported beside err.
func stopReconciler(cancel context.CancelFunc, reconciled <-chan error, err error) error {
	cancel()
	if rerr := <-reconciled; !errors.Is(rerr, context.Canceled) {
		err = errors.Join(err, rerr)
	}
	return err
}

func stopServer(srv *grpc.Server, grace time.Duration) {
	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(stopped)
	}()
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-stopped:
	case <-timer.C:
		srv.Stop()
		<-stopped
	}
}

// healthInterval is how often readiness re-checks the store and the
// cluster, and the timeout of one check round.
const healthInterval = 5 * time.Second

// pinger is a dependency readiness can check: the ConfigMap store and the
// KubeVirt agent. The memory store and the fake agent have nothing to reach.
type pinger interface {
	Ping(ctx context.Context) error
}

func healthChecks(deps ...any) []grpcapi.Check {
	var checks []grpcapi.Check
	for _, d := range deps {
		if p, ok := d.(pinger); ok {
			checks = append(checks, p.Ping)
		}
	}
	return checks
}

func newAgent(cfg config) (hypervisor.Agent, error) {
	if cfg.hypervisor != hypervisorKubeVirt {
		return fakeagent.New(), nil
	}
	return kubevirt.Open(cfg.kubeconfig, kubevirt.Config{
		Namespace:         cfg.vmNamespace,
		ImageNamespace:    cfg.imageNamespace,
		ImageStorageClass: cfg.imageStorageClass,
		ImageAccessMode:   cfg.imageAccessMode,
	})
}

func open(ctx context.Context, cfg config) (store.Repository, error) {
	if cfg.namespace == "" {
		return memstore.New().WithLegacyImageScope(cfg.legacyImageScope), nil
	}
	repo, err := kubestore.Open(cfg.kubeconfig, cfg.namespace, cfg.legacyImageScope)
	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "opened store", slog.String("namespace", cfg.namespace))
	return repo.WithLease(cfg.leaseName, cfg.leaseDuration, cfg.renewDeadline, cfg.retryPeriod), nil
}

// leader is a repository that several replicas share. Only the replica that
// holds its Lease runs the leader-only work: Repair and the reconciler.
type leader interface {
	Lead(ctx context.Context, identity string, term func(context.Context) error) error
}

// lead runs term while this replica leads, or always when the repository is
// this process's own (the memory store).
func lead(ctx context.Context, repo store.Repository, identity string, term func(context.Context) error) error {
	if l, ok := repo.(leader); ok {
		if err := l.Lead(ctx, identity, term); err != nil {
			return fmt.Errorf("lead: %w", err)
		}
		return nil
	}
	return term(ctx)
}

// replicaIdentity is the Lease holder name: the host name, which is the pod
// name in a cluster, plus a random suffix, so a restarted pod with the same
// name never mistakes the old holder for itself.
func replicaIdentity() (string, error) {
	host, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("host name: %w", err)
	}
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("replica identity: %w", err)
	}
	return host + "_" + hex.EncodeToString(b[:]), nil
}
