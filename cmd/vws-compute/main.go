// Command vws-compute serves the vws.compute.v1 resource services over gRPC.
//
// It runs the reconciler against the in-memory fake hypervisor agent. With
// -namespace set, desired state, observed status, and operations persist as
// ConfigMaps in that Kubernetes namespace. Startup repairs the namespace and
// re-applies the stored specs, so the fake agent is rebuilt after a restart.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:50051", "address to serve gRPC on")
	namespace := flag.String("namespace", "", "Kubernetes namespace that holds the ConfigMap store; empty keeps all state in memory")
	kubeconfig := flag.String("kubeconfig", "", "kubeconfig path for -namespace; empty uses the in-cluster service account, then $KUBECONFIG and the default path")
	seedImages := flag.String("seed-images", "", "JSON file of disk images to register before serving; empty registers none")
	storage := flag.String("storage", "", "vws-storage gRPC address CreateInstance verifies volumes against; empty makes CreateInstance answer DEPENDENCY_MISSING")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *listen, *namespace, *kubeconfig, *seedImages, *storage); err != nil {
		fmt.Fprintln(os.Stderr, "vws-compute:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, listen, namespace, kubeconfig, seedImages, storage string) error {
	seed, err := readSeed(seedImages)
	if err != nil {
		return err
	}
	repo, err := open(ctx, namespace, kubeconfig)
	if err != nil {
		return err
	}
	defer repo.Close()

	rec := reconciler.New(repo, fakeagent.New(), reconciler.WithReports(repo))
	ctl := controller.New(repo, controller.WithKick(rec.Kick))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	reconciled := make(chan error, 1)
	go func() { reconciled <- rec.Run(ctx) }()
	if err := applySeed(ctx, ctl, repo, seed); err != nil {
		return stopReconciler(cancel, reconciled, err)
	}

	lis, err := net.Listen("tcp", listen)
	if err != nil {
		return stopReconciler(cancel, reconciled, err)
	}
	var opts []grpcapi.Option
	if storage != "" {
		// The connection is lazy: the storage service may come up after this
		// process, and CreateInstance answers UNAVAILABLE until it does.
		lookup, closeStorage, err := grpcapi.DialStorage(storage)
		if err != nil {
			return stopReconciler(cancel, reconciled, err)
		}
		defer closeStorage()
		opts = append(opts, grpcapi.WithVolumes(lookup))
	}
	srv := grpc.NewServer(grpc.WaitForHandlers(true))
	grpcapi.Register(srv, ctl, repo, opts...)
	served := make(chan error, 1)
	go func() { served <- srv.Serve(lis) }()
	slog.Info("serving vws.compute.v1", "addr", lis.Addr().String(), "namespace", namespace)

	select {
	case <-ctx.Done():
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

// shutdownGrace is how long Stop waits for in-flight RPCs before it cancels
// them. WaitOperation may block for up to a minute, and GracefulStop does
// not cancel handlers.
const shutdownGrace = 2 * time.Second

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

func open(ctx context.Context, namespace, kubeconfig string) (store.Repository, error) {
	if namespace == "" {
		return memstore.New(), nil
	}
	repo, repaired, err := kubestore.Open(ctx, kubeconfig, namespace)
	if err != nil {
		return nil, err
	}
	slog.Info("opened store", "namespace", namespace, "repaired", repaired)
	return repo, nil
}
