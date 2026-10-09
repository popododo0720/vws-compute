// Command vws-compute serves vws.compute.v1.ComputeService over gRPC.
//
// It runs the reconciler against the in-memory fake hypervisor agent. With
// -db set, desired state, observed status, and operations persist in SQLite
// across restarts. The fake agent's machines and image artifacts do not.
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

	"google.golang.org/grpc"

	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
	"github.com/popododo0720/vws-compute/internal/store/sqlitestore"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:50051", "address to serve gRPC on")
	db := flag.String("db", "", "SQLite database path; empty keeps all state in memory")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *listen, *db); err != nil {
		fmt.Fprintln(os.Stderr, "vws-compute:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, listen, db string) error {
	repo, err := open(ctx, db)
	if err != nil {
		return err
	}
	defer repo.Close()

	lis, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	rec := reconciler.New(repo, fakeagent.New())
	srv := grpc.NewServer()
	grpcapi.Register(srv, controller.New(repo, controller.WithKick(rec.Kick)), repo)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	reconciled := make(chan error, 1)
	go func() { reconciled <- rec.Run(ctx) }()
	served := make(chan error, 1)
	go func() { served <- srv.Serve(lis) }()
	slog.Info("serving vws.compute.v1.ComputeService", "addr", lis.Addr().String(), "db", db)

	select {
	case <-ctx.Done():
		srv.GracefulStop()
		err = <-served
	case err = <-served:
	}
	cancel()
	if rerr := <-reconciled; !errors.Is(rerr, context.Canceled) {
		err = errors.Join(err, rerr)
	}
	return err
}

func open(ctx context.Context, path string) (store.Repository, error) {
	if path == "" {
		return memstore.New(), nil
	}
	return sqlitestore.Open(ctx, path)
}
