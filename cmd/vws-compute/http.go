package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// serveHTTP serves h on addr until the returned stop is called. The listener
// is opened before it returns, so a bad address fails startup.
func serveHTTP(ctx context.Context, addr string, h http.Handler) (stop func(), err error) {
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen http %s: %w", addr, err)
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(lis); !errors.Is(err, http.ErrServerClosed) {
			slog.ErrorContext(ctx, "http server", slog.Any("err", err))
		}
	}()
	slog.InfoContext(ctx, "serving http probes and metrics", slog.String("addr", lis.Addr().String()))
	return func() {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownGrace)
		defer cancel()
		if err := srv.Shutdown(sctx); err != nil {
			slog.WarnContext(ctx, "stop http server", slog.Any("err", err))
		}
		<-done
	}, nil
}
