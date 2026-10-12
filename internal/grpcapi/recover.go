package grpcapi

import (
	"context"
	"log/slog"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errPanic is what a caller sees when a handler panics. The panic value and
// stack go to the log only, never to the client.
var errPanic = status.Error(codes.Internal, "internal error")

// Recovery returns server options that turn a handler panic into
// codes.Internal for that one call, so a bug in one RPC does not take the
// process and every other call down with it.
func Recovery() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (resp any, err error) {
			defer recoverCall(ctx, info.FullMethod, &err)
			return h(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) (err error) {
			defer recoverCall(ss.Context(), info.FullMethod, &err)
			return h(srv, ss)
		}),
	}
}

func recoverCall(ctx context.Context, method string, err *error) {
	if r := recover(); r != nil {
		slog.ErrorContext(ctx, "handler panic", slog.String("method", method), slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
		*err = errPanic
	}
}
