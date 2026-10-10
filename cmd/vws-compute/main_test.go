package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type holdServer interface {
	Hold(context.Context) error
}

type hold struct{}

func (hold) Hold(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestStopServerCancelsHandlersAfterGrace(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	entered := make(chan struct{})
	var once sync.Once
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "vws.test.Hold",
		HandlerType: (*holdServer)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Hold",
			Handler: func(s any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				if err := dec(new(emptypb.Empty)); err != nil {
					return nil, err
				}
				once.Do(func() { close(entered) })
				return nil, s.(holdServer).Hold(ctx)
			},
		}},
	}, hold{})
	served := make(chan error, 1)
	go func() { served <- srv.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	invoked := make(chan error, 1)
	go func() {
		invoked <- conn.Invoke(context.Background(), "/vws.test.Hold/Hold", &emptypb.Empty{}, &emptypb.Empty{})
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	done := make(chan struct{})
	go func() {
		stopServer(srv, 50*time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stopServer blocked on a handler that ends when its context is canceled")
	}
	if err := <-served; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		t.Fatalf("Serve: %v", err)
	}
	if err := <-invoked; err == nil {
		t.Fatal("Hold returned nil after the server canceled it")
	}
}
