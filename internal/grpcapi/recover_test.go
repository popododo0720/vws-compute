package grpcapi_test

import (
	"context"
	"net"
	"strings"
	"testing"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/popododo0720/vws-compute/internal/grpcapi"
)

// panicky panics in both a unary and a streaming handler with a message
// that must not reach the client.
type panicky struct {
	healthpb.UnimplementedHealthServer
}

func (panicky) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	panic("secret internal state")
}

func (panicky) Watch(*healthpb.HealthCheckRequest, healthpb.Health_WatchServer) error {
	panic("secret internal state")
}

func TestRecoveryAnswersInternalWithoutLeaking(t *testing.T) {
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpcapi.Recovery()...)
	healthpb.RegisterHealthServer(srv, panicky{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	c := healthpb.NewHealthClient(cc)

	want := func(err error) {
		t.Helper()
		st := status.Convert(err)
		if st.Code() != codes.Internal || strings.Contains(st.Message(), "secret") {
			t.Fatalf("got %v %q, want Internal without the panic message", st.Code(), st.Message())
		}
	}
	_, err = c.Check(context.Background(), &healthpb.HealthCheckRequest{})
	want(err)
	w, err := c.Watch(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.Recv()
	want(err)
	// The process and the server survive: a second call is answered too.
	_, err = c.Check(context.Background(), &healthpb.HealthCheckRequest{})
	want(err)
}

// TestHostileNamesAreInvalidArgument sends malformed resource names to the
// read and write paths. None may reach the store or panic.
func TestHostileNamesAreInvalidArgument(t *testing.T) {
	h := serve(t)
	ctx := context.Background()
	for _, name := range []string{
		"",
		"projects/demo/zones/z1/instances/../../../etc",
		"projects/demo/zones/z1/instances/web\x00",
		"projects/demo/zones/z1/instances/" + strings.Repeat("a", 4096),
		"projects//zones//instances/",
		"projects/demo/zones/z1/instances/web/extra",
	} {
		_, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
		if c := status.Code(err); c != codes.InvalidArgument {
			t.Errorf("GetInstance(%.40q) = %v, want InvalidArgument", name, c)
		}
		_, err = h.inst.DeleteInstance(ctx, &computev1.DeleteInstanceRequest{Name: name})
		if c := status.Code(err); c != codes.InvalidArgument {
			t.Errorf("DeleteInstance(%.40q) = %v, want InvalidArgument", name, c)
		}
	}
}
