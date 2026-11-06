package main

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-lib/obs"
	"github.com/popododo0720/vws-lib/obs/grpcobs"
)

// tracedVolumes is a vws-storage VolumeService that records the trace and
// request id each GetVolume arrives with.
type tracedVolumes struct {
	fakeVolumes
	mu        sync.Mutex
	traceID   trace.TraceID
	requestID string
}

func (f *tracedVolumes) GetVolume(ctx context.Context, r *storagev1.GetVolumeRequest) (*storagev1.Volume, error) {
	f.mu.Lock()
	f.traceID = trace.SpanContextFromContext(ctx).TraceID()
	f.requestID = obs.RequestID(ctx)
	f.mu.Unlock()
	return f.fakeVolumes.GetVolume(ctx, r)
}

func (f *tracedVolumes) seenCall() (trace.TraceID, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.traceID, f.requestID
}

// TestStorageCallJoinsTheCallersTrace runs the binary between a traced
// client and a fake vws-storage. A CreateInstance that asks storage about its
// volume carries the caller's trace and request id to storage, and compute
// records the storage call as a client span under its CreateInstance span.
func TestStorageCallJoinsTheCallersTrace(t *testing.T) {
	spans := tracetest.NewSpanRecorder()
	old := setupObs
	setupObs = func(ctx context.Context, opts ...obs.Option) (func(context.Context) error, error) {
		return obs.Setup(ctx, append(opts, obs.WithOverride(func(c *obs.Config) {
			c.SpanProcessors = []sdktrace.SpanProcessor{spans}
			c.LogOutput = io.Discard
		}))...)
	}
	t.Cleanup(func() { setupObs = old })

	vols := &tracedVolumes{}
	slis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ssrv := grpc.NewServer(grpcobs.ServerOptions("storage")...)
	storagev1.RegisterVolumeServiceServer(ssrv, vols)
	go func() { _ = ssrv.Serve(slis) }()
	t.Cleanup(ssrv.Stop)

	addr := freeAddr(t)
	args := []string{"-listen", addr, "-storage", slis.Addr().String()}
	cfg, err := parseConfig(args, func(string) string { return "" }, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	t.Cleanup(func() { cancel(); <-done })

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	ins := computev1.NewInstanceServiceClient(cc)

	const (
		parent    = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
		requestID = "c3c3c3c3-c3c3-43c3-83c3-c3c3c3c3c3c3"
	)
	wantTrace, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatal(err)
	}
	callCtx := metadata.AppendToOutgoingContext(ctx, "traceparent", parent, grpcobs.RequestIDHeader, requestID)
	eventually(t, "CreateInstance", func() bool {
		_, err := ins.CreateInstance(callCtx, &computev1.CreateInstanceRequest{
			Parent: "projects/demo/zones/vws-1a", InstanceId: "i-0123456789abcdef1", RequestId: requestID,
			Instance: &computev1.Instance{Spec: &computev1.InstanceSpec{
				MachineType: "vws.small", Power: computev1.InstancePower_INSTANCE_POWER_RUNNING,
				Volumes: []*computev1.VolumeAttachment{{Volume: termVolumes + "vol-root", DeviceName: "boot", Boot: true}},
			}},
		})
		return err == nil
	})

	gotTrace, gotRequest := vols.seenCall()
	if gotTrace != wantTrace {
		t.Errorf("storage saw trace %s, want the caller's %s", gotTrace, wantTrace)
	}
	if gotRequest != requestID {
		t.Errorf("storage saw request id %q, want %s", gotRequest, requestID)
	}

	servers := map[trace.SpanID]bool{}
	var clients []sdktrace.ReadOnlySpan
	for _, s := range spans.Ended() {
		switch {
		case s.Name() == "vws.compute.v1.InstanceService/CreateInstance" && s.SpanKind() == trace.SpanKindServer:
			servers[s.SpanContext().SpanID()] = true
		case s.Name() == "vws.storage.v1.VolumeService/GetVolume" && s.SpanKind() == trace.SpanKindClient:
			clients = append(clients, s)
		}
	}
	if len(servers) == 0 || len(clients) == 0 {
		t.Fatalf("recorded %d CreateInstance server spans and %d storage client spans, want both", len(servers), len(clients))
	}
	for _, c := range clients {
		if !servers[c.Parent().SpanID()] || c.SpanContext().TraceID() != wantTrace {
			t.Errorf("storage client span parent %s trace %s, want a CreateInstance span in trace %s",
				c.Parent().SpanID(), c.SpanContext().TraceID(), wantTrace)
		}
	}
}
