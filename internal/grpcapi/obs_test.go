package grpcapi_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-lib/obs"
	"github.com/popododo0720/vws-lib/obs/grpcobs"

	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

const (
	testParentTrace = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	testRequestID   = "11111111-1111-4111-8111-111111111111"
)

// TestReportStatusKeepsTraceAndAuditsMutations checks the two edge paths the
// service owns: an incoming traceparent and request id reach the report
// handler, and a mutating RPC writes one audit line and one audit metric.
func TestReportStatusKeepsTraceAndAuditsMutations(t *testing.T) {
	old := slog.Default()
	var logs bytes.Buffer
	reader := sdkmetric.NewManualReader()
	shutdown, err := obs.Setup(context.Background(), obs.ForService("compute"), obs.WithOverride(func(c *obs.Config) {
		c.MetricReaders = []sdkmetric.Reader{reader}
		c.LogOutput = &logs
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = shutdown(context.Background())
		slog.SetDefault(old)
	})

	var got struct {
		requestID string
		traceID   string
		spanID    string
	}
	inspect := grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo,
		next grpc.UnaryHandler,
	) (any, error) {
		if strings.HasSuffix(info.FullMethod, "/ReportInstanceStatus") {
			got.requestID = obs.RequestID(ctx)
			sc := trace.SpanContextFromContext(ctx)
			got.traceID, got.spanID = sc.TraceID().String(), sc.SpanID().String()
		}
		return next(ctx, req)
	})
	h := serveRepoServer(t, memstore.New().WithLegacyImageScope(demoScope), append(grpcapi.ServerOptions(), inspect))
	h.seedControllerDriven(t, "seed-1", "web-1", "root-1")

	instName := "projects/demo/zones/z1/instances/web-1"
	ctx := metadata.AppendToOutgoingContext(context.Background(),
		"traceparent", testParentTrace, grpcobs.RequestIDHeader, testRequestID)
	if _, err := h.inst.ReportInstanceStatus(ctx, &computev1.ReportInstanceStatusRequest{
		Name: instName, RequestId: testRequestID,
		Status: &computev1.InstanceStatus{
			Observed: &commonv1.ObservedState{
				State: commonv1.ReconciliationState_RECONCILIATION_STATE_READY, Generation: 1, Node: "host-a",
			},
			Power: computev1.InstancePower_INSTANCE_POWER_RUNNING,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if got.requestID != testRequestID {
		t.Fatalf("report request id = %q, want %s", got.requestID, testRequestID)
	}
	if got.traceID != "4bf92f3577b34da6a3ce929d0e0e4736" || got.spanID == "" || got.spanID == "0000000000000000" {
		t.Fatalf("report trace = %q span = %q, want the client's trace and a child span", got.traceID, got.spanID)
	}

	if _, err := h.inst.DeleteInstance(ctx, &computev1.DeleteInstanceRequest{Name: instName}); err != nil {
		t.Fatal(err)
	}
	line := logs.String()
	for _, want := range []string{
		`"audit":true`, `"op":"DeleteInstance"`, `"outcome":"ok"`,
		`"resource_id":"` + instName + `"`, `"request_id":"` + testRequestID + `"`,
	} {
		if !strings.Contains(line, want) {
			t.Errorf("audit log lacks %s:\n%s", want, line)
		}
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	if !hasAuditPoint(rm, "DeleteInstance", "ok") {
		t.Error("vws.audit.events has no DeleteInstance outcome=ok point")
	}
	assertNoForbiddenLabels(t, rm)
}

func hasAuditPoint(rm metricdata.ResourceMetrics, action, outcome string) bool {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != obs.MetricAuditEvents {
				continue
			}
			for _, labels := range metricLabels(m.Data) {
				if labels[obs.LabelService] == grpcapi.ServiceName &&
					labels[obs.LabelAction] == action && labels[obs.LabelOutcome] == outcome {
					return true
				}
			}
		}
	}
	return false
}

func metricLabels(data metricdata.Aggregation) []map[string]string {
	var out []map[string]string
	switch d := data.(type) {
	case metricdata.Sum[int64]:
		for _, p := range d.DataPoints {
			out = append(out, labelSet(p.Attributes))
		}
	case metricdata.Sum[float64]:
		for _, p := range d.DataPoints {
			out = append(out, labelSet(p.Attributes))
		}
	case metricdata.Gauge[int64]:
		for _, p := range d.DataPoints {
			out = append(out, labelSet(p.Attributes))
		}
	case metricdata.Gauge[float64]:
		for _, p := range d.DataPoints {
			out = append(out, labelSet(p.Attributes))
		}
	case metricdata.Histogram[int64]:
		for _, p := range d.DataPoints {
			out = append(out, labelSet(p.Attributes))
		}
	case metricdata.Histogram[float64]:
		for _, p := range d.DataPoints {
			out = append(out, labelSet(p.Attributes))
		}
	}
	return out
}

func labelSet(set attribute.Set) map[string]string {
	out := map[string]string{}
	for _, a := range set.ToSlice() {
		out[string(a.Key)] = a.Value.AsString()
	}
	return out
}

func assertNoForbiddenLabels(t *testing.T, rm metricdata.ResourceMetrics) {
	t.Helper()
	forbidden := map[string]bool{"resource_id": true, "request_id": true, "trace_id": true, "span_id": true}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			for _, labels := range metricLabels(m.Data) {
				for key := range labels {
					if forbidden[key] {
						t.Errorf("%s has forbidden label %q", m.Name, key)
					}
				}
			}
		}
	}
}
