package grpcapi_test

import (
	"context"
	"testing"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

const zoneParent = "projects/demo/zones/z1"

// seedControllerDriven creates a controller-driven instance through the controller, the
// same path CreateInstance takes once the volume is verified.
func (h *harness) seedControllerDriven(t *testing.T, req, id, volume string) compute.Operation {
	t.Helper()
	in, err := compute.NewCreateInstance(compute.CreateInstanceParams{
		ID: id, Project: "demo", Zone: "z1", MachineType: "n1.medium", Power: compute.PowerRunning,
		Volumes: []compute.VolumeAttachment{{Volume: mustVolume(t, "projects/demo/zones/z1/volumes/"+volume), DeviceName: "vda", Boot: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return h.submit(t, req, in)
}

func mustVolume(t *testing.T, name string) compute.VolumeRef {
	t.Helper()
	v, err := compute.ParseVolumeRef(name)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func (h *harness) opByName(t *testing.T, name string) *longrunningpb.Operation {
	t.Helper()
	op, err := h.ops.GetOperation(context.Background(), &longrunningpb.GetOperationRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func (h *harness) opState(t *testing.T, op compute.Operation) *longrunningpb.Operation {
	t.Helper()
	return h.opByName(t, "projects/demo/operations/"+op.ID.String())
}

func TestControllerLifecycleOfAnInstance(t *testing.T) {
	ctx := context.Background()
	h := serve(t)
	instName := "projects/demo/zones/z1/instances/web-1"

	op := h.seedControllerDriven(t, "seed-1", "web-1", "root-1")
	got, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: instName})
	if err != nil || got.GetStatus().GetObserved().GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_PENDING {
		t.Fatalf("unreported instance = %v, %v", got, err)
	}

	report := func(from string, gen int64, state commonv1.ReconciliationState, host string) (*computev1.ReportInstanceStatusResponse, error) {
		return h.inst.ReportInstanceStatus(ctx, &computev1.ReportInstanceStatusRequest{
			Name: from,
			Status: &computev1.InstanceStatus{
				Observed: &commonv1.ObservedState{State: state, Generation: gen, Node: host},
				Power:    computev1.InstancePower_INSTANCE_POWER_RUNNING,
				NetworkInterfaces: []*computev1.ObservedNetworkInterface{
					{DeviceName: "eth0", PrivateIpv4: "10.0.0.7"},
				},
			},
		})
	}
	ready := commonv1.ReconciliationState_RECONCILIATION_STATE_READY
	_, err = report(instName, 2, ready, "host-a")
	wantCode(t, err, codes.Aborted)
	wantReason(t, err, "GENERATION_CONFLICT")
	_, err = report(instName, 1, commonv1.ReconciliationState_RECONCILIATION_STATE_UNSPECIFIED, "host-a")
	wantCode(t, err, codes.InvalidArgument)
	_, err = report("projects/demo/zones/z1/instances/nope", 1, ready, "host-a")
	wantCode(t, err, codes.NotFound)
	_, err = report("projects/demo/zones/other/instances/web-1", 1, ready, "host-a")
	wantCode(t, err, codes.NotFound)
	if o := h.opState(t, op); o.GetDone() {
		t.Fatal("a refused report settled the create operation")
	}

	got2, err := report(instName, 1, ready, "host-a")
	if err != nil {
		t.Fatal(err)
	}
	i2 := got2.GetInstance()
	if i2.GetStatus().GetObserved().GetNode() != "host-a" || i2.GetStatus().GetObserved().GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_READY ||
		i2.GetStatus().GetObserved().GetGeneration() != 1 || len(i2.GetStatus().GetNetworkInterfaces()) != 1 {
		t.Fatalf("after report = %v", i2.GetStatus())
	}
	if o := h.opState(t, op); !o.GetDone() || o.GetError() != nil {
		t.Fatalf("create operation after READY = %v", o)
	}

	// Delete: the instance stays until the controller reports DELETING.
	del, err := h.inst.DeleteInstance(ctx, &computev1.DeleteInstanceRequest{Name: instName})
	if err != nil || del.GetDone() {
		t.Fatalf("DeleteInstance = %v, %v", del, err)
	}
	if _, err := report(instName, 2, ready, "host-a"); err != nil {
		t.Fatal(err)
	}
	if o := h.opByName(t, del.GetName()); o.GetDone() {
		t.Fatal("READY converged a delete")
	}
	if _, err := report(instName, 2, commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING, "host-a"); err != nil {
		t.Fatal(err)
	}
	if o := h.opByName(t, del.GetName()); !o.GetDone() || o.GetError() != nil {
		t.Fatalf("delete operation = %v", o)
	}
	_, err = h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: instName})
	wantCode(t, err, codes.NotFound)
	if _, err := report(instName, 2, commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING, "host-a"); err != nil {
		t.Fatalf("a retried DELETING report after removal = %v", err)
	}
}

// TestUnreportedDeleteStaysUntilTheControllerReports covers an instance no
// controller ever reported. Nothing here places instances, so a delete cannot
// tell "never started" from "started and not yet reported". It waits.
func TestUnreportedDeleteStaysUntilTheControllerReports(t *testing.T) {
	ctx := context.Background()
	h := serve(t)
	name := "projects/demo/zones/z1/instances/web-1"
	h.seedControllerDriven(t, "seed-1", "web-1", "root-1")
	del, err := h.inst.DeleteInstance(ctx, &computev1.DeleteInstanceRequest{Name: name})
	if err != nil || del.GetDone() {
		t.Fatalf("DeleteInstance = %v, %v", del, err)
	}
	got, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
	if err == nil && got.GetStatus().GetObserved().GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING {
		t.Fatalf("deleting instance reads as %v", got.GetStatus().GetObserved().GetState())
	}
	_, err = h.inst.ReportInstanceStatus(ctx, &computev1.ReportInstanceStatusRequest{
		Name: name,
		Status: &computev1.InstanceStatus{Observed: &commonv1.ObservedState{
			State: commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING, Generation: 2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if o := h.opByName(t, del.GetName()); !o.GetDone() || o.GetError() != nil {
		t.Fatalf("delete operation = %v", o)
	}
}

// TestControllerStateSurvivesRestart starts a second server over the same
// store, which is what a process restart does. The sqlite store keeps the
// same rows across a real restart: see TestControllerDrivenStateSurvivesRestart
// in sqlitestore.
func TestControllerStateSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	repo := memstore.New()
	h := serveRepo(t, repo)
	h.seedControllerDriven(t, "seed-1", "web-1", "root-1")
	h.stop()

	h = serveRepo(t, repo)
	name := "projects/demo/zones/z1/instances/web-1"
	if _, err := h.inst.ReportInstanceStatus(ctx, &computev1.ReportInstanceStatusRequest{
		Name: name,
		Status: &computev1.InstanceStatus{Observed: &commonv1.ObservedState{
			State: commonv1.ReconciliationState_RECONCILIATION_STATE_READY, Generation: 1, Node: "host-a"}},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
	if err != nil || got.GetStatus().GetObserved().GetGeneration() != 1 {
		t.Fatalf("after restart = %v, %v", got, err)
	}
}
