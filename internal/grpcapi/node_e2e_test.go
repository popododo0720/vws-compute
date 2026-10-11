package grpcapi_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

const (
	nodeParent = "projects/demo/zones/z1"
	nodeA      = nodeParent + "/nodes/node-a"
)

func slots(n int32) *int32 { return &n }

func nodeBody(n int32) *computev1.Node {
	return &computev1.Node{Spec: &computev1.NodeSpec{InstanceSlots: slots(n)}}
}

func (h *harness) createNode(t *testing.T, id string, n int32, req string) *longrunningpb.Operation {
	t.Helper()
	op, err := h.nodes.CreateNode(context.Background(), &computev1.CreateNodeRequest{
		Parent: nodeParent, NodeId: id, Node: nodeBody(n), RequestId: req,
	})
	if err != nil {
		t.Fatalf("CreateNode %s: %v", id, err)
	}
	return op
}

func (h *harness) syncNode(name string, state commonv1.ReconciliationState) (*computev1.SyncNodeResponse, error) {
	return h.nodes.SyncNode(context.Background(), &computev1.SyncNodeRequest{
		Name: name, Observed: &commonv1.ObservedState{State: state},
	})
}

// seedNodeDriven creates a node-driven instance through the controller, the
// same path CreateInstance takes once the volume is verified.
func (h *harness) seedNodeDriven(t *testing.T, req, id, volume string) compute.Operation {
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

func TestCreateNodeAndReads(t *testing.T) {
	ctx := context.Background()
	h := serve(t)
	const req = "21212121-2121-4121-8121-212121212121"

	op := h.createNode(t, "node-a", 4, req)
	if !op.GetDone() {
		t.Fatal("CreateNode operation is not done: registration succeeds when the row is stored")
	}
	var md commonv1.OperationMetadata
	if err := anypb.UnmarshalTo(op.GetMetadata(), &md, proto.UnmarshalOptions{}); err != nil ||
		md.GetResource() != nodeA || md.GetVerb() != commonv1.OperationVerb_OPERATION_VERB_CREATE || md.GetGeneration() != 1 {
		t.Fatalf("metadata = %v, %v", &md, err)
	}
	var created computev1.Node
	if err := anypb.UnmarshalTo(op.GetResponse(), &created, proto.UnmarshalOptions{}); err != nil {
		t.Fatal(err)
	}
	if created.GetName() != nodeA || created.GetSpec().GetInstanceSlots() != 4 || created.GetGeneration() != 1 || created.GetUid() == "" || created.GetEtag() == "" {
		t.Fatalf("created = %v", &created)
	}
	if created.GetStatus().GetLastSyncTime() != nil || created.GetStatus().GetObserved().GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_PENDING {
		t.Fatalf("a node that never synced = %v", created.GetStatus())
	}

	// The same request id returns the original operation.
	replay := h.createNode(t, "node-a", 4, req)
	if replay.GetName() != op.GetName() {
		t.Fatalf("replay operation %s, want %s", replay.GetName(), op.GetName())
	}
	// The same node under a new request id is a succeeded operation for it.
	again := h.createNode(t, "node-a", 4, "22222222-2222-4222-8222-222222222222")
	if !again.GetDone() || again.GetError() != nil {
		t.Fatalf("re-registration = %v", again)
	}
	// A different spec, or the same request id with different content, is refused.
	_, err := h.nodes.CreateNode(ctx, &computev1.CreateNodeRequest{Parent: nodeParent, NodeId: "node-a", Node: nodeBody(5)})
	wantCode(t, err, codes.AlreadyExists)
	wantReason(t, err, "ALREADY_EXISTS")
	_, err = h.nodes.CreateNode(ctx, &computev1.CreateNodeRequest{Parent: nodeParent, NodeId: "node-a", Node: nodeBody(5), RequestId: req})
	wantCode(t, err, codes.Aborted)
	wantReason(t, err, "IDEMPOTENCY_CONFLICT")

	for name, body := range map[string]*computev1.Node{
		"negative slots": nodeBody(-1),
		"too many slots": nodeBody(201),
		"no slots":       {Spec: &computev1.NodeSpec{}},
		"no spec":        {},
		"output-only":    {Spec: nodeBody(1).Spec, Uid: "x"},
		"wrong name":     {Spec: nodeBody(1).Spec, Name: nodeParent + "/nodes/other"},
	} {
		_, err := h.nodes.CreateNode(ctx, &computev1.CreateNodeRequest{Parent: nodeParent, NodeId: "node-x", Node: body})
		if got := fmt.Sprint(err); !strings.Contains(got, "InvalidArgument") {
			t.Errorf("%s: err = %v", name, err)
		}
		wantReason(t, err, map[bool]string{true: "INVALID_RESOURCE_ID", false: "INVALID_SPEC"}[name == "wrong name"])
	}
	if _, err := h.nodes.GetNode(ctx, &computev1.GetNodeRequest{Name: nodeParent + "/nodes/node-x"}); err == nil {
		t.Fatal("a refused CreateNode stored a node")
	}
	// Zero is a real value.
	h.createNode(t, "node-zero", 0, "")

	// validate_only writes nothing.
	vo, err := h.nodes.CreateNode(ctx, &computev1.CreateNodeRequest{Parent: nodeParent, NodeId: "node-v", Node: nodeBody(1), ValidateOnly: true})
	if err != nil || !vo.GetDone() || vo.GetResponse() != nil {
		t.Fatalf("validate_only = %v, %v", vo, err)
	}
	_, err = h.nodes.GetNode(ctx, &computev1.GetNodeRequest{Name: nodeParent + "/nodes/node-v"})
	wantCode(t, err, codes.NotFound)

	got, err := h.nodes.GetNode(ctx, &computev1.GetNodeRequest{Name: nodeA})
	if err != nil || got.GetSpec().GetInstanceSlots() != 4 {
		t.Fatalf("GetNode = %v, %v", got, err)
	}
	_, err = h.nodes.GetNode(ctx, &computev1.GetNodeRequest{Name: "projects/demo/zones/z1/instances/node-a"})
	wantCode(t, err, codes.InvalidArgument)

	h.createNode(t, "node-other", 1, "")
	first, err := h.nodes.ListNodes(ctx, &computev1.ListNodesRequest{Parent: nodeParent, PageSize: 2})
	if err != nil || len(first.GetNodes()) != 2 || first.GetNextPageToken() == "" {
		t.Fatalf("first page = %v, %v", first, err)
	}
	second, err := h.nodes.ListNodes(ctx, &computev1.ListNodesRequest{Parent: nodeParent, PageSize: 2, PageToken: first.GetNextPageToken()})
	if err != nil || len(second.GetNodes()) != 1 || second.GetNextPageToken() != "" {
		t.Fatalf("second page = %v, %v", second, err)
	}
	elsewhere, err := h.nodes.ListNodes(ctx, &computev1.ListNodesRequest{Parent: "projects/demo/zones/z2"})
	if err != nil || len(elsewhere.GetNodes()) != 0 {
		t.Fatalf("another zone lists %v, %v", elsewhere, err)
	}
	_, err = h.nodes.ListNodes(ctx, &computev1.ListNodesRequest{Parent: nodeParent, PageSize: -1})
	wantCode(t, err, codes.InvalidArgument)

	// Update and delete are not served yet.
	_, err = h.nodes.UpdateNode(ctx, &computev1.UpdateNodeRequest{Node: &computev1.Node{Name: nodeA}})
	wantCode(t, err, codes.Unimplemented)
	_, err = h.nodes.DeleteNode(ctx, &computev1.DeleteNodeRequest{Name: nodeA})
	wantCode(t, err, codes.Unimplemented)
}

func TestSyncNodeValidatesAndHeartbeats(t *testing.T) {
	ctx := context.Background()
	h := serve(t)
	h.createNode(t, "node-a", 4, "")

	_, err := h.syncNode(nodeParent+"/nodes/ghost", commonv1.ReconciliationState_RECONCILIATION_STATE_READY)
	wantCode(t, err, codes.NotFound)
	wantReason(t, err, "RESOURCE_NOT_FOUND")

	for _, state := range []commonv1.ReconciliationState{
		commonv1.ReconciliationState_RECONCILIATION_STATE_UNSPECIFIED,
		commonv1.ReconciliationState_RECONCILIATION_STATE_PENDING,
		commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING,
	} {
		_, err := h.syncNode(nodeA, state)
		wantCode(t, err, codes.InvalidArgument)
		wantReason(t, err, "INVALID_SPEC")
	}
	_, err = h.nodes.SyncNode(ctx, &computev1.SyncNodeRequest{Name: nodeA})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.nodes.SyncNode(ctx, &computev1.SyncNodeRequest{
		Name: nodeA, AgentVersion: strings.Repeat("v", 64),
		Observed: &commonv1.ObservedState{State: commonv1.ReconciliationState_RECONCILIATION_STATE_READY},
	})
	wantCode(t, err, codes.InvalidArgument)
	if n, _ := h.nodes.GetNode(ctx, &computev1.GetNodeRequest{Name: nodeA}); n.GetStatus().GetLastSyncTime() != nil {
		t.Fatal("a refused sync wrote a heartbeat")
	}

	resp, err := h.nodes.SyncNode(ctx, &computev1.SyncNodeRequest{
		Name: nodeA, AgentVersion: strings.Repeat("v", 63),
		Observed: &commonv1.ObservedState{
			State: commonv1.ReconciliationState_RECONCILIATION_STATE_RECONCILING, Message: "warming up",
			Generation: 99, Node: "projects/demo/zones/z1/nodes/lies",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	st := resp.GetNode().GetStatus()
	if st.GetLastSyncTime() == nil || st.GetAgentVersion() != strings.Repeat("v", 63) || len(resp.GetInstances()) != 0 ||
		st.GetObserved().GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_RECONCILING ||
		st.GetObserved().GetMessage() != "warming up" ||
		st.GetObserved().GetNode() != nodeA || st.GetObserved().GetGeneration() != 1 {
		t.Fatalf("status = %v: the server owns observed.node and observed.generation", st)
	}
	resp, err = h.syncNode(nodeA, commonv1.ReconciliationState_RECONCILIATION_STATE_READY)
	if err != nil || resp.GetNode().GetStatus().GetAgentVersion() != strings.Repeat("v", 63) {
		t.Fatalf("an empty agent_version must keep the stored one: %v, %v", resp.GetNode().GetStatus(), err)
	}
}

func TestNodeLifecycleOfAnInstance(t *testing.T) {
	ctx := context.Background()
	h := serve(t)
	h.createNode(t, "node-a", 1, "")
	h.createNode(t, "node-b", 5, "")
	instName := "projects/demo/zones/z1/instances/web-1"

	op := h.seedNodeDriven(t, "seed-1", "web-1", "root-1")
	// With no synced node the instance waits unassigned.
	got, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: instName})
	if err != nil || got.GetStatus().GetAssignedNode() != "" || got.GetStatus().GetObserved().GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_PENDING {
		t.Fatalf("unplaced instance = %v, %v", got, err)
	}

	// node-b has the most free slots and is the first to sync.
	if _, err := h.syncNode(nodeParent+"/nodes/node-b", commonv1.ReconciliationState_RECONCILIATION_STATE_READY); err != nil {
		t.Fatal(err)
	}
	nodeB := nodeParent + "/nodes/node-b"
	resp, err := h.syncNode(nodeA, commonv1.ReconciliationState_RECONCILIATION_STATE_READY)
	if err != nil || len(resp.GetInstances()) != 0 {
		t.Fatalf("node-a was given %v, %v", resp.GetInstances(), err)
	}
	synced, err := h.syncNode(nodeB, commonv1.ReconciliationState_RECONCILIATION_STATE_READY)
	if err != nil || len(synced.GetInstances()) != 1 {
		t.Fatalf("node-b sync = %v, %v", synced, err)
	}
	inst := synced.GetInstances()[0]
	if inst.GetName() != instName || inst.GetStatus().GetAssignedNode() != nodeB || inst.GetSpec().GetVolumes()[0].GetVolume() != "projects/demo/zones/z1/volumes/root-1" {
		t.Fatalf("assigned instance = %v", inst)
	}

	report := func(from string, gen int64, state commonv1.ReconciliationState, node string) (*computev1.ReportInstanceStatusResponse, error) {
		return h.inst.ReportInstanceStatus(ctx, &computev1.ReportInstanceStatusRequest{
			Name: from,
			Status: &computev1.InstanceStatus{
				Observed: &commonv1.ObservedState{State: state, Generation: gen, Node: node},
				Power:    computev1.InstancePower_INSTANCE_POWER_RUNNING,
				// The server keeps its own value for this field.
				AssignedNode: nodeA,
				NetworkInterfaces: []*computev1.ObservedNetworkInterface{
					{DeviceName: "eth0", PrivateIpv4: "10.0.0.7"},
				},
			},
		})
	}
	ready := commonv1.ReconciliationState_RECONCILIATION_STATE_READY
	_, err = report(instName, 1, ready, nodeA)
	wantCode(t, err, codes.InvalidArgument)
	wantReason(t, err, "INVALID_SPEC")
	_, err = report(instName, 1, ready, "")
	wantCode(t, err, codes.InvalidArgument)
	_, err = report(instName, 2, ready, nodeB)
	wantCode(t, err, codes.Aborted)
	wantReason(t, err, "GENERATION_CONFLICT")
	_, err = report(instName, 1, commonv1.ReconciliationState_RECONCILIATION_STATE_UNSPECIFIED, nodeB)
	wantCode(t, err, codes.InvalidArgument)
	_, err = report("projects/demo/zones/z1/instances/nope", 1, ready, nodeB)
	wantCode(t, err, codes.NotFound)
	_, err = report("projects/demo/zones/other/instances/web-1", 1, ready, nodeB)
	wantCode(t, err, codes.NotFound)
	if o := h.opState(t, op); o.GetDone() {
		t.Fatal("a refused report settled the create operation")
	}

	got2, err := report(instName, 1, ready, nodeB)
	if err != nil {
		t.Fatal(err)
	}
	i2 := got2.GetInstance()
	if i2.GetStatus().GetAssignedNode() != nodeB || i2.GetStatus().GetObserved().GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_READY ||
		i2.GetStatus().GetObserved().GetGeneration() != 1 || len(i2.GetStatus().GetNetworkInterfaces()) != 1 {
		t.Fatalf("after report = %v", i2.GetStatus())
	}
	if o := h.opState(t, op); !o.GetDone() || o.GetError() != nil {
		t.Fatalf("create operation after READY = %v", o)
	}

	// Delete: the node keeps the instance until it reports DELETING.
	del, err := h.inst.DeleteInstance(ctx, &computev1.DeleteInstanceRequest{Name: instName})
	if err != nil || del.GetDone() {
		t.Fatalf("DeleteInstance on an assigned instance = %v, %v", del, err)
	}
	resp, err = h.syncNode(nodeB, ready)
	if err != nil || len(resp.GetInstances()) != 1 || resp.GetInstances()[0].GetDeleteTime() == nil || resp.GetInstances()[0].GetGeneration() != 2 {
		t.Fatalf("sync during delete = %v, %v", resp, err)
	}
	if _, err := report(instName, 2, ready, nodeB); err != nil {
		t.Fatal(err)
	}
	if o := h.opByName(t, del.GetName()); o.GetDone() {
		t.Fatal("READY converged a delete")
	}
	if _, err := report(instName, 2, commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING, nodeB); err != nil {
		t.Fatal(err)
	}
	if o := h.opByName(t, del.GetName()); !o.GetDone() || o.GetError() != nil {
		t.Fatalf("delete operation = %v", o)
	}
	_, err = h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: instName})
	wantCode(t, err, codes.NotFound)
	if _, err := report(instName, 2, commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING, nodeB); err != nil {
		t.Fatalf("a retried DELETING report after removal = %v", err)
	}
	resp, err = h.syncNode(nodeB, ready)
	if err != nil || len(resp.GetInstances()) != 0 {
		t.Fatalf("a removed instance is still synced: %v, %v", resp.GetInstances(), err)
	}
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

func TestSyncNodeRefusesAnOversizedResponseAndWritesNothing(t *testing.T) {
	ctx := context.Background()
	h := serveRepo(t, memstore.New(), grpcapi.WithMaxSyncResponseBytes(1))
	h.createNode(t, "node-a", 2, "")
	h.seedNodeDriven(t, "seed-1", "web-1", "root-1")

	_, err := h.syncNode(nodeA, commonv1.ReconciliationState_RECONCILIATION_STATE_READY)
	wantCode(t, err, codes.ResourceExhausted)
	n, err := h.nodes.GetNode(ctx, &computev1.GetNodeRequest{Name: nodeA})
	if err != nil || n.GetStatus().GetLastSyncTime() != nil {
		t.Fatalf("a refused sync heartbeated: %v, %v", n.GetStatus(), err)
	}
	got, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: "projects/demo/zones/z1/instances/web-1"})
	if err != nil || got.GetStatus().GetAssignedNode() != "" {
		t.Fatalf("a refused sync placed the instance: %v, %v", got.GetStatus(), err)
	}
}

// TestNodeStateSurvivesRestart starts a second controller and server over the
// same store, which is what a process restart does. The sqlite store keeps the
// same rows across a real restart: see TestNodeDrivenStateSurvivesRestart in
// sqlitestore.
func TestNodeStateSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	repo := memstore.New()
	h := serveRepo(t, repo)
	const req = "23232323-2323-4323-8323-232323232323"
	h.createNode(t, "node-a", 2, req)
	h.seedNodeDriven(t, "seed-1", "web-1", "root-1")
	if _, err := h.syncNode(nodeA, commonv1.ReconciliationState_RECONCILIATION_STATE_READY); err != nil {
		t.Fatal(err)
	}
	h.stop()

	h = serveRepo(t, repo)
	resp, err := h.syncNode(nodeA, commonv1.ReconciliationState_RECONCILIATION_STATE_READY)
	if err != nil || len(resp.GetInstances()) != 1 || resp.GetInstances()[0].GetStatus().GetAssignedNode() != nodeA {
		t.Fatalf("sync after restart = %v, %v", resp, err)
	}
	if replay := h.createNode(t, "node-a", 2, req); !replay.GetDone() {
		t.Fatalf("replay after restart = %v", replay)
	}
	if _, err := h.nodes.GetNode(ctx, &computev1.GetNodeRequest{Name: nodeA}); err != nil {
		t.Fatal(err)
	}
}
