package grpcapi

import (
	"context"
	"fmt"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

const (
	// maxSyncResponseBytes is the SyncNode response size agents accept.
	maxSyncResponseBytes = 16 << 20
	maxAgentVersionBytes = 63
	maxNodeSlots         = compute.MaxNodeSlots
)

func nodeResourceName(project, zone, id string) string {
	return "projects/" + project + "/zones/" + zone + "/nodes/" + id
}

func parseNodeResource(name string) (compute.NodeName, error) {
	if _, _, _, err := parseScopedName(name, "zones", "nodes"); err != nil {
		return compute.NodeName{}, err
	}
	n, err := compute.ParseNodeName(name)
	if err != nil {
		return compute.NodeName{}, invalidID("node " + name + " is not a resource name")
	}
	return n, nil
}

func (s *server) GetNode(ctx context.Context, r *computev1.GetNodeRequest) (*computev1.Node, error) {
	name, err := parseNodeResource(r.GetName())
	if err != nil {
		return nil, err
	}
	n, err := s.read.Node(ctx, name)
	if err != nil {
		return nil, toStatus(err)
	}
	return nodePB(n), nil
}

func (s *server) ListNodes(ctx context.Context, r *computev1.ListNodesRequest) (*computev1.ListNodesResponse, error) {
	project, zone, err := parseZonalParent(r.GetParent())
	if err != nil {
		return nil, err
	}
	if r.GetPageSize() < 0 {
		return nil, invalidSpec("page_size must not be negative")
	}
	size := int(r.GetPageSize())
	if size == 0 {
		size = defaultPageSize
	}
	size = min(size, maxPageSize)
	page, err := s.read.Nodes(ctx, store.NodeQuery{
		Prefix: nodeResourceName(project, zone, ""), PageSize: size, After: r.GetPageToken(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	out := &computev1.ListNodesResponse{NextPageToken: page.Next}
	for _, n := range page.Items {
		out.Nodes = append(out.Nodes, nodePB(n))
	}
	return out, nil
}

func (s *server) CreateNode(ctx context.Context, r *computev1.CreateNodeRequest) (*longrunningpb.Operation, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	project, zone, err := parseZonalParent(r.GetParent())
	if err != nil {
		return nil, err
	}
	if err := validateResourceID(r.GetNodeId()); err != nil {
		return nil, err
	}
	name := nodeResourceName(project, zone, r.GetNodeId())
	body := r.GetNode()
	if err := validateCreateNode(name, body); err != nil {
		return nil, err
	}
	labels, err := compute.ParseLabels(body.GetLabels())
	if err != nil {
		return nil, toStatus(err)
	}
	in, err := compute.NewCreateNode(name, int(body.GetSpec().GetInstanceSlots()), labels)
	if err != nil {
		return nil, toStatus(err)
	}
	if r.GetValidateOnly() {
		node, perr := compute.ParseNodeName(name)
		if perr != nil {
			return nil, toStatus(perr)
		}
		if _, err := s.read.Node(ctx, node); err == nil {
			return nil, alreadyExists("node " + name + " already exists")
		} else if compute.CodeOf(err) != compute.CodeNotFound {
			return nil, toStatus(err)
		}
		return doneOperation(project, name, commonv1.OperationVerb_OPERATION_VERB_CREATE, r.GetRequestId(), nil)
	}
	rc, err := s.submit(ctx, r.GetRequestId(), in, nil, nil)
	if err != nil {
		return nil, err
	}
	return s.operationPB(ctx, project, name, rc.Operation)
}

func validateCreateNode(name string, n *computev1.Node) error {
	if n == nil || n.GetSpec() == nil {
		return invalidSpec("node.spec is required")
	}
	if n.GetName() != "" && n.GetName() != name {
		return invalidID("node.name does not match parent and node_id")
	}
	if n.GetUid() != "" || n.GetGeneration() != 0 || n.GetEtag() != "" || n.GetStatus() != nil ||
		n.GetCreateTime() != nil || n.GetUpdateTime() != nil || n.GetDeleteTime() != nil {
		return invalidSpec("output-only fields must be unset")
	}
	if n.GetSpec().InstanceSlots == nil {
		return invalidSpec("node.spec.instance_slots is required")
	}
	if slots := n.GetSpec().GetInstanceSlots(); slots < 0 || slots > maxNodeSlots {
		return invalidSpec(fmt.Sprintf("node.spec.instance_slots must be 0 to %d", maxNodeSlots))
	}
	return nil
}

// UpdateNode and DeleteNode are not served yet. See the follow-up issue
// linked from the pull request that added NodeService.
func (s *server) UpdateNode(context.Context, *computev1.UpdateNodeRequest) (*longrunningpb.Operation, error) {
	return nil, status.Error(codes.Unimplemented, "UpdateNode is not served by this build")
}

func (s *server) DeleteNode(context.Context, *computev1.DeleteNodeRequest) (*longrunningpb.Operation, error) {
	return nil, status.Error(codes.Unimplemented, "DeleteNode is not served by this build")
}

func (s *server) SyncNode(ctx context.Context, r *computev1.SyncNodeRequest) (*computev1.SyncNodeResponse, error) {
	name, err := parseNodeResource(r.GetName())
	if err != nil {
		return nil, err
	}
	obs := r.GetObserved()
	if obs == nil {
		return nil, invalidSpec("observed is required")
	}
	var state compute.NodeState
	switch obs.GetState() {
	case commonv1.ReconciliationState_RECONCILIATION_STATE_READY:
		state = compute.NodeReady
	case commonv1.ReconciliationState_RECONCILIATION_STATE_RECONCILING:
		state = compute.NodeReconciling
	case commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR:
		state = compute.NodeError
	default:
		return nil, invalidSpec("observed.state must be READY, RECONCILING, or ERROR")
	}
	if len(r.GetAgentVersion()) > maxAgentVersionBytes {
		return nil, invalidSpec(fmt.Sprintf("agent_version exceeds %d bytes", maxAgentVersionBytes))
	}
	hb := compute.Heartbeat{State: state, Message: obs.GetMessage(), AgentVersion: r.GetAgentVersion()}
	var resp *computev1.SyncNodeResponse
	_, _, err = s.nodes.SyncNode(ctx, name, hb, s.now(), func(n compute.Node, insts []compute.Instance) error {
		resp = &computev1.SyncNodeResponse{Node: nodePB(n)}
		for _, inst := range insts {
			resp.Instances = append(resp.Instances, instancePB(inst, instanceResourceName(inst.Spec.Project, inst.Spec.Zone, inst.Spec.ID.String())))
		}
		if size := proto.Size(resp); size > s.maxSync {
			return withReason(codes.ResourceExhausted, "RESPONSE_TOO_LARGE",
				fmt.Sprintf("the sync response is %d bytes, over the %d byte limit", size, s.maxSync))
		}
		return nil
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return resp, nil
}

func nodePB(n compute.Node) *computev1.Node {
	state := commonv1.ReconciliationState_RECONCILIATION_STATE_PENDING
	switch n.Status.State {
	case compute.NodeReady:
		state = commonv1.ReconciliationState_RECONCILIATION_STATE_READY
	case compute.NodeReconciling:
		state = commonv1.ReconciliationState_RECONCILIATION_STATE_RECONCILING
	case compute.NodeError:
		state = commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR
	}
	slots := int32(n.Spec.Slots)
	out := &computev1.Node{
		Name:       n.Spec.Name.String(),
		Uid:        n.Spec.UID,
		Generation: n.Spec.Generation,
		Etag:       etagOf(n.Spec.Generation, n.Meta.Revision),
		Labels:     n.Meta.Labels.Map(),
		Spec:       &computev1.NodeSpec{InstanceSlots: &slots},
		Status: &computev1.NodeStatus{
			Observed: &commonv1.ObservedState{
				State:      state,
				Generation: n.Spec.Generation,
				Node:       n.Spec.Name.String(),
				Message:    n.Status.Message,
			},
			AgentVersion: n.Status.AgentVersion,
		},
		CreateTime: timestamppb.New(n.Spec.CreatedAt),
		UpdateTime: timestamppb.New(latest(n.Spec.ChangedAt, n.Meta.ChangedAt)),
	}
	if !n.Status.LastSync.IsZero() {
		out.Status.LastSyncTime = timestamppb.New(n.Status.LastSync)
	}
	return out
}

func (s *server) now() time.Time { return s.clock().UTC() }
