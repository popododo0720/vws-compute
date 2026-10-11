package compute

import (
	"fmt"
	"io"
	"sort"
	"time"
)

// SyncWindow is how recent a node's last heartbeat must be for placement to
// use it. Agents sync more often than this while they want assignments.
const SyncWindow = 30 * time.Second

// MaxNodeSlots is the largest NodeSpec.instance_slots.
const MaxNodeSlots = 200

// NodeName is projects/{project}/zones/{zone}/nodes/{node}. Every segment is
// a ResourceId. It is the node's identity everywhere: the store key, the
// Ref ID, and the value placement writes to an instance's assigned node.
type NodeName struct{ s string }

// ParseNodeName validates a node resource name. It fails with CodeInvalid.
func ParseNodeName(s string) (NodeName, error) {
	if err := parseResourceName(s, "zones", "nodes"); err != nil {
		return NodeName{}, Errorf(CodeInvalid, "invalid node %q", s)
	}
	return NodeName{s}, nil
}

func (n NodeName) String() string { return n.s }
func (n NodeName) IsZero() bool   { return n.s == "" }

// Ref returns the generic pointer to this node.
func (n NodeName) Ref() Ref { return Ref{Kind: KindNode, ID: n.s} }

// Project and Zone are the name's scope segments.
func (n NodeName) Project() string { return segment(n.s, 1) }
func (n NodeName) Zone() string    { return segment(n.s, 3) }

func segment(name string, i int) string {
	start := 0
	for ; i > 0; i-- {
		for start < len(name) && name[start] != '/' {
			start++
		}
		start++
	}
	end := start
	for end < len(name) && name[end] != '/' {
		end++
	}
	if start > len(name) {
		return ""
	}
	return name[start:end]
}

// NodeSpec is a node's desired state at one generation. It is scheduler
// input: the agent does not apply it.
type NodeSpec struct {
	Name       NodeName
	UID        string
	Generation int64
	CreatedAt  time.Time
	ChangedAt  time.Time
	// Slots is how many instances automatic placement may put here, 0 to
	// MaxNodeSlots.
	Slots int
}

func (s NodeSpec) Ref() Ref   { return s.Name.Ref() }
func (s NodeSpec) Gen() int64 { return s.Generation }
func (NodeSpec) isSpec()      {}

// NodeState is the state an agent reports in a heartbeat.
type NodeState uint8

const (
	NodeUnreported NodeState = iota
	NodeReady
	NodeReconciling
	NodeError
)

// NodeStatus is what heartbeats last wrote.
type NodeStatus struct {
	State        NodeState
	Message      string
	LastSync     time.Time
	AgentVersion string
}

// Node is the merged read model of a node.
type Node struct {
	Spec   NodeSpec
	Status NodeStatus
	Meta   Metadata
}

// Observed is derived. A node's spec is applied by the server, not by an
// agent, so it is converged the moment it is stored, like a key pair's.
func (n Node) Observed() Observed { return Observed{Generation: n.Spec.Generation} }

// Heartbeat is one parsed SyncNode request.
type Heartbeat struct {
	State        NodeState
	Message      string
	AgentVersion string
}

// NodeLoad is a node and how many instances occupy its slots.
type NodeLoad struct {
	Node     Node
	Assigned int
}

// Eligible reports whether placement may put a new instance on n at now:
// its state is Ready or Reconciling, its last heartbeat is no more than
// SyncWindow old, and a slot is free.
func (n NodeLoad) Eligible(now time.Time) bool {
	st := n.Node.Status
	if st.State != NodeReady && st.State != NodeReconciling {
		return false
	}
	if st.LastSync.IsZero() || now.Sub(st.LastSync) > SyncWindow {
		return false
	}
	return n.Assigned < n.Node.Spec.Slots
}

// ChooseNode picks the node for an instance in project and zone: among
// eligible nodes in that project and zone, the one with the most free slots,
// ties to the smallest name. It reports false when none is eligible.
func ChooseNode(project, zone string, nodes []NodeLoad, now time.Time) (NodeName, bool) {
	var cand []NodeLoad
	for _, n := range nodes {
		if n.Node.Spec.Name.Project() == project && n.Node.Spec.Name.Zone() == zone && n.Eligible(now) {
			cand = append(cand, n)
		}
	}
	if len(cand) == 0 {
		return NodeName{}, false
	}
	sort.Slice(cand, func(i, j int) bool {
		fi, fj := cand[i].Node.Spec.Slots-cand[i].Assigned, cand[j].Node.Spec.Slots-cand[j].Assigned
		if fi != fj {
			return fi > fj
		}
		return cand[i].Node.Spec.Name.s < cand[j].Node.Spec.Name.s
	})
	return cand[0].Node.Spec.Name, true
}

func newUID(r io.Reader) (string, error) {
	var b [16]byte
	if r == nil {
		return "", Errorf(CodeInternal, "no random source")
	}
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return "", Errorf(CodeInternal, "mint uid: %v", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

type createNode struct {
	name   NodeName
	slots  int
	labels Labels
}

// NewCreateNode registers a node under the caller's name. Plan fails with
// CodeAlreadyExists unless the stored node has the same slots and labels, in
// which case the intent changes nothing and its operation tracks that node.
func NewCreateNode(name string, slots int, labels Labels) (Intent, error) {
	n, err := ParseNodeName(name)
	if err != nil {
		return nil, err
	}
	if slots < 0 || slots > MaxNodeSlots {
		return nil, Errorf(CodeInvalid, "instance_slots must be 0 to %d", MaxNodeSlots)
	}
	return createNode{name: n, slots: slots, labels: labels}, nil
}

func (createNode) Kind() OperationKind      { return OpCreateNode }
func (in createNode) carriesMetadata() bool { return in.labels.Len() > 0 }

func (in createNode) encode(e *encoder) {
	e.field(in.name.s)
	e.field(fmt.Sprint(in.slots))
	if in.labels.Len() > 0 {
		e.labelSet("labels=", in.labels)
	}
}

func (in createNode) plan(v View, env Env) (Change, error) {
	cur, err := v.Node(in.name)
	switch {
	case err == nil:
		if cur.Spec.Slots == in.slots && cur.Meta.Labels.Equal(in.labels) {
			return Change{Spec: cur.Spec}, nil
		}
		return Change{}, Errorf(CodeAlreadyExists, "node %s already exists", in.name)
	case CodeOf(err) != CodeNotFound:
		return Change{}, err
	}
	uid, err := newUID(env.Rand)
	if err != nil {
		return Change{}, err
	}
	ch := Change{Write: true, Spec: NodeSpec{
		Name: in.name, UID: uid, Generation: 1, CreatedAt: env.Now, ChangedAt: env.Now, Slots: in.slots,
	}}
	if in.labels.Len() > 0 {
		ch.Meta = &Metadata{Labels: in.labels}
	}
	return ch, nil
}

// Placement is one decision of PlanPlacement.
type Placement struct {
	Instance InstanceID
	Node     NodeName
}

// PlanPlacement decides which of the unassigned instances, taken in the
// order given, get a node. Each decision counts against the node's free
// slots before the next, so one pass never overfills a node. An instance
// with no eligible node is left out. Stores call it inside the transaction
// that writes the result, which is what makes placement serial per zone.
func PlanPlacement(project, zone string, nodes []NodeLoad, unassigned []InstanceID, now time.Time) []Placement {
	loads := append([]NodeLoad(nil), nodes...)
	var out []Placement
	for _, id := range unassigned {
		name, ok := ChooseNode(project, zone, loads, now)
		if !ok {
			break
		}
		for i := range loads {
			if loads[i].Node.Spec.Name == name {
				loads[i].Assigned++
			}
		}
		out = append(out, Placement{Instance: id, Node: name})
	}
	return out
}

// Apply returns n with hb recorded at now. An empty AgentVersion keeps the
// stored one.
func (n Node) Apply(hb Heartbeat, now time.Time) Node {
	n.Status.State = hb.State
	n.Status.Message = hb.Message
	n.Status.LastSync = now
	if hb.AgentVersion != "" {
		n.Status.AgentVersion = hb.AgentVersion
	}
	return n
}
