package storetest

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

const (
	zonePrefix = "projects/p/zones/z/nodes/"
	volPrefix  = "projects/p/zones/z/volumes/"
)

func nodeName(n string) compute.NodeName { return ok(compute.ParseNodeName(zonePrefix + n)) }

func nodeSpec(n string, slots int) compute.NodeSpec {
	return compute.NodeSpec{Name: nodeName(n), UID: "uid-" + n, Generation: 1, CreatedAt: t0, ChangedAt: t0, Slots: slots}
}

func (f fixture) addNode(req, n string, slots int) {
	f.t.Helper()
	in := ok(compute.NewCreateNode(zonePrefix+n, slots, compute.Labels{}))
	op := f.mustCommit(req, in, nodeSpec(n, slots), true)
	f.wantState(op, compute.OperationSucceeded)
}

func (f fixture) sync(n string, state compute.NodeState, now time.Time) []compute.Instance {
	f.t.Helper()
	_, insts, err := f.s.SyncNode(context.Background(), nodeName(n), compute.Heartbeat{State: state},
		now, func(compute.Node, []compute.Instance) error { return nil })
	if err != nil {
		f.t.Fatalf("SyncNode %s: %v", n, err)
	}
	return insts
}

func ids(insts []compute.Instance) []string {
	out := []string{}
	for _, i := range insts {
		out = append(out, i.Spec.ID.String())
	}
	return out
}

func nodeDrivenSpec(id string, vol string, gen int64, power compute.Power) compute.InstanceSpec {
	return compute.InstanceSpec{
		ID: ok(compute.ParseInstanceID(id)), Generation: gen, CreatedAt: t0, ChangedAt: t0,
		Type: ok(compute.ParseMachineType("n1.medium")), Power: power,
		Project: "p", Zone: "z", UserData: "#cloud-config",
		Volumes: []compute.VolumeAttachment{{Volume: ok(compute.ParseVolumeRef(volPrefix + vol)), DeviceName: "vda", Boot: true}},
		NICs:    []compute.NetworkInterface{{DeviceName: "eth0", Subnet: "projects/p/regions/r/vpcs/v/subnets/s", SecurityGroups: []string{"projects/p/regions/r/vpcs/v/securityGroups/g"}}},
	}
}

func createIntent(id, vol string) compute.Intent {
	return ok(compute.NewCreateInstance(compute.CreateInstanceParams{
		ID: id, Project: "p", Zone: "z", MachineType: "n1.medium", Power: compute.PowerRunning, UserData: "#cloud-config",
		Volumes: []compute.VolumeAttachment{{Volume: ok(compute.ParseVolumeRef(volPrefix + vol)), DeviceName: "vda", Boot: true}},
		NICs:    []compute.NetworkInterface{{DeviceName: "eth0", Subnet: "projects/p/regions/r/vpcs/v/subnets/s"}},
	}))
}

func (f fixture) createNodeDriven(req, id, vol string, at time.Time) (compute.Operation, error) {
	f.t.Helper()
	spec := nodeDrivenSpec(id, vol, 1, compute.PowerRunning)
	op, err := compute.NewOperation(compute.Env{Now: at, Rand: rand.Reader}, ok(compute.ParseRequestID(req)),
		createIntent(id, vol), compute.Change{Spec: spec, Write: true})
	if err != nil {
		f.t.Fatal(err)
	}
	return f.s.CommitDesired(context.Background(), store.DesiredChange{Op: op, Spec: spec, Write: true})
}

func (f fixture) mustCreateNodeDriven(req, id, vol string, at time.Time) compute.Operation {
	f.t.Helper()
	op, err := f.createNodeDriven(req, id, vol, at)
	if err != nil {
		f.t.Fatalf("create %s: %v", id, err)
	}
	return op
}

func (f fixture) instance(id string) compute.Instance {
	f.t.Helper()
	return ok(f.s.Instance(context.Background(), ok(compute.ParseInstanceID(id))))
}

func (f fixture) report(id string, rep compute.InstanceReport, now time.Time) (compute.Instance, error) {
	return f.s.ReportInstanceStatus(context.Background(), ok(compute.ParseInstanceID(id)), rep, now)
}

func nodesRoundTrip(f fixture) {
	ctx := context.Background()
	f.addNode("req-a", "a", 4)
	f.addNode("req-b", "b", 0)
	got := ok(f.s.Node(ctx, nodeName("a")))
	if got.Spec.Slots != 4 || got.Spec.UID != "uid-a" || got.Spec.Generation != 1 || got.Status.State != compute.NodeUnreported || !got.Status.LastSync.IsZero() {
		f.t.Fatalf("node a = %+v", got)
	}
	_, err := f.s.Node(ctx, nodeName("missing"))
	wantCode(f.t, err, compute.CodeNotFound)

	other := ok(compute.ParseNodeName("projects/p/zones/other/nodes/a"))
	spec := nodeSpec("a", 1)
	spec.Name = other
	f.mustCommit("req-c", ok(compute.NewCreateNode(other.String(), 1, compute.Labels{})), spec, true)

	page := ok(f.s.Nodes(ctx, store.NodeQuery{Prefix: zonePrefix, PageSize: 1}))
	if len(page.Items) != 1 || page.Items[0].Spec.Name != nodeName("a") || page.Next == "" {
		f.t.Fatalf("first page = %+v", page)
	}
	page = ok(f.s.Nodes(ctx, store.NodeQuery{Prefix: zonePrefix, PageSize: 1, After: page.Next}))
	if len(page.Items) != 1 || page.Items[0].Spec.Name != nodeName("b") || page.Next != "" {
		f.t.Fatalf("second page = %+v", page)
	}
	if all := ok(f.s.Nodes(ctx, store.NodeQuery{})); len(all.Items) != 3 {
		f.t.Fatalf("all nodes = %d", len(all.Items))
	}
}

func nodeDrivenRoundTrip(f fixture) {
	op := f.mustCreateNodeDriven("req-1", "web-1", "root", t0)
	f.wantState(op, compute.OperationPending)
	got := f.instance("web-1")
	want := nodeDrivenSpec("web-1", "root", 1, compute.PowerRunning)
	if got.Spec.Project != "p" || got.Spec.Zone != "z" || got.Spec.UserData != "#cloud-config" || !got.Spec.NodeDriven() ||
		got.Spec.Type.Name() != "n1.medium" || len(got.Spec.Volumes) != 1 || got.Spec.Volumes[0] != want.Volumes[0] ||
		len(got.Spec.NICs) != 1 || got.Spec.NICs[0].SecurityGroups[0] != want.NICs[0].SecurityGroups[0] ||
		!got.Spec.Image.IsZero() || got.Status.AssignedNode != "" {
		f.t.Fatalf("instance = %+v", got)
	}
	// A second live instance cannot take the volume, and nothing is written.
	_, err := f.createNodeDriven("req-2", "web-2", "root", t0)
	wantCode(f.t, err, compute.CodeFailedPrecondition)
	_, err = f.s.Instance(context.Background(), ok(compute.ParseInstanceID("web-2")))
	wantCode(f.t, err, compute.CodeNotFound)
	if _, err := f.s.OperationByRequest(context.Background(), ok(compute.ParseRequestID("req-2"))); compute.CodeOf(err) != compute.CodeNotFound {
		f.t.Fatalf("failed create left an operation: %v", err)
	}
	// Another volume is fine.
	f.mustCreateNodeDriven("req-3", "web-3", "other", t0)
}

func nodeDrivenNotUnconverged(f fixture) {
	f.mustCreateNodeDriven("req-1", "web-1", "root", t0)
	f.mustCommit("req-2", runIntent("vws.small"), instanceSpec(1, 1, compute.PowerRunning), true)
	for name, list := range map[string]func() ([]compute.Ref, error){
		"Unconverged": func() ([]compute.Ref, error) { return f.s.Unconverged(context.Background(), t1) },
		"Lagging":     func() ([]compute.Ref, error) { return f.s.Lagging(context.Background()) },
	} {
		refs, err := list()
		if err != nil || len(refs) != 1 || refs[0].ID != instanceID(1).String() {
			f.t.Fatalf("%s = %v, %v; want only the reconciler's instance", name, refs, err)
		}
	}
}

func createPlaces(f fixture) {
	f.addNode("req-a", "a", 1)
	// Never synced: not eligible.
	f.mustCreateNodeDriven("req-1", "web-1", "v1", t0)
	if got := f.instance("web-1").Status.AssignedNode; got != "" {
		f.t.Fatalf("placed on an unsynced node: %q", got)
	}
	// The sync places it. A later create sees the node full.
	if got := ids(f.sync("a", compute.NodeReady, t0)); len(got) != 1 || got[0] != "web-1" {
		f.t.Fatalf("sync returned %v", got)
	}
	f.mustCreateNodeDriven("req-2", "web-2", "v2", t0.Add(time.Second))
	if got := f.instance("web-2").Status.AssignedNode; got != "" {
		f.t.Fatalf("placed on a full node: %q", got)
	}
	// A second node with a fresh heartbeat takes the next create at commit.
	f.addNode("req-b", "b", 2)
	f.sync("b", compute.NodeReconciling, t0.Add(10*time.Second))
	f.mustCreateNodeDriven("req-3", "web-3", "v3", t0.Add(20*time.Second))
	if got := f.instance("web-3").Status.AssignedNode; got != zonePrefix+"b" {
		f.t.Fatalf("web-3 assigned to %q, want node b", got)
	}
	// Placement ran for the whole zone, so web-2 got the other b slot too.
	if got := f.instance("web-2").Status.AssignedNode; got != zonePrefix+"b" {
		f.t.Fatalf("web-2 assigned to %q after b synced", got)
	}
}

func syncPlaces(f fixture) {
	f.addNode("req-a", "a", 2)
	for i, v := range []string{"v1", "v2", "v3"} {
		f.mustCreateNodeDriven(fmt.Sprintf("req-%d", i), fmt.Sprintf("web-%d", i+1), v, t0)
	}
	got := ids(f.sync("a", compute.NodeReady, t0.Add(time.Second)))
	if len(got) != 2 || got[0] != "web-1" || got[1] != "web-2" {
		f.t.Fatalf("first sync = %v, want web-1 and web-2", got)
	}
	if f.instance("web-3").Status.AssignedNode != "" {
		f.t.Fatal("web-3 was placed past the node's slots")
	}
	// A retry heartbeats again and returns the same assignment.
	again := ids(f.sync("a", compute.NodeReady, t0.Add(2*time.Second)))
	if len(again) != 2 || again[0] != "web-1" || again[1] != "web-2" {
		f.t.Fatalf("second sync = %v", again)
	}
	n := ok(f.s.Node(context.Background(), nodeName("a")))
	if !n.Status.LastSync.Equal(t0.Add(2*time.Second)) || n.Status.State != compute.NodeReady {
		f.t.Fatalf("node status = %+v", n.Status)
	}
	// A node in ERROR heartbeats but takes nothing new, and still gets what it has.
	f.addNode("req-b", "b", 5)
	if got := ids(f.sync("b", compute.NodeError, t0.Add(3*time.Second))); len(got) != 0 {
		f.t.Fatalf("error node was given %v", got)
	}
	if got := ids(f.sync("a", compute.NodeError, t0.Add(4*time.Second))); len(got) != 2 {
		f.t.Fatalf("error node lost its assignments: %v", got)
	}
	// The agent version is kept when a heartbeat leaves it empty.
	_, _, err := f.s.SyncNode(context.Background(), nodeName("a"), compute.Heartbeat{State: compute.NodeReady, AgentVersion: "v1"}, t0.Add(5*time.Second),
		func(compute.Node, []compute.Instance) error { return nil })
	if err != nil {
		f.t.Fatal(err)
	}
	f.sync("a", compute.NodeReady, t0.Add(6*time.Second))
	if got := ok(f.s.Node(context.Background(), nodeName("a"))).Status.AgentVersion; got != "v1" {
		f.t.Fatalf("agent version = %q", got)
	}
	_, _, err = f.s.SyncNode(context.Background(), nodeName("ghost"), compute.Heartbeat{State: compute.NodeReady}, t0,
		func(compute.Node, []compute.Instance) error { return nil })
	wantCode(f.t, err, compute.CodeNotFound)
}

func syncAccept(f fixture) {
	f.addNode("req-a", "a", 2)
	f.mustCreateNodeDriven("req-1", "web-1", "v1", t0)
	boom := errors.New("response too large")
	var seen []compute.Instance
	_, _, err := f.s.SyncNode(context.Background(), nodeName("a"), compute.Heartbeat{State: compute.NodeReady}, t0,
		func(n compute.Node, insts []compute.Instance) error {
			seen = insts
			if n.Status.LastSync.IsZero() {
				f.t.Error("accept saw a node without the heartbeat")
			}
			return boom
		})
	if !errors.Is(err, boom) {
		f.t.Fatalf("err = %v", err)
	}
	if len(seen) != 1 || seen[0].Status.AssignedNode != zonePrefix+"a" {
		f.t.Fatalf("accept saw %v", ids(seen))
	}
	if n := ok(f.s.Node(context.Background(), nodeName("a"))); !n.Status.LastSync.IsZero() {
		f.t.Fatalf("a refused sync wrote a heartbeat: %+v", n.Status)
	}
	if got := f.instance("web-1").Status.AssignedNode; got != "" {
		f.t.Fatalf("a refused sync placed an instance on %q", got)
	}
}

func reportSettles(f fixture) {
	f.addNode("req-a", "a", 2)
	f.sync("a", compute.NodeReady, t0)
	op := f.mustCreateNodeDriven("req-1", "web-1", "v1", t0.Add(time.Second))
	f.wantState(op, compute.OperationPending)
	node := zonePrefix + "a"
	if got := f.instance("web-1").Status.AssignedNode; got != node {
		f.t.Fatalf("assigned %q", got)
	}
	ready := compute.InstanceReport{Node: node, Generation: 1, State: compute.ReportReady, Power: compute.PowerRunning,
		Interfaces: []compute.ObservedInterface{{DeviceName: "eth0", PrivateIPv4: "10.0.0.5"}}}

	if _, err := f.report("web-1", compute.InstanceReport{Node: zonePrefix + "b", Generation: 1, State: compute.ReportReady}, t1); compute.CodeOf(err) != compute.CodeInvalid {
		f.t.Fatalf("report from another node: %v", err)
	}
	if _, err := f.report("web-1", compute.InstanceReport{Node: node, Generation: 2, State: compute.ReportReady}, t1); compute.CodeOf(err) != compute.CodeAborted {
		f.t.Fatalf("report ahead of spec: %v", err)
	}
	f.wantState(op, compute.OperationPending)

	got, err := f.report("web-1", ready, t1)
	if err != nil {
		f.t.Fatal(err)
	}
	if got.Status.Observed.Generation != 1 || got.Status.AssignedNode != node || len(got.Status.Report.Interfaces) != 1 {
		f.t.Fatalf("after report = %+v", got.Status)
	}
	f.wantState(op, compute.OperationSucceeded)

	// A stale report is a no-op and a repeat is harmless.
	if _, err := f.report("web-1", ready, t1.Add(time.Second)); err != nil {
		f.t.Fatal(err)
	}
	// The reconciler's status commit does not clobber placement or the report.
	if err := f.s.CommitInstanceStatus(context.Background(), ok(compute.ParseInstanceID("web-1")),
		compute.InstanceStatus{Observed: compute.Observed{Generation: 1}, UpdatedAt: t1.Add(time.Minute)}); err != nil {
		f.t.Fatal(err)
	}
	after := f.instance("web-1")
	if after.Status.AssignedNode != node || after.Status.Report.State != compute.ReportReady {
		f.t.Fatalf("status commit clobbered node fields: %+v", after.Status)
	}
	_, err = f.report("nope", ready, t1)
	wantCode(f.t, err, compute.CodeNotFound)
}

func deleteFlows(f fixture) {
	f.addNode("req-a", "a", 1)
	// web-1 never gets a node.
	f.mustCreateNodeDriven("req-1", "web-1", "v1", t0)
	term := nodeDrivenSpec("web-1", "v1", 2, compute.PowerTerminated)
	term.ChangedAt = t1
	op := f.mustCommit("req-del-1", ok(compute.NewTerminateInstance("web-1")), term, true)
	f.wantState(op, compute.OperationSucceeded)
	if !f.instance("web-1").Removed() {
		f.t.Fatal("an unassigned delete is not removed")
	}
	// Its volume is free again, and it did not take a slot.
	f.sync("a", compute.NodeReady, t0)
	f.mustCreateNodeDriven("req-2", "web-2", "v1", t0)
	if got := f.instance("web-2").Status.AssignedNode; got != zonePrefix+"a" {
		f.t.Fatalf("web-2 assigned to %q", got)
	}
	// An assigned delete waits for the node.
	term2 := nodeDrivenSpec("web-2", "v1", 2, compute.PowerTerminated)
	term2.ChangedAt = t1
	op2 := f.mustCommit("req-del-2", ok(compute.NewTerminateInstance("web-2")), term2, true)
	f.wantState(op2, compute.OperationPending)
	if got := ids(f.sync("a", compute.NodeReady, t0.Add(time.Second))); len(got) != 1 || got[0] != "web-2" {
		f.t.Fatalf("a deleting instance must still be synced: %v", got)
	}
	f.mustCreateNodeDriven("req-3", "web-3", "v3", t0.Add(2*time.Second))
	if got := f.instance("web-3").Status.AssignedNode; got != "" {
		f.t.Fatalf("a deleting instance gave up its slot early: %q", got)
	}
	del := compute.InstanceReport{Node: zonePrefix + "a", Generation: 2, State: compute.ReportDeleting}
	if _, err := f.report("web-2", del, t1); err != nil {
		f.t.Fatal(err)
	}
	f.wantState(op2, compute.OperationSucceeded)
	if got := ids(f.sync("a", compute.NodeReady, t0.Add(3*time.Second))); len(got) != 1 || got[0] != "web-3" {
		f.t.Fatalf("after removal the node holds %v", got)
	}
}
