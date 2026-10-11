package compute_test

import (
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
)

const zoneNodes = "projects/p/zones/z/nodes/"

func testNode(name string, slots int, state compute.NodeState, lastSync time.Time) compute.Node {
	return compute.Node{
		Spec:   compute.NodeSpec{Name: ok(compute.ParseNodeName(zoneNodes + name)), Generation: 1, Slots: slots},
		Status: compute.NodeStatus{State: state, LastSync: lastSync},
	}
}

func TestParseNodeName(t *testing.T) {
	n := ok(compute.ParseNodeName("projects/p/zones/z/nodes/n1"))
	if n.Project() != "p" || n.Zone() != "z" {
		t.Fatalf("project %q zone %q", n.Project(), n.Zone())
	}
	for _, bad := range []string{"", "projects/p/zones/z/nodes", "projects/p/regions/z/nodes/n", "projects/P/zones/z/nodes/n"} {
		if _, err := compute.ParseNodeName(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}

func TestChooseNode(t *testing.T) {
	now := t0.Add(time.Hour)
	fresh := now.Add(-10 * time.Second)
	cases := map[string]struct {
		nodes []compute.NodeLoad
		want  string
	}{
		"most free slots wins": {
			nodes: []compute.NodeLoad{
				{Node: testNode("a", 4, compute.NodeReady, fresh), Assigned: 3},
				{Node: testNode("b", 4, compute.NodeReady, fresh), Assigned: 1},
			},
			want: "b",
		},
		"tie goes to the smallest name": {
			nodes: []compute.NodeLoad{
				{Node: testNode("b", 2, compute.NodeReady, fresh)},
				{Node: testNode("a", 2, compute.NodeReconciling, fresh)},
			},
			want: "a",
		},
		"stale heartbeat is skipped": {
			nodes: []compute.NodeLoad{
				{Node: testNode("a", 9, compute.NodeReady, now.Add(-31*time.Second))},
				{Node: testNode("b", 1, compute.NodeReady, now.Add(-30*time.Second))},
			},
			want: "b",
		},
		"error node is skipped": {
			nodes: []compute.NodeLoad{
				{Node: testNode("a", 9, compute.NodeError, fresh)},
				{Node: testNode("b", 1, compute.NodeReady, fresh)},
			},
			want: "b",
		},
		"never synced is skipped": {
			nodes: []compute.NodeLoad{{Node: testNode("a", 9, compute.NodeUnreported, time.Time{})}},
		},
		"a full node is skipped": {
			nodes: []compute.NodeLoad{{Node: testNode("a", 2, compute.NodeReady, fresh), Assigned: 2}},
		},
		"zero slots accepts nothing": {
			nodes: []compute.NodeLoad{{Node: testNode("a", 0, compute.NodeReady, fresh)}},
		},
		"another zone is skipped": {
			nodes: []compute.NodeLoad{{Node: compute.Node{
				Spec:   compute.NodeSpec{Name: ok(compute.ParseNodeName("projects/p/zones/other/nodes/a")), Generation: 1, Slots: 5},
				Status: compute.NodeStatus{State: compute.NodeReady, LastSync: fresh},
			}}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, found := compute.ChooseNode("p", "z", tc.nodes, now)
			if found != (tc.want != "") || (found && got.String() != zoneNodes+tc.want) {
				t.Fatalf("got %q, %v; want %q", got, found, tc.want)
			}
		})
	}
}

func TestPlanPlacementCountsEachDecision(t *testing.T) {
	now := t0
	nodes := []compute.NodeLoad{
		{Node: testNode("a", 2, compute.NodeReady, now)},
		{Node: testNode("b", 1, compute.NodeReady, now)},
	}
	ids := []compute.InstanceID{
		ok(compute.ParseInstanceID("i1")), ok(compute.ParseInstanceID("i2")),
		ok(compute.ParseInstanceID("i3")), ok(compute.ParseInstanceID("i4")),
	}
	got := compute.PlanPlacement("p", "z", nodes, ids, now)
	want := []string{"a", "a", "b"}
	if len(got) != len(want) {
		t.Fatalf("placed %d, want %d: %v", len(got), len(want), got)
	}
	for i, p := range got {
		if p.Node.String() != zoneNodes+want[i] || p.Instance != ids[i] {
			t.Errorf("placement %d = %v", i, p)
		}
	}
}

func TestCreateNodeIsIdempotent(t *testing.T) {
	in := ok(compute.NewCreateNode(zoneNodes+"a", 4, compute.Labels{}))
	ch, err := compute.Plan(newView(), envNow(), in)
	if err != nil || !ch.Write || ch.Spec.Gen() != 1 {
		t.Fatalf("first create: %+v, %v", ch, err)
	}
	spec := ch.Spec.(compute.NodeSpec)
	if spec.UID == "" || spec.Slots != 4 {
		t.Fatalf("spec %+v", spec)
	}
	v := newView().with(compute.Node{Spec: spec})
	ch, err = compute.Plan(v, envNow(), in)
	if err != nil || ch.Write {
		t.Fatalf("same create: %+v, %v", ch, err)
	}
	other := ok(compute.NewCreateNode(zoneNodes+"a", 5, compute.Labels{}))
	if _, err := compute.Plan(v, envNow(), other); compute.CodeOf(err) != compute.CodeAlreadyExists {
		t.Fatalf("different create: %v", err)
	}
	for _, slots := range []int{-1, compute.MaxNodeSlots + 1} {
		if _, err := compute.NewCreateNode(zoneNodes+"a", slots, compute.Labels{}); err == nil {
			t.Errorf("slots %d accepted", slots)
		}
	}
}

func nodeDriven(power compute.Power) compute.Instance {
	return compute.Instance{Spec: compute.InstanceSpec{
		ID:         ok(compute.ParseInstanceID("web-1")),
		Generation: 2,
		Project:    "p",
		Zone:       "z",
		Power:      power,
	}}
}

func TestApplyReport(t *testing.T) {
	node := zoneNodes + "a"
	now := t0
	t.Run("ready at the spec generation converges", func(t *testing.T) {
		st, changed, err := compute.ApplyReport(nodeDriven(compute.PowerRunning),
			compute.InstanceReport{Node: node, Generation: 2, State: compute.ReportReady, Power: compute.PowerRunning}, now)
		if err != nil || !changed || st.Observed.Generation != 2 || st.Observed.Failure != nil {
			t.Fatalf("%+v %v %v", st, changed, err)
		}
	})
	t.Run("reconciling records the report and does not advance", func(t *testing.T) {
		st, changed, err := compute.ApplyReport(nodeDriven(compute.PowerRunning),
			compute.InstanceReport{Node: node, Generation: 2, State: compute.ReportReconciling, Message: "booting"}, now)
		if err != nil || !changed || st.Observed.Generation != 0 || st.Report.Message != "booting" {
			t.Fatalf("%+v %v %v", st, changed, err)
		}
	})
	t.Run("error parks the generation", func(t *testing.T) {
		st, _, err := compute.ApplyReport(nodeDriven(compute.PowerRunning),
			compute.InstanceReport{Node: node, Generation: 2, State: compute.ReportError, Message: "no disk"}, now)
		if err != nil || !st.Observed.Parked(2) || st.Observed.Failure.Message != "no disk" {
			t.Fatalf("%+v %v", st, err)
		}
	})
	t.Run("terminated converges only on DELETING", func(t *testing.T) {
		inst := nodeDriven(compute.PowerTerminated)
		st, _, _ := compute.ApplyReport(inst, compute.InstanceReport{Node: node, Generation: 2, State: compute.ReportReady}, now)
		if st.Observed.Generation != 0 {
			t.Fatalf("READY converged a delete: %+v", st)
		}
		st, _, _ = compute.ApplyReport(inst, compute.InstanceReport{Node: node, Generation: 2, State: compute.ReportDeleting}, now)
		inst.Status = st
		if st.Observed.Generation != 2 || !inst.Removed() {
			t.Fatalf("DELETING did not remove: %+v", st)
		}
	})
	t.Run("a report ahead of the spec is aborted", func(t *testing.T) {
		_, _, err := compute.ApplyReport(nodeDriven(compute.PowerRunning),
			compute.InstanceReport{Node: node, Generation: 3, State: compute.ReportReady}, now)
		if compute.CodeOf(err) != compute.CodeAborted {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("an assigned instance takes reports only from its node", func(t *testing.T) {
		inst := nodeDriven(compute.PowerRunning)
		inst.Status.AssignedNode = node
		for _, from := range []string{"", zoneNodes + "b"} {
			_, _, err := compute.ApplyReport(inst, compute.InstanceReport{Node: from, Generation: 2, State: compute.ReportReady}, now)
			if compute.CodeOf(err) != compute.CodeInvalid {
				t.Errorf("node %q: got %v", from, err)
			}
		}
		st, _, err := compute.ApplyReport(inst, compute.InstanceReport{Node: node, Generation: 2, State: compute.ReportReady}, now)
		if err != nil || st.AssignedNode != node {
			t.Fatalf("%+v %v", st, err)
		}
	})
	t.Run("a stale report changes nothing", func(t *testing.T) {
		inst := nodeDriven(compute.PowerRunning)
		inst.Status.Observed.Generation = 2
		_, changed, err := compute.ApplyReport(inst, compute.InstanceReport{Node: node, Generation: 1, State: compute.ReportReady}, now)
		if err != nil || changed {
			t.Fatalf("changed %v err %v", changed, err)
		}
	})
	t.Run("a missing state is invalid", func(t *testing.T) {
		_, _, err := compute.ApplyReport(nodeDriven(compute.PowerRunning), compute.InstanceReport{Node: node, Generation: 2}, now)
		if compute.CodeOf(err) != compute.CodeInvalid {
			t.Fatalf("got %v", err)
		}
	})
}

func createParams() compute.CreateInstanceParams {
	return compute.CreateInstanceParams{
		ID: "web-1", Project: "p", Zone: "z", MachineType: "n1.medium", Power: compute.PowerRunning,
		Volumes: []compute.VolumeAttachment{{Volume: ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/root")), DeviceName: "vda", Boot: true}},
		NICs:    []compute.NetworkInterface{{DeviceName: "eth0", Subnet: "projects/p/regions/r/vpcs/v/subnets/s"}},
	}
}

func TestCreateInstanceValidation(t *testing.T) {
	mutate := map[string]func(*compute.CreateInstanceParams){
		"no boot volume": func(p *compute.CreateInstanceParams) { p.Volumes[0].Boot = false },
		"two boot volumes": func(p *compute.CreateInstanceParams) {
			p.Volumes = append(p.Volumes, p.Volumes[0])
			p.Volumes[1].DeviceName = "vdb"
		},
		"duplicate device": func(p *compute.CreateInstanceParams) {
			p.Volumes = append(p.Volumes, p.Volumes[0])
			p.Volumes[1].Boot = false
		},
		"volume in another zone": func(p *compute.CreateInstanceParams) {
			p.Volumes[0].Volume = ok(compute.ParseVolumeRef("projects/p/zones/other/volumes/root"))
		},
		"power unspecified":  func(p *compute.CreateInstanceParams) { p.Power = 0 },
		"power terminated":   func(p *compute.CreateInstanceParams) { p.Power = compute.PowerTerminated },
		"bad machine type":   func(p *compute.CreateInstanceParams) { p.MachineType = "N1" },
		"duplicate nic":      func(p *compute.CreateInstanceParams) { p.NICs = append(p.NICs, p.NICs[0]) },
		"user data too long": func(p *compute.CreateInstanceParams) { p.UserData = string(make([]byte, compute.MaxUserDataBytes+1)) },
	}
	for name, m := range mutate {
		t.Run(name, func(t *testing.T) {
			p := createParams()
			m(&p)
			if _, err := compute.NewCreateInstance(p); compute.CodeOf(err) != compute.CodeInvalid {
				t.Fatalf("got %v", err)
			}
		})
	}
	if _, err := compute.NewCreateInstance(createParams()); err != nil {
		t.Fatal(err)
	}
}

func TestCreateInstancePlan(t *testing.T) {
	in := ok(compute.NewCreateInstance(createParams()))
	ch, err := compute.Plan(newView(), envNow(), in)
	if err != nil || !ch.Write {
		t.Fatalf("%+v %v", ch, err)
	}
	spec := ch.Spec.(compute.InstanceSpec)
	if spec.Generation != 1 || !spec.NodeDriven() || spec.Image.String() != "" || spec.Type.Name() != "n1.medium" {
		t.Fatalf("spec %+v", spec)
	}
	existing := compute.Instance{Spec: spec}
	v := newView().with(existing)

	t.Run("same request on a live instance changes nothing", func(t *testing.T) {
		ch, err := compute.Plan(v, envNow(), in)
		if err != nil || ch.Write || ch.Spec.Gen() != 1 {
			t.Fatalf("%+v %v", ch, err)
		}
	})
	t.Run("a different spec is already exists", func(t *testing.T) {
		p := createParams()
		p.UserData = "#!/bin/sh"
		_, err := compute.Plan(v, envNow(), ok(compute.NewCreateInstance(p)))
		if compute.CodeOf(err) != compute.CodeAlreadyExists {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("a terminated instance keeps its id", func(t *testing.T) {
		gone := existing
		gone.Spec.Power = compute.PowerTerminated
		_, err := compute.Plan(newView().with(gone), envNow(), in)
		if compute.CodeOf(err) != compute.CodeAlreadyExists {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("fingerprint tracks every field", func(t *testing.T) {
		base := compute.FingerprintOf(in)
		p := createParams()
		p.NICs[0].SecurityGroups = []string{"projects/p/regions/r/vpcs/v/securityGroups/sg"}
		if compute.FingerprintOf(ok(compute.NewCreateInstance(p))) == base {
			t.Fatal("security group did not change the fingerprint")
		}
		if compute.FingerprintOf(ok(compute.NewCreateInstance(createParams()))) != base {
			t.Fatal("fingerprint is not deterministic")
		}
	})
}
