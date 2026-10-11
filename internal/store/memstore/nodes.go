package memstore

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

func (r *nodeRow) read() compute.Node {
	return compute.Node{Spec: r.spec, Status: r.status, Meta: r.meta}
}

func (s *Store) Node(_ context.Context, name compute.NodeName) (compute.Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.nodes[name.String()]
	if !ok {
		return compute.Node{}, compute.Errorf(compute.CodeNotFound, "node %s not found", name)
	}
	return row.read(), nil
}

func (s *Store) Nodes(_ context.Context, q store.NodeQuery) (store.Page[compute.Node], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	matched := map[string]*nodeRow{}
	for name, row := range s.nodes {
		if strings.HasPrefix(name, q.Prefix) {
			matched[name] = row
		}
	}
	names := selectKeys(matched, nil, q.After)
	return page(names, q.PageSize, func(n string) compute.Node { return matched[n].read() }), nil
}

// checkVolumes fails when a live instance already holds one of spec's volumes.
func (s *Store) checkVolumes(spec compute.InstanceSpec) error {
	for id, row := range s.instances {
		if row.spec.Power == compute.PowerTerminated || id == spec.ID.String() {
			continue
		}
		for _, have := range row.spec.Volumes {
			for _, want := range spec.Volumes {
				if have.Volume == want.Volume {
					return compute.Errorf(compute.CodeFailedPrecondition, "volume %s is already attached to instance %s", want.Volume, id)
				}
			}
		}
	}
	return nil
}

// afterNodeDrivenWrite runs inside CommitDesired, after the spec is stored.
func (s *Store) afterNodeDrivenWrite(spec compute.InstanceSpec, now time.Time) {
	row := s.instances[spec.ID.String()]
	if spec.Power == compute.PowerTerminated && row.status.AssignedNode == "" {
		row.status.Observed = compute.Observed{Generation: spec.Generation}
		row.status.UpdatedAt = now
		return
	}
	s.applyPlacement(s.planPlacement(spec.Project, spec.Zone, compute.Node{}, now))
}

// loads counts, for every node in project and zone, the instances that
// occupy its slots: assigned to it and not yet removed. A delete in flight
// still counts. over replaces the stored node of the same name.
func (s *Store) loads(project, zone string, over compute.Node) []compute.NodeLoad {
	var out []compute.NodeLoad
	for name, row := range s.nodes {
		n := row.read()
		if !over.Spec.Name.IsZero() && over.Spec.Name.String() == name {
			n = over
		}
		if n.Spec.Name.Project() != project || n.Spec.Name.Zone() != zone {
			continue
		}
		load := compute.NodeLoad{Node: n}
		for _, ir := range s.instances {
			if ir.status.AssignedNode == name && !(compute.Instance{Spec: ir.spec, Status: ir.status}).Removed() {
				load.Assigned++
			}
		}
		out = append(out, load)
	}
	return out
}

// planPlacement decides for every unassigned live instance in project and
// zone, in ID order. over is the node as a pending heartbeat would leave it.
func (s *Store) planPlacement(project, zone string, over compute.Node, now time.Time) []compute.Placement {
	var ids []string
	for id, row := range s.instances {
		if row.spec.NodeDriven() && row.spec.Project == project && row.spec.Zone == zone &&
			row.status.AssignedNode == "" && row.spec.Power != compute.PowerTerminated {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	unassigned := make([]compute.InstanceID, 0, len(ids))
	for _, id := range ids {
		iid, _ := compute.ParseInstanceID(id)
		unassigned = append(unassigned, iid)
	}
	return compute.PlanPlacement(project, zone, s.loads(project, zone, over), unassigned, now)
}

// applyPlacement writes each decision by compare-and-set while the instance
// has no node. It does not advance the generation or touch an operation.
func (s *Store) applyPlacement(ps []compute.Placement) {
	for _, p := range ps {
		if row := s.instances[p.Instance.String()]; row != nil && row.status.AssignedNode == "" {
			row.status.AssignedNode = p.Node.String()
		}
	}
}

func (s *Store) SyncNode(_ context.Context, name compute.NodeName, hb compute.Heartbeat, now time.Time,
	accept func(compute.Node, []compute.Instance) error,
) (compute.Node, []compute.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.nodes[name.String()]
	if !ok {
		return compute.Node{}, nil, compute.Errorf(compute.CodeNotFound, "node %s not found", name)
	}
	next := row.read().Apply(hb, now)
	placed := s.planPlacement(name.Project(), name.Zone(), next, now)
	assigned := map[string]string{}
	for _, p := range placed {
		assigned[p.Instance.String()] = p.Node.String()
	}
	var mine []compute.Instance
	ids := make([]string, 0, len(s.instances))
	for id := range s.instances {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		inst := s.instances[id].read()
		if to, ok := assigned[id]; ok {
			inst.Status.AssignedNode = to
		}
		if inst.Status.AssignedNode == name.String() && !inst.Removed() {
			mine = append(mine, inst)
		}
	}
	if err := accept(next, mine); err != nil {
		return compute.Node{}, nil, err
	}
	row.status = next.Status
	s.applyPlacement(placed)
	return next, mine, nil
}

func (s *Store) ReportInstanceStatus(_ context.Context, id compute.InstanceID, rep compute.InstanceReport, now time.Time) (compute.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.instances[id.String()]
	if !ok {
		return compute.Instance{}, compute.Errorf(compute.CodeNotFound, "instance %s not found", id)
	}
	next, changed, err := compute.ApplyReport(row.read(), rep, now)
	if err != nil {
		return compute.Instance{}, err
	}
	if changed {
		if err := store.CheckObservation(next.Observed, row.status.Observed, row.spec.Generation, now); err != nil {
			return compute.Instance{}, err
		}
		row.status = cloneInstanceStatus(next)
		s.settlePending(id.Ref(), row.spec.Generation, row.status.Observed, now)
	}
	return row.read(), nil
}
