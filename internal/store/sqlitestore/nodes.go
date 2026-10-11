package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

const nodeColumns = `id, uid, generation, created_at, changed_at, slots, state, message, last_sync, agent_version, ` + metaColumns

// nodeRow mirrors one row of the nodes table.
type nodeRow struct {
	id, uid                                 string
	generation, createdAt, changedAt, slots int64
	state, lastSync                         int64
	message, agentVersion                   string
	meta                                    metaRow
}

func scanNode(sc scanner) (compute.Node, error) {
	var r nodeRow
	err := sc.Scan(append([]any{&r.id, &r.uid, &r.generation, &r.createdAt, &r.changedAt, &r.slots, &r.state,
		&r.message, &r.lastSync, &r.agentVersion}, r.meta.dest()...)...)
	if err != nil {
		return compute.Node{}, err
	}
	name, err1 := compute.ParseNodeName(r.id)
	err2 := inRange("node state", r.state, int64(compute.NodeUnreported), int64(compute.NodeError))
	meta, err3 := r.meta.metadata()
	if err := errors.Join(err1, err2, err3); err != nil {
		return compute.Node{}, corrupt("node", r.id, err)
	}
	return compute.Node{
		Spec: compute.NodeSpec{
			Name: name, UID: r.uid, Generation: r.generation,
			CreatedAt: fromNanos(r.createdAt), ChangedAt: fromNanos(r.changedAt), Slots: int(r.slots),
		},
		Status: compute.NodeStatus{
			State: compute.NodeState(r.state), Message: r.message, LastSync: fromNanos(r.lastSync), AgentVersion: r.agentVersion,
		},
		Meta: meta,
	}, nil
}

func (s *Store) Node(ctx context.Context, name compute.NodeName) (compute.Node, error) {
	return getNode(ctx, s.db, name)
}

func getNode(ctx context.Context, q querier, name compute.NodeName) (compute.Node, error) {
	n, err := scanNode(q.QueryRowContext(ctx, `SELECT `+nodeColumns+` FROM nodes WHERE id = ?`, name.String()))
	return n, notFound(err, "node %s not found", name)
}

func (s *Store) Nodes(ctx context.Context, q store.NodeQuery) (store.Page[compute.Node], error) {
	where := ""
	if q.Prefix != "" {
		// Prefixes are resource-name collections, which contain no LIKE
		// wildcard characters.
		where = `substr(id, 1, ` + strconv.Itoa(len(q.Prefix)) + `) = '` + strings.ReplaceAll(q.Prefix, `'`, `''`) + `'`
	}
	return list(ctx, s.db, "nodes", nodeColumns, "id", where, nil, q.After, q.PageSize, scanNode,
		func(n compute.Node) string { return n.Spec.Name.String() })
}

// claimVolumes records s's volumes as held by s, failing when a live
// instance already holds one.
func claimVolumes(ctx context.Context, tx *sql.Tx, s compute.InstanceSpec) error {
	for _, v := range s.Volumes {
		var holder string
		err := tx.QueryRowContext(ctx, `SELECT instance_id FROM instance_volumes WHERE volume = ?`, v.Volume.String()).Scan(&holder)
		switch {
		case err == nil:
			return compute.Errorf(compute.CodeFailedPrecondition, "volume %s is already attached to instance %s", v.Volume, holder)
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO instance_volumes (volume, instance_id) VALUES (?, ?)`,
			v.Volume.String(), s.ID.String()); err != nil {
			return err
		}
	}
	return nil
}

// afterNodeDrivenWrite runs inside CommitDesired, after the spec is stored.
func afterNodeDrivenWrite(ctx context.Context, tx *sql.Tx, spec compute.InstanceSpec, now time.Time) error {
	var assigned string
	if err := tx.QueryRowContext(ctx, `SELECT assigned_node FROM instances WHERE id = ?`, spec.ID.String()).Scan(&assigned); err != nil {
		return err
	}
	if spec.Power == compute.PowerTerminated && assigned == "" {
		_, err := tx.ExecContext(ctx, `UPDATE instances SET observed_generation = ?, failure_code = NULL,
			failure_message = NULL, updated_at = ? WHERE id = ?`, spec.Generation, nanos(now), spec.ID.String())
		return err
	}
	ps, err := planPlacement(ctx, tx, spec.Project, spec.Zone, compute.Node{}, now)
	if err != nil {
		return err
	}
	return applyPlacement(ctx, tx, ps)
}

// planPlacement decides for every unassigned live instance in project and
// zone, in ID order. over is the node as a pending heartbeat would leave it.
func planPlacement(ctx context.Context, q querier, project, zone string, over compute.Node, now time.Time) ([]compute.Placement, error) {
	prefix := "projects/" + project + "/zones/" + zone + "/nodes/"
	nodes, err := collect(ctx, q, scanNode, `SELECT `+nodeColumns+` FROM nodes WHERE substr(id, 1, ?) = ? ORDER BY id`, len(prefix), prefix)
	if err != nil {
		return nil, err
	}
	loads := make([]compute.NodeLoad, 0, len(nodes))
	for _, n := range nodes {
		if !over.Spec.Name.IsZero() && over.Spec.Name == n.Spec.Name {
			n = over
		}
		var assigned int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM instances WHERE assigned_node = ?
			AND NOT (power = ? AND observed_generation = generation)`,
			n.Spec.Name.String(), int64(compute.PowerTerminated)).Scan(&assigned); err != nil {
			return nil, err
		}
		loads = append(loads, compute.NodeLoad{Node: n, Assigned: assigned})
	}
	rawIDs, err := collect(ctx, q, func(sc scanner) (string, error) {
		var id string
		return id, sc.Scan(&id)
	}, `SELECT id FROM instances WHERE node_driven = 1 AND project = ? AND zone = ? AND assigned_node = '' AND power <> ?
		ORDER BY id`, project, zone, int64(compute.PowerTerminated))
	if err != nil {
		return nil, err
	}
	ids := make([]compute.InstanceID, 0, len(rawIDs))
	for _, raw := range rawIDs {
		id, err := compute.ParseInstanceID(raw)
		if err != nil {
			return nil, corrupt("instance", raw, err)
		}
		ids = append(ids, id)
	}
	return compute.PlanPlacement(project, zone, loads, ids, now), nil
}

// applyPlacement writes each decision by compare-and-set while the instance
// has no node. It does not advance the generation or touch an operation.
func applyPlacement(ctx context.Context, tx *sql.Tx, ps []compute.Placement) error {
	for _, p := range ps {
		if _, err := tx.ExecContext(ctx, `UPDATE instances SET assigned_node = ? WHERE id = ? AND assigned_node = ''`,
			p.Node.String(), p.Instance.String()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) SyncNode(ctx context.Context, name compute.NodeName, hb compute.Heartbeat, now time.Time,
	accept func(compute.Node, []compute.Instance) error,
) (compute.Node, []compute.Instance, error) {
	var (
		next compute.Node
		mine []compute.Instance
	)
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		cur, err := getNode(ctx, tx, name)
		if err != nil {
			return err
		}
		next = cur.Apply(hb, now)
		placed, err := planPlacement(ctx, tx, name.Project(), name.Zone(), next, now)
		if err != nil {
			return err
		}
		insts, err := collect(ctx, tx, scanInstance, `SELECT `+instanceColumns+` FROM instances
			WHERE assigned_node = ? ORDER BY id`, name.String())
		if err != nil {
			return err
		}
		for _, in := range insts {
			if !in.Removed() {
				mine = append(mine, in)
			}
		}
		for _, p := range placed {
			if p.Node != name {
				continue
			}
			in, err := scanInstance(tx.QueryRowContext(ctx, `SELECT `+instanceColumns+` FROM instances WHERE id = ?`, p.Instance.String()))
			if err != nil {
				return err
			}
			in.Status.AssignedNode = name.String()
			mine = append(mine, in)
		}
		sortInstances(mine)
		if err := accept(next, mine); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE nodes SET state = ?, message = ?, last_sync = ?, agent_version = ? WHERE id = ?`,
			int64(next.Status.State), next.Status.Message, nanos(now), next.Status.AgentVersion, name.String()); err != nil {
			return err
		}
		return applyPlacement(ctx, tx, placed)
	})
	if err != nil {
		return compute.Node{}, nil, err
	}
	return next, mine, nil
}

func sortInstances(in []compute.Instance) {
	slices.SortFunc(in, func(a, b compute.Instance) int { return strings.Compare(a.Spec.ID.String(), b.Spec.ID.String()) })
}

func (s *Store) ReportInstanceStatus(ctx context.Context, id compute.InstanceID, rep compute.InstanceReport, now time.Time) (compute.Instance, error) {
	var out compute.Instance
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		cur, err := scanInstance(tx.QueryRowContext(ctx, `SELECT `+instanceColumns+` FROM instances WHERE id = ?`, id.String()))
		if err != nil {
			return notFound(err, "instance %s not found", id)
		}
		next, changed, err := compute.ApplyReport(cur, rep, now)
		if err != nil {
			return err
		}
		out = cur
		if !changed {
			return nil
		}
		if err := store.CheckObservation(next.Observed, cur.Status.Observed, cur.Spec.Generation, now); err != nil {
			return err
		}
		report, err := reportToJSON(next.Report)
		if err != nil {
			return err
		}
		code, msg := failureArgs(next.Observed.Failure)
		if _, err := tx.ExecContext(ctx, `UPDATE instances SET observed_generation = ?, failure_code = ?,
			failure_message = ?, report = ?, updated_at = ? WHERE id = ?`,
			next.Observed.Generation, code, msg, report, nanos(now), id.String()); err != nil {
			return err
		}
		out.Status = next
		return settlePending(ctx, tx, id.Ref(), cur.Spec.Generation, next.Observed, now)
	})
	return out, err
}
