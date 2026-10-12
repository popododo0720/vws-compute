package kubestore

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// Repaired counts what one Repair pass changed or found.
type Repaired struct {
	// OrphanIndexes are request-id indexes deleted because the resource never
	// took their operation.
	OrphanIndexes int
	// RestoredIndexes are indexes recreated for operations a resource holds.
	RestoredIndexes int
	// StaleClaims are key name and volume claims deleted because no live
	// holder stands behind them.
	StaleClaims int
	// Pending is the number of operations still in flight. The reconciler and
	// the controller report path resume them. They need no repair.
	Pending int
}

// Repair converges the two-write gaps a crash can leave. It is idempotent.
// Run it once at startup, before serving. It is not safe against a second
// process that is committing at the same moment.
func (s *Store) Repair(ctx context.Context) (Repaired, error) {
	var r Repaired
	var all []*entry
	for _, k := range []compute.Kind{compute.KindInstance, compute.KindImage, compute.KindKeyPair} {
		es, err := s.list(ctx, k)
		if err != nil {
			return r, err
		}
		all = append(all, es...)
	}
	byRef := map[compute.Ref]*entry{}
	for _, e := range all {
		byRef[e.ref] = e
	}

	// Indexes whose operation the resource does not hold.
	var idxs corev1.ConfigMapList
	if err := s.c.List(ctx, &idxs, client.InNamespace(s.ns), client.MatchingLabels{labelKind: kindRequest}); err != nil {
		return r, fmt.Errorf("list request indexes: %w", err)
	}
	have := map[compute.RequestID]bool{}
	for i := range idxs.Items {
		x, err := decodeIndex(&idxs.Items[i])
		if err != nil {
			return r, err
		}
		have[x.request] = true
		holds := false
		if e := byRef[x.target]; e != nil {
			_, holds = e.op(func(o compute.Operation) bool { return o.Request == x.request })
		}
		if holds {
			continue
		}
		if err := s.c.Delete(ctx, &idxs.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return r, fmt.Errorf("delete orphaned index %s: %w", idxs.Items[i].Name, err)
		}
		r.OrphanIndexes++
	}

	// Operations without an index, and the pending count.
	for _, e := range all {
		for _, op := range e.ops {
			if !op.State().Terminal() {
				r.Pending++
			}
			if have[op.Request] {
				continue
			}
			if err := s.claimIndex(ctx, indexOf(op)); err != nil && !isCAS(err) {
				return r, err
			}
			r.RestoredIndexes++
		}
	}

	// Claims whose holder is missing or no longer qualifies.
	var claims corev1.ConfigMapList
	for _, k := range []string{kindClaimKey, kindClaimVolume} {
		if err := s.c.List(ctx, &claims, client.InNamespace(s.ns), client.MatchingLabels{labelKind: k}); err != nil {
			return r, fmt.Errorf("list %s claims: %w", k, err)
		}
		for i := range claims.Items {
			cm := &claims.Items[i]
			holder, subject := cm.Data["holder"], cm.Data["subject"]
			var live bool
			switch k {
			case kindClaimKey:
				e := byRef[compute.Ref{Kind: compute.KindKeyPair, ID: holder}]
				live = e != nil && !e.key.Spec.Deleted && e.key.Spec.Name.String() == subject
			default:
				e := byRef[compute.Ref{Kind: compute.KindInstance, ID: holder}]
				live = e != nil && e.inst.Spec.Power != compute.PowerTerminated && holdsVolume(e.inst.Spec, subject)
			}
			if live {
				continue
			}
			if err := s.c.Delete(ctx, cm); err != nil && !apierrors.IsNotFound(err) {
				return r, fmt.Errorf("delete stale claim %s: %w", cm.Name, err)
			}
			r.StaleClaims++
		}
	}
	return r, nil
}

func holdsVolume(spec compute.InstanceSpec, volume string) bool {
	for _, v := range spec.Volumes {
		if v.Volume.String() == volume {
			return true
		}
	}
	return false
}
