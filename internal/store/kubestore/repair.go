package kubestore

import (
	"context"
	"fmt"
	"time"

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
// It removes every orphaned index and stale claim, so run it only while no
// other process commits: once at startup of a single writer. Replicas
// repair through Lead instead.
func (s *Store) Repair(ctx context.Context) (Repaired, error) {
	return s.repair(ctx, time.Now().Add(time.Hour))
}

// dataWritten is the ConfigMap data key that holds when a request index or a
// claim was last written, RFC 3339 UTC.
const dataWritten = "written"

func writtenNow() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// writtenBefore reports whether cm was last written before cutoff. A
// ConfigMap written before the key existed falls back to its creation time.
func writtenBefore(cm *corev1.ConfigMap, cutoff time.Time) bool {
	at := cm.CreationTimestamp.Time
	if t, err := time.Parse(time.RFC3339Nano, cm.Data[dataWritten]); err == nil {
		at = t
	}
	return at.Before(cutoff)
}

// repair is Repair that leaves every index and claim written at or after
// cutoff: another replica may be committing it right now, and an index or a
// claim is written before the resource that holds it.
func (s *Store) repair(ctx context.Context, cutoff time.Time) (Repaired, error) {
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
		if holds || !writtenBefore(&idxs.Items[i], cutoff) {
			continue
		}
		if gone, err := s.deleteUnchanged(ctx, &idxs.Items[i]); err != nil {
			return r, fmt.Errorf("orphaned index: %w", err)
		} else if !gone {
			continue
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
			if live || !writtenBefore(cm, cutoff) {
				continue
			}
			if gone, err := s.deleteUnchanged(ctx, cm); err != nil {
				return r, fmt.Errorf("stale claim: %w", err)
			} else if !gone {
				continue
			}
			r.StaleClaims++
		}
	}
	return r, nil
}

// deleteUnchanged deletes cm only at the resourceVersion Repair read, so an
// index another replica repointed or a claim it took over since stays. It
// reports whether this call removed cm.
func (s *Store) deleteUnchanged(ctx context.Context, cm *corev1.ConfigMap) (bool, error) {
	rv := cm.ResourceVersion
	err := s.c.Delete(ctx, cm, client.Preconditions{ResourceVersion: &rv})
	switch {
	case err == nil:
		return true, nil
	case apierrors.IsNotFound(err), apierrors.IsConflict(err):
		return false, nil
	default:
		return false, fmt.Errorf("delete %s: %w", cm.Name, err)
	}
}

func holdsVolume(spec compute.InstanceSpec, volume string) bool {
	for _, v := range spec.Volumes {
		if v.Volume.String() == volume {
			return true
		}
	}
	return false
}
