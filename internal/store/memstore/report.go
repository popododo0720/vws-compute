package memstore

import (
	"context"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

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
