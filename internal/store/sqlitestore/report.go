package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

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
