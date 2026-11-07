package kubestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	rl "k8s.io/client-go/tools/leaderelection/resourcelock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LeaseName is the coordination.k8s.io Lease in the store namespace that the
// replicas of one deployment campaign for.
const LeaseName = "vws-compute"

// The client-go defaults. A leader that cannot renew for RenewDeadline stops
// its term. A follower takes over LeaseDuration after the last renew it
// observed, at most one RetryPeriod later, or at once after a release.
const (
	DefaultLeaseDuration = 15 * time.Second
	DefaultRenewDeadline = 10 * time.Second
	DefaultRetryPeriod   = 2 * time.Second
)

// While leading, Repair runs at the start of each term and every
// repairInterval after. It leaves an index or claim written less than
// repairGrace ago: followers keep committing, and such a write may be a
// commit between its two writes.
const (
	repairInterval = 5 * time.Minute
	repairGrace    = 2 * time.Minute
)

type lease struct {
	duration, renewDeadline, retryPeriod time.Duration
}

// WithLease sets the leader election timings Lead uses. client-go requires
// duration > renewDeadline > 1.2 * retryPeriod > 0.
func (s *Store) WithLease(duration, renewDeadline, retryPeriod time.Duration) *Store {
	s.lease = lease{duration, renewDeadline, retryPeriod}
	return s
}

// Lead campaigns for the Lease and runs term while this replica holds it,
// until ctx ends. Each term starts with a Repair. A lost Lease cancels the
// term's context; Lead waits for term to return and campaigns again. On
// shutdown it releases the Lease after term returned, so a follower takes
// over without waiting out the Lease. A term error other than its context
// ending stops Lead with that error.
func (s *Store) Lead(ctx context.Context, identity string, term func(context.Context) error) error {
	if identity == "" {
		return errors.New("leader election: empty identity")
	}
	for {
		err := s.leadOnce(ctx, identity, term)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
}

func (s *Store) leadOnce(ctx context.Context, identity string, term func(context.Context) error) error {
	lock := &leaseLock{c: s.c, key: client.ObjectKey{Namespace: s.ns, Name: LeaseName}, identity: identity}
	// The elector runs on its own context, so a shutdown first ends the
	// term and only then stops renewing.
	elect, stopElect := context.WithCancel(context.WithoutCancel(ctx))
	defer stopElect()
	acquired := make(chan context.Context, 1)
	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		Name:          LeaseName,
		LeaseDuration: s.lease.duration,
		RenewDeadline: s.lease.renewDeadline,
		RetryPeriod:   s.lease.retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(held context.Context) { acquired <- held },
			OnStoppedLeading: func() {},
		},
	})
	if err != nil {
		return fmt.Errorf("leader election: %w", err)
	}
	ran := make(chan struct{})
	go func() {
		defer close(ran)
		le.Run(elect)
	}()
	var held context.Context
	select {
	case <-ctx.Done():
		stopElect()
		<-ran
		return ctx.Err()
	case held = <-acquired:
	}
	slog.InfoContext(ctx, "leading", slog.String("lease", LeaseName), slog.String("identity", identity))
	termCtx, cancel := context.WithCancel(held)
	stop := context.AfterFunc(ctx, cancel)
	// The term ends with the Lease, which held tracks, and with ctx through
	// AfterFunc.
	err = s.term(termCtx, term) //nolint:contextcheck // held, not ctx, is the term's parent
	stop()
	cancel()
	stopElect()
	<-ran
	if ctx.Err() != nil {
		rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), s.lease.renewDeadline)
		defer rcancel()
		if rerr := lock.release(rctx); rerr != nil {
			slog.WarnContext(ctx, "release lease", slog.String("lease", LeaseName), slog.String("err", rerr.Error()))
		}
	}
	slog.InfoContext(ctx, "stopped leading", slog.String("lease", LeaseName), slog.String("identity", identity))
	return err
}

// term runs Repair, then term, and Repair again every repairInterval until
// term returns.
func (s *Store) term(ctx context.Context, term func(context.Context) error) error {
	repairCtx, stopRepair := context.WithCancel(ctx)
	defer stopRepair()
	repaired := make(chan struct{})
	go func() {
		ctx := repairCtx
		defer close(repaired)
		tick := time.NewTicker(repairInterval)
		defer tick.Stop()
		for {
			r, err := s.repair(ctx, time.Now().Add(-repairGrace))
			switch {
			case ctx.Err() != nil:
				return
			case err != nil:
				slog.WarnContext(ctx, "repair store", slog.String("err", err.Error()))
			default:
				slog.InfoContext(ctx, "repaired store", slog.Any("repaired", r))
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	err := term(ctx)
	stopRepair()
	<-repaired
	return err
}

// leaseLock is the client-go resource lock on a coordination.k8s.io Lease,
// through the store's controller-runtime client.
type leaseLock struct {
	c        client.Client
	key      client.ObjectKey
	identity string
	lease    *coordinationv1.Lease
}

var _ rl.Interface = (*leaseLock)(nil)

func (l *leaseLock) Get(ctx context.Context) (*rl.LeaderElectionRecord, []byte, error) {
	var cur coordinationv1.Lease
	if err := l.c.Get(ctx, l.key, &cur); err != nil {
		return nil, nil, err //nolint:wrapcheck // the elector branches on apierrors.IsNotFound
	}
	l.lease = &cur
	rec := rl.LeaseSpecToLeaderElectionRecord(&cur.Spec)
	raw, err := json.Marshal(rec)
	if err != nil {
		return nil, nil, fmt.Errorf("encode lease record: %w", err)
	}
	return rec, raw, nil
}

func (l *leaseLock) Create(ctx context.Context, rec rl.LeaderElectionRecord) error {
	next := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: l.key.Namespace, Name: l.key.Name},
		Spec:       rl.LeaderElectionRecordToLeaseSpec(&rec),
	}
	if err := l.c.Create(ctx, next); err != nil {
		return err //nolint:wrapcheck // the elector logs and retries
	}
	l.lease = next
	return nil
}

func (l *leaseLock) Update(ctx context.Context, rec rl.LeaderElectionRecord) error {
	if l.lease == nil {
		return errors.New("lease not initialized, call Get or Create first")
	}
	next := l.lease.DeepCopy()
	next.Spec = rl.LeaderElectionRecordToLeaseSpec(&rec)
	if err := l.c.Update(ctx, next); err != nil {
		return err //nolint:wrapcheck // the elector branches on apierrors.IsConflict
	}
	l.lease = next
	return nil
}

func (l *leaseLock) RecordEvent(string) {}

func (l *leaseLock) Identity() string { return l.identity }

func (l *leaseLock) Describe() string { return l.key.String() }

// release clears the holder if this replica still holds the Lease, so a
// follower acquires it on its next retry. A CAS loss means someone else
// already holds it.
func (l *leaseLock) release(ctx context.Context) error {
	var cur coordinationv1.Lease
	err := l.c.Get(ctx, l.key, &cur)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get lease: %w", err)
	}
	if cur.Spec.HolderIdentity == nil || *cur.Spec.HolderIdentity != l.identity {
		return nil
	}
	one := int32(1)
	now := metav1.NewMicroTime(time.Now())
	cur.Spec.HolderIdentity = nil
	cur.Spec.LeaseDurationSeconds = &one
	cur.Spec.RenewTime = &now
	if err := l.c.Update(ctx, &cur); err != nil && !apierrors.IsConflict(err) {
		return fmt.Errorf("release lease: %w", err)
	}
	return nil
}
