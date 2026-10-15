package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

type volMap map[string]VolumeInfo

func (m volMap) Volume(_ context.Context, name string) (VolumeInfo, error) {
	info, ok := m[name]
	if !ok {
		return VolumeInfo{}, ErrVolumeNotFound
	}
	return info, nil
}

// TestImageNotFoundPublishesReadyReasonAndDeletesRootDisk publishes one
// READY/FALSE condition for a launch the agent refused, Internal after the
// give-up deadline, and Internal for a permanent claim error with no cause.
func TestImageNotFoundPublishesReadyReasonAndDeletesRootDisk(t *testing.T) {
	ctx := context.Background()
	repo := memstore.New()
	agent := fakeagent.New()
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	badVol := "projects/p/zones/z/volumes/bad"
	missingVol := "projects/p/zones/z/volumes/missing"
	lookup := volMap{
		badVol: {State: commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR, Message: "clone failed"},
		missingVol: {
			State:   commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR,
			Message: "clone failed",
			Reason:  "ImageNotFound",
		},
	}
	rec := reconciler.New(repo, agent,
		reconciler.WithClock(clock),
		reconciler.WithReports(repo),
		reconciler.WithClaims(Claims(lookup)))
	ctl := controller.New(repo, controller.WithClock(clock), controller.WithKick(rec.Kick))

	reg, err := ctl.Submit(ctx, mustRequest(t, "register-image"), okIntent(
		compute.NewRegisterImage("debian-13", "https://images.example/debian-13.qcow2")))
	if err != nil {
		t.Fatal(err)
	}
	imgID := mustImage(t, reg.Operation.Target.ID)
	if err := repo.CommitImageStatus(ctx, imgID, compute.ImageStatus{
		Observed: compute.Observed{Generation: 1}, Present: true, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if agent.HasImage(imgID.String()) {
		t.Fatal("the agent has an artifact for the stored image")
	}

	run, err := ctl.Submit(ctx, mustRequest(t, "run-missing"), okIntent(
		compute.NewRunInstance(imgID.String(), "vws.small", "")))
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	inst := mustLoad(t, repo, run.Operation.Target.ID)
	pb := instancePB(inst, "")
	wantMsg := fmt.Sprintf("machine %s: image artifact %s not found", inst.Spec.ID, imgID)
	wantReadyFalse(t, pb, "ImageNotFound", wantMsg)
	if inst.Status.Observed.Failure == nil ||
		inst.Status.Observed.Failure.Code != compute.CodeFailedPrecondition ||
		inst.Status.Observed.Failure.Reason != compute.LaunchImageNotFound ||
		inst.Status.Observed.Failure.Message != wantMsg {
		t.Fatalf("failure = %+v, want FailedPrecondition ImageNotFound %q", inst.Status.Observed.Failure, wantMsg)
	}

	run2, err := ctl.Submit(ctx, mustRequest(t, "run-transient"), okIntent(
		compute.NewRunInstance(imgID.String(), "vws.small", "")))
	if err != nil {
		t.Fatal(err)
	}
	agent.Fail(run2.Operation.Target.ID, errors.New("agent busy"), errors.New("agent busy"))
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	now = now.Add(reconciler.GiveUpAfter)
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	inst2 := mustLoad(t, repo, run2.Operation.Target.ID)
	gaveUp := fmt.Sprintf("gave up after %s of transient failures: agent busy", reconciler.GiveUpAfter)
	wantReadyFalse(t, instancePB(inst2, ""), "Internal", gaveUp)
	if inst2.Status.Observed.Failure == nil || inst2.Status.Observed.Failure.Reason != compute.LaunchInternal ||
		inst2.Status.Observed.Failure.Code != compute.CodeInternal {
		t.Fatalf("give-up failure = %+v, want Internal", inst2.Status.Observed.Failure)
	}

	_, err = ctl.Submit(ctx, mustRequest(t, "create-bad"), okIntent(compute.NewCreateInstance(compute.CreateInstanceParams{
		ID: "web-bad", Project: "p", Zone: "z", MachineType: "vws.small", Power: compute.PowerRunning,
		Volumes: []compute.VolumeAttachment{{
			Volume: mustVolumeRef(t, badVol), DeviceName: "vda", Boot: true,
		}},
	})))
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = ctl.Submit(ctx, mustRequest(t, "create-missing"), okIntent(compute.NewCreateInstance(
		compute.CreateInstanceParams{
			ID: "web-missing", Project: "p", Zone: "z", MachineType: "vws.small", Power: compute.PowerRunning,
			Volumes: []compute.VolumeAttachment{{
				Volume: mustVolumeRef(t, missingVol), DeviceName: "vda", Boot: true,
			}},
		})))
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	bad := mustLoad(t, repo, "web-bad")
	wantReadyFalse(t, instancePB(bad, ""), "Internal", "volume "+badVol+" failed in storage: clone failed")
	if bad.Status.Observed.Failure == nil || bad.Status.Observed.Failure.Code != compute.CodeFailedPrecondition ||
		bad.Status.Observed.Failure.Reason != compute.LaunchInternal {
		t.Fatalf("claim failure = %+v, want FailedPrecondition Internal", bad.Status.Observed.Failure)
	}
	missing := mustLoad(t, repo, "web-missing")
	wantReadyFalse(t, instancePB(missing, ""), "ImageNotFound", "volume "+missingVol+" failed in storage: clone failed")
	if missing.Status.Observed.Failure == nil || missing.Status.Observed.Failure.Code != compute.CodeFailedPrecondition ||
		missing.Status.Observed.Failure.Reason != compute.LaunchImageNotFound {
		t.Fatalf("missing-image claim = %+v, want FailedPrecondition ImageNotFound", missing.Status.Observed.Failure)
	}

	reported, err := parseInstanceReport(&computev1.InstanceStatus{Observed: &commonv1.ObservedState{
		State:      commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR,
		Generation: 1,
		Conditions: []*commonv1.Condition{{
			Type:   commonv1.ConditionType_CONDITION_TYPE_READY,
			State:  commonv1.ConditionState_CONDITION_STATE_FALSE,
			Reason: "ImageNotFound",
		}},
	}})
	if err != nil || reported.Reason != compute.LaunchImageNotFound {
		t.Fatalf("parsed report = %+v %v, want ImageNotFound", reported, err)
	}
	plain, err := parseInstanceReport(&computev1.InstanceStatus{Observed: &commonv1.ObservedState{
		State: commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR, Generation: 1,
	}})
	if err != nil || plain.Reason != compute.LaunchInternal {
		t.Fatalf("parsed report without a condition = %+v %v, want Internal", plain, err)
	}
}

func wantReadyFalse(t *testing.T, pb *computev1.Instance, reason, message string) {
	t.Helper()
	obs := pb.GetStatus().GetObserved()
	if obs.GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR {
		t.Fatalf("state = %s, want ERROR", obs.GetState())
	}
	conds := obs.GetConditions()
	if len(conds) != 1 {
		t.Fatalf("conditions = %d, want 1", len(conds))
	}
	c := conds[0]
	if c.GetType() != commonv1.ConditionType_CONDITION_TYPE_READY ||
		c.GetState() != commonv1.ConditionState_CONDITION_STATE_FALSE ||
		c.GetReason() != reason || c.GetMessage() != message {
		t.Fatalf("condition type %s state %s reason %q message %q, want READY FALSE %s %q",
			c.GetType(), c.GetState(), c.GetReason(), c.GetMessage(), reason, message)
	}
}

func mustRequest(t *testing.T, id string) compute.RequestID {
	t.Helper()
	got, err := compute.ParseRequestID(id)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func okIntent(in compute.Intent, err error) compute.Intent {
	if err != nil {
		panic(err)
	}
	return in
}

func mustImage(t *testing.T, id string) compute.ImageID {
	t.Helper()
	got, err := compute.ParseImageID(id)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func mustLoad(t *testing.T, repo *memstore.Store, id string) compute.Instance {
	t.Helper()
	iid, err := compute.ParseInstanceID(id)
	if err != nil {
		t.Fatal(err)
	}
	got, err := repo.Instance(context.Background(), iid)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func mustVolumeRef(t *testing.T, name string) compute.VolumeRef {
	t.Helper()
	got, err := compute.ParseVolumeRef(name)
	if err != nil {
		t.Fatal(err)
	}
	return got
}
