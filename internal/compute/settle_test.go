package compute_test

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// pendingOp builds the operation a Stop at target generation gen would get.
func pendingOp(t *testing.T, gen int64) compute.Operation {
	t.Helper()
	in, err := compute.NewStopInstance(instanceID)
	if err != nil {
		t.Fatal(err)
	}
	spec := runningInstance().Spec
	spec.Generation = gen
	op, err := compute.NewOperation(compute.Env{Now: t0, Rand: rand.Reader}, mustRequest(t, "req-1"), in, compute.Change{Spec: spec, Write: true})
	if err != nil {
		t.Fatal(err)
	}
	if op.State() != compute.OperationPending {
		t.Fatalf("new operation state = %s", op.State())
	}
	return op
}

func TestSettle(t *testing.T) {
	now := t0.Add(time.Minute)
	boom := &compute.Failure{Code: compute.CodeFailedPrecondition, Message: "image artifact missing"}
	cases := []struct {
		name      string
		spec      int64
		obs       compute.Observed
		want      compute.OperationState
		wantFail  bool
		wantStamp bool
	}{
		{"observed below target stays pending", 3, compute.Observed{Generation: 2}, compute.OperationPending, false, false},
		{"observed equal without failure succeeds", 3, compute.Observed{Generation: 3}, compute.OperationSucceeded, false, true},
		{"observed equal with failure fails", 3, compute.Observed{Generation: 3, Failure: boom}, compute.OperationFailed, true, true},
		{
			"observed equal with attachment failure fails", 3,
			compute.Observed{Generation: 3, AttachmentFailure: boom},
			compute.OperationFailed, true, true,
		},
		{"observed above target supersedes", 4, compute.Observed{Generation: 4}, compute.OperationSuperseded, false, true},
		{"newer spec supersedes before observation", 4, compute.Observed{Generation: 2}, compute.OperationSuperseded, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := compute.Settle(pendingOp(t, 3), c.spec, c.obs, now)
			if got.State() != c.want {
				t.Fatalf("state = %s, want %s", got.State(), c.want)
			}
			if (got.Failure() != nil) != c.wantFail {
				t.Fatalf("failure = %v, want present=%v", got.Failure(), c.wantFail)
			}
			if c.wantFail && *got.Failure() != *boom {
				t.Fatalf("failure = %+v, want %+v", *got.Failure(), *boom)
			}
			if stamped := got.SettledAt().Equal(now); stamped != c.wantStamp {
				t.Fatalf("settledAt = %v, want stamped=%v", got.SettledAt(), c.wantStamp)
			}
		})
	}
}

func TestSettleTerminalIsAbsorbing(t *testing.T) {
	done := compute.Settle(pendingOp(t, 3), 3, compute.Observed{Generation: 3}, t0)
	later := t0.Add(time.Hour)
	for _, obs := range []compute.Observed{
		{Generation: 3, Failure: &compute.Failure{Code: compute.CodeInternal, Message: "late"}},
		{Generation: 5},
	} {
		got := compute.Settle(done, 5, obs, later)
		if got.State() != compute.OperationSucceeded || got.Failure() != nil || !got.SettledAt().Equal(t0) {
			t.Fatalf("terminal op moved: state=%s failure=%v settledAt=%v", got.State(), got.Failure(), got.SettledAt())
		}
	}
}

func TestRestoreOperationRejectsImpossibleRecords(t *testing.T) {
	base := pendingOp(t, 1)
	fail := &compute.Failure{Code: compute.CodeInternal, Message: "x"}
	cases := []struct {
		name    string
		state   compute.OperationState
		failure *compute.Failure
		settled time.Time
		ok      bool
	}{
		{"pending", compute.OperationPending, nil, time.Time{}, true},
		{"failed", compute.OperationFailed, fail, t0, true},
		{"pending with settled time", compute.OperationPending, nil, t0, false},
		{"terminal without settled time", compute.OperationSucceeded, nil, time.Time{}, false},
		{"succeeded with failure", compute.OperationSucceeded, fail, t0, false},
		{"failed without failure", compute.OperationFailed, nil, t0, false},
		{"unknown state", compute.OperationState(9), nil, t0, false},
	}
	for _, c := range cases {
		got, err := compute.RestoreOperation(base, c.state, c.failure, c.settled)
		if (err == nil) != c.ok {
			t.Errorf("%s: err = %v, want ok=%v", c.name, err, c.ok)
			continue
		}
		if c.ok && got.State() != c.state {
			t.Errorf("%s: state = %s, want %s", c.name, got.State(), c.state)
		}
	}
}
