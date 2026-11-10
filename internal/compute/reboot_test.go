package compute_test

import (
	"math"
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
)

func TestRebootPlan(t *testing.T) {
	in := ok(compute.NewRebootInstance(instanceID, "projects/p/zones/z/instances/"+instanceID))
	for _, tc := range []struct {
		name  string
		power compute.Power
		epoch int64
		code  compute.Code
	}{
		{"running", compute.PowerRunning, 0, 0},
		{"stopped", compute.PowerStopped, 0, compute.CodeIncorrectState},
		{"terminated", compute.PowerTerminated, 0, compute.CodeIncorrectState},
		{"overflow", compute.PowerRunning, math.MaxInt64, compute.CodeLimitExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inst := runningInstance()
			inst.Spec.Power = tc.power
			inst.Spec.RebootEpoch = tc.epoch
			ch, err := compute.Plan(newView().with(inst), compute.Env{Now: t0}, in)
			if tc.code != 0 {
				if err == nil || compute.CodeOf(err) != tc.code {
					t.Fatalf("error = %v, want %v", err, tc.code)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			next := ch.Spec.(compute.InstanceSpec)
			if !ch.Write || next.RebootEpoch != 1 || next.Generation != 2 || next.Power != compute.PowerRunning {
				t.Fatalf("change = %+v", ch)
			}
		})
	}
	if _, err := compute.NewRebootInstance("!", "!"); err == nil {
		t.Fatal("accepted invalid id")
	}
	other := ok(compute.NewRebootInstance(instanceID, "projects/foreign/zones/z/instances/"+instanceID))
	if compute.FingerprintOf(in) == compute.FingerprintOf(other) {
		t.Fatal("resource scope absent from fingerprint")
	}
}

func TestRebootReportMustCompleteAndNeverRegress(t *testing.T) {
	inst := runningInstance()
	inst.Spec.Project = "p"
	inst.Spec.Zone = "z"
	inst.Spec.Generation = 2
	inst.Spec.RebootEpoch = 1
	report := compute.InstanceReport{Generation: 2, State: compute.ReportReady, Power: compute.PowerRunning}
	st, _, err := compute.ApplyReport(inst, report, t0)
	if err != nil {
		t.Fatal(err)
	}
	if st.Observed.Generation != 1 || st.RebootEpoch != 0 {
		t.Fatalf("unfinished reboot converged: %+v", st)
	}
	report.RebootEpoch = 1
	st, _, err = compute.ApplyReport(inst, report, t0)
	if err != nil || st.Observed.Generation != 2 || st.RebootEpoch != 1 {
		t.Fatalf("completed reboot = %+v, %v", st, err)
	}
	inst.Status = st
	report.RebootEpoch = 0
	st, _, err = compute.ApplyReport(inst, report, t0)
	if err != nil || st.RebootEpoch != 1 {
		t.Fatalf("count regressed: %+v, %v", st, err)
	}
	for _, count := range []int64{-1, 2} {
		report.RebootEpoch = count
		if _, _, err := compute.ApplyReport(inst, report, t0); compute.CodeOf(err) != compute.CodeInvalid {
			t.Fatalf("count %d accepted: %v", count, err)
		}
	}
}
