package kubevirt

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// TestPowerOnWaitsForReadyVMI: a powered-on machine
// is converged only once its VirtualMachineInstance is Running with Ready
// True. Every other state is a transient error naming what is missing, and
// the check writes nothing.
func TestPowerOnWaitsForReadyVMI(t *testing.T) {
	cases := []struct {
		name         string
		phase, ready string // phase "" means no VMI at all
		want         string // "" means converged
	}{
		{"missing", "", "", "no virtual machine instance yet"},
		{"pending", "Pending", "", `phase is "Pending"`},
		{"scheduling", "Scheduling", "False", `phase is "Scheduling"`},
		{"scheduled", "Scheduled", "False", `phase is "Scheduled"`},
		{"running-not-ready", "Running", "False", `Ready condition is "False"`},
		{"running-no-condition", "Running", "", `Ready condition is ""`},
		{"failed", "Failed", "False", `phase is "Failed"`},
		{"succeeded", "Succeeded", "", `phase is "Succeeded"`},
		{"running-ready", "Running", "True", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c, ns, w := newBareClient(t)
			standardImage(t, c, ns)
			a := New(c, Config{Namespace: ns})
			name := "i-0123456789abcdefa"
			s := spec(name, hypervisor.PowerOn)
			if tc.phase != "" {
				setVMI(ctx, t, c, ns, name, tc.phase, tc.ready)
			}
			for pass := range 2 {
				before := w.n.Load()
				err := a.ConvergeMachine(ctx, s)
				switch {
				case tc.want == "" && err != nil:
					t.Fatalf("pass %d: %v, want converged", pass, err)
				case tc.want != "" && (!hypervisor.IsNotReady(err) || hypervisor.IsPermanent(err) || !strings.Contains(err.Error(), tc.want)):
					t.Fatalf("pass %d: %v, want a NotReady error containing %q", pass, err, tc.want)
				}
				// The first pass creates the VirtualMachine. A second pass
				// on an unchanged spec only reads, ready or not.
				if got := w.n.Load() - before; pass == 1 && got != 0 {
					t.Fatalf("second pass wrote %d times, want 0", got)
				}
			}
		})
	}
}

// TestPowerOffNeedsNoInstance: a stopped machine converges without a VMI.
func TestPowerOffNeedsNoInstance(t *testing.T) {
	c, ns, _ := newBareClient(t)
	standardImage(t, c, ns)
	if err := New(c, Config{Namespace: ns}).ConvergeMachine(context.Background(), spec("i-0123456789abcdefb", hypervisor.PowerOff)); err != nil {
		t.Fatal(err)
	}
}

// TestInstanceReadyOnlyAfterVMIRunning drives CreateInstance through the gRPC
// API and the reconciler: while the VMI is Scheduling, Running without Ready,
// Failed, or missing, the instance stays RECONCILING at observed generation 0,
// and each sweep re-checks. It turns READY at generation 1 only on the sweep
// after the VMI is Running and Ready. This is the T1 defect (#49): READY was
// reported while the VMI was still Scheduling.
func TestInstanceReadyOnlyAfterVMIRunning(t *testing.T) {
	ctx := context.Background()
	c, ns, _ := newBareClient(t)
	claim(t, c, ns, "pvc-root-1", corev1.ReadWriteOnce)
	repo, rec := createUnreconciled(t, New(c, Config{Namespace: ns, ImageNamespace: ns}), "vws.small", "pvc-root-1")

	sweep := func(wantState compute.ReportState, wantGen int64, wantMsg string) {
		t.Helper()
		// A sweep that leaves an instance reconciling returns its error, so
		// the caller logs it and sweeps again.
		err := rec.Sweep(ctx)
		if (wantState == compute.ReportReady) != (err == nil) {
			t.Fatalf("sweep: %v", err)
		}
		got := status(ctx, t, repo)
		if got.Report.State != wantState || got.Observed.Generation != wantGen || !strings.Contains(got.Report.Message, wantMsg) {
			t.Fatalf("status = %+v, want state %v at generation %d with message containing %q", got, wantState, wantGen, wantMsg)
		}
	}

	sweep(compute.ReportReconciling, 0, "no virtual machine instance yet")
	setVMI(ctx, t, c, ns, "web-1", "Scheduling", "False")
	sweep(compute.ReportReconciling, 0, `phase is "Scheduling"`)
	setVMI(ctx, t, c, ns, "web-1", "Running", "False")
	sweep(compute.ReportReconciling, 0, `Ready condition is "False"`)
	setVMI(ctx, t, c, ns, "web-1", "Failed", "False")
	sweep(compute.ReportReconciling, 0, `phase is "Failed"`)
	setVMI(ctx, t, c, ns, "web-1", "Running", "True")
	sweep(compute.ReportReady, 1, "")
	// Converged stays converged on the next sweep.
	sweep(compute.ReportReady, 1, "")
}

func status(ctx context.Context, t *testing.T, repo *memstore.Store) compute.InstanceStatus {
	t.Helper()
	got, err := repo.Instance(ctx, ok(compute.ParseInstanceID("web-1")))
	if err != nil {
		t.Fatal(err)
	}
	return got.Status
}
