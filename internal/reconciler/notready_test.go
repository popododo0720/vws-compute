package reconciler_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// records keeps every log record it handles.
type records struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (r *records) Enabled(context.Context, slog.Level) bool { return true }
func (r *records) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = append(r.recs, rec.Clone())
	return nil
}
func (r *records) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *records) WithGroup(string) slog.Handler      { return r }

// at returns the error text of every record logged at level.
func (r *records) at(level slog.Level) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, rec := range r.recs {
		if rec.Level != level {
			continue
		}
		var msg string
		rec.Attrs(func(a slog.Attr) bool {
			if a.Key == "err" {
				msg = a.Value.String()
			}
			return true
		})
		out = append(out, msg)
	}
	return out
}

// TestNotReadyLogsAtDebug: while a machine is still booting, Run logs each
// sweep's not-ready error at debug and never as a warning. A real transient
// failure in between is still a warning.
func TestNotReadyLogsAtDebug(t *testing.T) {
	repo := memstore.New()
	agent := fakeagent.New()
	agent.Fail("web",
		hypervisor.NotReady(errors.New("machine web: instance phase is \"Scheduling\", not Running")),
		hypervisor.NotReady(errors.New("machine web: instance phase is \"Scheduling\", not Running")),
		errors.New("api down"),
		hypervisor.NotReady(errors.New("machine web: no virtual machine instance yet")),
	)
	logs := &records{}
	rec := reconciler.New(repo, agent, reconciler.WithReports(repo), reconciler.WithInterval(time.Millisecond), reconciler.WithLogger(slog.New(logs)))
	ctl := controller.New(repo)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rec.Run(ctx) }()
	stop := sync.OnceFunc(func() {
		cancel()
		<-done
	})
	defer stop()
	if _, err := ctl.Submit(ctx, ok(compute.ParseRequestID("create")), createInstance("web")); err != nil {
		t.Fatal(err)
	}
	id := ok(compute.ParseInstanceID("web"))
	deadline := time.Now().Add(5 * time.Second)
	for ok(repo.Instance(ctx, id)).Status.Report.State != compute.ReportReady {
		if time.Now().After(deadline) {
			t.Fatal("instance did not turn READY")
		}
		time.Sleep(time.Millisecond)
	}
	stop()

	debug, warn := logs.at(slog.LevelDebug), logs.at(slog.LevelWarn)
	if len(debug) != 3 {
		t.Errorf("debug records = %q, want the 3 not-ready sweeps", debug)
	}
	for _, d := range debug {
		if !strings.Contains(d, "not Running") && !strings.Contains(d, "no virtual machine instance yet") {
			t.Errorf("debug record %q is not a not-ready error", d)
		}
	}
	if len(warn) != 1 || !strings.Contains(warn[0], "api down") {
		t.Errorf("warn records = %q, want only the api down failure", warn)
	}
}
