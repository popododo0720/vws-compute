package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
	"github.com/popododo0720/vws-compute/internal/store/sqlitestore"
)

const seededImageID = "debian-12"

func TestParseSeed(t *testing.T) {
	disk := writeDisk(t)
	good := `{"images":[{"id":"` + seededImageID + `","family":"debian-12","path":"` + disk + `"}]}`
	entries, err := parseSeed(strings.NewReader(good))
	if err != nil || len(entries) != 1 || entries[0].id.String() != seededImageID {
		t.Fatalf("parse = %v %v", entries, err)
	}
	cases := []string{
		`{"images":[{"id":"debian-12","family":"debian-12","path":"` + disk + `","url":"https://example"}]}`,
		`{"images":[{"id":"9debian","family":"debian-12","path":"` + disk + `"}]}`,
		`{"images":[{"family":"debian-12","path":"` + disk + `"}]}`,
		`{"images":[{"id":"debian-12","family":"debian-12","path":"images/debian-12.qcow2"}]}`,
		`{"images":[{"id":"debian-12","family":"debian-12","path":"` + disk + `"},{"id":"debian-12","family":"debian-12","path":"` + disk + `"}]}`,
		good + `{"images":[]}`,
	}
	for _, doc := range cases {
		if _, err := parseSeed(strings.NewReader(doc)); err == nil {
			t.Fatalf("accepted %s", doc)
		}
	}
	if _, err := readSeed(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("missing file was accepted")
	}
}

func TestSeedIsIdempotentAcrossRestarts(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	disk := writeDisk(t)
	doc := filepath.Join(dir, "images.json")
	writeSeedFile(t, doc, disk, seededImageID)
	db := filepath.Join(dir, "state.db")

	repo, err := sqlitestore.Open(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	ctl, agent, stop := boot(t, repo)
	entries, err := readSeed(doc)
	if err != nil {
		stop()
		t.Fatal(err)
	}
	if err := applySeed(ctx, ctl, repo, entries); err != nil {
		stop()
		t.Fatal(err)
	}
	first, err := repo.OperationByRequest(ctx, entries[0].req)
	if err != nil {
		stop()
		t.Fatal(err)
	}
	if !agent.HasImage(seededImageID) {
		stop()
		t.Fatal("seeded artifact is missing")
	}
	stop()

	repo, err = sqlitestore.Open(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	ctl, agent, stop = boot(t, repo)
	defer stop()
	if err := applySeed(ctx, ctl, repo, entries); err != nil {
		t.Fatal(err)
	}
	second, err := repo.OperationByRequest(ctx, entries[0].req)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID {
		t.Fatalf("operation = %s, want %s", second.ID, first.ID)
	}
	page, err := repo.Images(ctx, store.ImageQuery{})
	if err != nil || len(page.Items) != 1 || page.Items[0].Spec.ID.String() != seededImageID {
		t.Fatalf("images after restart = %+v %v", page.Items, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !agent.HasImage(seededImageID) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !agent.HasImage(seededImageID) {
		t.Fatal("restart did not rebuild the artifact")
	}

	writeSeedFile(t, doc, disk+"-other", seededImageID)
	changed, err := parseSeed(mustOpen(t, doc))
	if err != nil {
		t.Fatal(err)
	}
	if err := applySeed(ctx, ctl, repo, changed); err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("changed seed = %v, want an error that says changed", err)
	}
}

func TestSeededImageLaunchStopStartTerminate(t *testing.T) {
	disk := writeDisk(t)
	doc := `{"images":[{"id":"` + seededImageID + `","family":"debian-12","path":"` + disk + `"}]}`
	stores := map[string]func(t *testing.T) store.Repository{
		"memstore": func(t *testing.T) store.Repository { return memstore.New() },
		"sqlitestore": func(t *testing.T) store.Repository {
			t.Helper()
			s, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			return s
		},
	}
	for name, open := range stores {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			repo := open(t)
			ctl, agent, stop := boot(t, repo)
			defer stop()
			entries, err := parseSeed(strings.NewReader(doc))
			if err != nil {
				t.Fatal(err)
			}
			if err := applySeed(ctx, ctl, repo, entries); err != nil {
				t.Fatal(err)
			}
			img, err := repo.Image(ctx, okID(t, seededImageID))
			if err != nil || !img.Available() {
				t.Fatalf("seeded image = %+v %v", img, err)
			}
			if _, ok := img.Spec.Source.(compute.FromFile); !ok {
				t.Fatalf("source = %#v, want a file", img.Spec.Source)
			}

			run := step(t, ctl, repo, "run", mustIntent(compute.NewRunInstance(seededImageID, "vws.small", "")))
			id := run.Target.ID
			wantMachine(t, agent, id, hypervisor.PowerOn)
			if life := instanceLife(t, repo, id); life != compute.InstanceRunning {
				t.Fatalf("after run = %d", life)
			}

			step(t, ctl, repo, "stop", mustIntent(compute.NewStopInstance(id)))
			wantMachine(t, agent, id, hypervisor.PowerOff)
			if life := instanceLife(t, repo, id); life != compute.InstanceStopped {
				t.Fatalf("after stop = %d", life)
			}

			step(t, ctl, repo, "start", mustIntent(compute.NewStartInstance(id)))
			wantMachine(t, agent, id, hypervisor.PowerOn)
			if got := agent.Creations(id); got != 1 {
				t.Fatalf("creations after start = %d, want 1", got)
			}
			if life := instanceLife(t, repo, id); life != compute.InstanceRunning {
				t.Fatalf("after start = %d", life)
			}

			step(t, ctl, repo, "terminate", mustIntent(compute.NewTerminateInstance(id)))
			if _, exists := agent.Machine(id); exists {
				t.Fatal("machine exists after terminate")
			}
			if life := instanceLife(t, repo, id); life != compute.InstanceTerminated {
				t.Fatalf("after terminate = %d", life)
			}
			_, err = ctl.Submit(ctx, okReq(t, "start-again"), mustIntent(compute.NewStartInstance(id)))
			if compute.CodeOf(err) != compute.CodeFailedPrecondition {
				t.Fatalf("start after terminate = %v, want FailedPrecondition", err)
			}
		})
	}
}

func boot(t *testing.T, repo store.Repository) (*controller.Controller, *fakeagent.Agent, func()) {
	t.Helper()
	agent := fakeagent.New()
	rec := reconciler.New(repo, agent, reconciler.WithInterval(10*time.Millisecond))
	ctl := controller.New(repo, controller.WithKick(rec.Kick))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = rec.Run(ctx)
		close(done)
	}()
	return ctl, agent, func() {
		cancel()
		<-done
		_ = repo.Close()
	}
}

func step(t *testing.T, ctl *controller.Controller, repo store.Reader, req string, in compute.Intent) compute.Operation {
	t.Helper()
	rc, err := ctl.Submit(context.Background(), okReq(t, req), in)
	if err != nil {
		t.Fatalf("%s: %v", req, err)
	}
	op, err := settle(context.Background(), repo, rc.Operation.ID)
	if err != nil || op.State() != compute.OperationSucceeded {
		t.Fatalf("%s: state %s err %v failure %+v", req, op.State(), err, op.Failure())
	}
	return op
}

func wantMachine(t *testing.T, agent *fakeagent.Agent, id string, power hypervisor.Power) {
	t.Helper()
	m, ok := agent.Machine(id)
	if !ok || m.Power != power {
		t.Fatalf("machine %s = %+v exists %v, want power %d", id, m, ok, power)
	}
}

func instanceLife(t *testing.T, repo store.Reader, id string) compute.InstanceLifecycle {
	t.Helper()
	inst, err := repo.Instance(context.Background(), okInstance(t, id))
	if err != nil {
		t.Fatal(err)
	}
	return inst.Lifecycle()
}

func writeDisk(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "debian-12.qcow2")
	if err := os.WriteFile(path, []byte("disk"), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeSeedFile(t *testing.T, doc, disk, id string) {
	t.Helper()
	body := `{"images":[{"id":"` + id + `","family":"debian-12","path":"` + disk + `"}]}`
	if err := os.WriteFile(doc, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustOpen(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func okID(t *testing.T, s string) compute.ImageID {
	t.Helper()
	id, err := compute.ParseImageID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func okInstance(t *testing.T, s string) compute.InstanceID {
	t.Helper()
	id, err := compute.ParseInstanceID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func okReq(t *testing.T, s string) compute.RequestID {
	t.Helper()
	id, err := compute.ParseRequestID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustIntent(in compute.Intent, err error) compute.Intent {
	if err != nil {
		panic(err)
	}
	return in
}
