package sqlitestore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/storetest"
)

func open(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Repository {
		s := open(t, filepath.Join(t.TempDir(), "state.db"))
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

func TestSchemaHasNoPrivateKeyColumn(t *testing.T) {
	if strings.Contains(strings.ToLower(strings.Join(migrations, "\n")), "private") {
		t.Fatal("schema mentions private: private keys are never stored")
	}
	s := open(t, filepath.Join(t.TempDir(), "state.db"))
	defer s.Close()
	rows, err := s.db.Query(`SELECT m.name, p.name FROM sqlite_master m JOIN pragma_table_info(m.name) p WHERE m.type = 'table'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns := 0
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			t.Fatal(err)
		}
		columns++
		if c := strings.ToLower(column); strings.Contains(c, "private") || strings.Contains(c, "secret") {
			t.Errorf("column %s.%s looks like key material", table, column)
		}
	}
	if columns < 40 {
		t.Fatalf("read %d columns, want the full schema", columns)
	}
}

func TestReopenPreservesState(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	s := open(t, path)
	in, err := compute.NewRegisterImage("debian-13", "https://images.example/d.qcow2")
	if err != nil {
		t.Fatal(err)
	}
	env := compute.Env{Now: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC), Rand: rand.Reader}
	ch, err := compute.Plan(nil, env, in)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := compute.ParseRequestID("reopen")
	op, err := compute.NewOperation(env, req, in, ch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitDesired(ctx, store.DesiredChange{Op: op, Spec: ch.Spec, Write: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open(t, path)
	defer s.Close()
	got, err := s.Operation(ctx, op.ID)
	if err != nil || got.State() != compute.OperationPending || got.Target != ch.Spec.Ref() {
		t.Fatalf("operation after reopen = %+v, %v", got, err)
	}
	refs, err := s.Unconverged(ctx, env.Now)
	if err != nil || len(refs) != 1 || refs[0] != ch.Spec.Ref() {
		t.Fatalf("Unconverged after reopen = %v, %v; want [%v]", refs, err, ch.Spec.Ref())
	}

	imgSpec := ch.Spec.(compute.ImageSpec)
	img, err := s.Image(ctx, imgSpec.ID)
	if err != nil {
		t.Fatal(err)
	}
	labels, err := compute.ParseLabels(map[string]string{"vws-fingerprint": "fp"})
	if err != nil {
		t.Fatal(err)
	}
	desc, err := compute.ParseDescription("kept\nacross restart")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CommitImageStatus(ctx, img.Spec.ID, compute.ImageStatus{
		Observed: compute.Observed{Generation: img.Spec.Generation}, Present: true, UpdatedAt: env.Now,
	}); err != nil {
		t.Fatal(err)
	}
	meta := &compute.Metadata{Labels: labels, Description: desc, Revision: 1, ChangedAt: env.Now.Add(time.Minute)}
	patch := compute.MetadataPatch{}.ReplaceLabels(labels).SetDescription(desc)
	inMeta, err := compute.NewUpdateImageMetadata(img.Spec.ID.String(), patch)
	if err != nil {
		t.Fatal(err)
	}
	opMeta, err := compute.NewOperation(env, mustRequest(t, "reopen-meta"), inMeta, compute.Change{Spec: img.Spec, Meta: meta})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitDesired(ctx, store.DesiredChange{Op: opMeta, Spec: img.Spec, Meta: meta}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open(t, path)
	defer s.Close()
	gotImg, err := s.Image(ctx, img.Spec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotImg.Meta.Revision != 1 || !gotImg.Meta.Labels.Equal(labels) || gotImg.Meta.Description.String() != desc.String() {
		t.Fatalf("metadata after reopen = %+v", gotImg.Meta)
	}
	refs, err = s.Unconverged(ctx, env.Now.Add(time.Hour))
	if err != nil || len(refs) != 0 {
		t.Fatalf("Unconverged after a label-only write = %v, %v; want none", refs, err)
	}
}

func mustRequest(t *testing.T, s string) compute.RequestID {
	t.Helper()
	id, err := compute.ParseRequestID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestOpenMigratesAVersion1File(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.db")
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(schemaV1); err != nil {
		t.Fatal(err)
	}
	const id = "i-00000000000000001"
	if _, err := raw.Exec(`INSERT INTO instances (id, generation, created_at, changed_at, image_id, instance_type, power, reboot_epoch)
		VALUES (?, 1, 1, 1, 'img-00000000000000001', 'vws.small', 1, 0)`, id); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := raw.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != 0 {
		t.Fatalf("user_version before Open = %d, %v; want 0", version, err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	s := open(t, path)
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != len(migrations) {
		t.Fatalf("user_version after Open = %d, %v; want %d", version, err, len(migrations))
	}
	inst, err := s.Instance(ctx, mustInstance(t, id))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(inst.Meta, compute.Metadata{}) || inst.Spec.Generation != 1 {
		t.Fatalf("migrated instance = gen %d meta %+v, want gen 1 and empty metadata", inst.Spec.Generation, inst.Meta)
	}
	labels, err := compute.ParseLabels(map[string]string{"vws-fingerprint": "migrated"})
	if err != nil {
		t.Fatal(err)
	}
	when := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	meta := &compute.Metadata{Labels: labels, Revision: 1, ChangedAt: when}
	in, err := compute.NewUpdateInstanceMetadata(id, compute.MetadataPatch{}.ReplaceLabels(labels))
	if err != nil {
		t.Fatal(err)
	}
	op, err := compute.NewOperation(compute.Env{Now: when, Rand: rand.Reader}, mustRequest(t, "migrate-meta"), in, compute.Change{Spec: inst.Spec, Meta: meta})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitDesired(ctx, store.DesiredChange{Op: op, Spec: inst.Spec, Meta: meta}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open(t, path)
	defer s.Close()
	got, err := s.Instance(ctx, inst.Spec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Meta.Labels.Equal(labels) || got.Meta.Revision != 1 || !got.Meta.ChangedAt.Equal(when) {
		t.Fatalf("metadata after migration reopen = %+v", got.Meta)
	}
}

func TestOpenMigratesAnImageSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v2.db")
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(schemaV1); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(metadataV2); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`PRAGMA user_version = 2`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`INSERT INTO images (id, generation, created_at, changed_at, name, source_url, deregistered)
		VALUES ('debian-12', 1, 1, 1, 'debian-12', 'https://images.example/d.qcow2', 0)`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	s := open(t, path)
	defer s.Close()
	img, err := s.Image(ctx, mustImage(t, "debian-12"))
	if err != nil {
		t.Fatal(err)
	}
	src, ok := img.Spec.Source.(compute.FromURL)
	if !ok || src.URL.String() != "https://images.example/d.qcow2" || img.Spec.Name.String() != "debian-12" {
		t.Fatalf("migrated image = %+v", img.Spec)
	}
}

func mustImage(t *testing.T, s string) compute.ImageID {
	t.Helper()
	id, err := compute.ParseImageID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestOpenRefusesNewerSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new.db")
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`PRAGMA user_version = 99`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), path); err == nil || !strings.Contains(err.Error(), "newer") {
		t.Fatalf("Open of a newer schema = %v, want a refusal", err)
	}
}

func mustInstance(t *testing.T, s string) compute.InstanceID {
	t.Helper()
	id, err := compute.ParseInstanceID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestSecondOpenFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	first := open(t, path)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen the existing file. Creating the schema no longer writes, so the
	// exclusive lock has to be taken explicitly.
	held := open(t, path)
	defer held.Close()
	if second, err := Open(context.Background(), path); err == nil {
		_ = second.Close()
		t.Fatal("a second Open on a held database succeeded")
	}
}

// TestNodeDrivenStateSurvivesRestart reopens the file between every step and
// checks that placement, the node report, and volume ownership are on disk.
func TestNodeDrivenStateSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	env := compute.Env{Now: now, Rand: rand.Reader}
	reopen := func(s *Store) *Store {
		t.Helper()
		if s != nil {
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		}
		return open(t, path)
	}

	s := reopen(nil)
	const node = "projects/p/zones/z/nodes/a"
	nin, err := compute.NewCreateNode(node, 1, compute.Labels{})
	if err != nil {
		t.Fatal(err)
	}
	nch, err := compute.Plan(viewOf{s}, env, nin)
	if err != nil {
		t.Fatal(err)
	}
	nop, err := compute.NewOperation(env, mustRequest(t, "node-1"), nin, nch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitDesired(ctx, store.DesiredChange{Op: nop, Spec: nch.Spec, Write: nch.Write, Meta: nch.Meta}); err != nil {
		t.Fatal(err)
	}
	name, _ := compute.ParseNodeName(node)
	s = reopen(s)
	if _, _, err := s.SyncNode(ctx, name, compute.Heartbeat{State: compute.NodeReady, AgentVersion: "agent-1"}, now,
		func(compute.Node, []compute.Instance) error { return nil }); err != nil {
		t.Fatal(err)
	}

	s = reopen(s)
	vol, _ := compute.ParseVolumeRef("projects/p/zones/z/volumes/root")
	in, err := compute.NewCreateInstance(compute.CreateInstanceParams{
		ID: "web-1", Project: "p", Zone: "z", MachineType: "n1.medium", Power: compute.PowerRunning,
		Volumes: []compute.VolumeAttachment{{Volume: vol, DeviceName: "vda", Boot: true}},
		NICs:    []compute.NetworkInterface{{DeviceName: "eth0", Subnet: "projects/p/regions/r/vpcs/v/subnets/s"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := compute.Plan(viewOf{s}, env, in)
	if err != nil {
		t.Fatal(err)
	}
	op, err := compute.NewOperation(env, mustRequest(t, "inst-1"), in, ch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitDesired(ctx, store.DesiredChange{Op: op, Spec: ch.Spec, Write: ch.Write}); err != nil {
		t.Fatal(err)
	}

	s = reopen(s)
	id, _ := compute.ParseInstanceID("web-1")
	got, err := s.Instance(ctx, id)
	if err != nil || got.Status.AssignedNode != node || len(got.Spec.Volumes) != 1 {
		t.Fatalf("after restart: %+v, %v", got, err)
	}
	if _, err := s.ReportInstanceStatus(ctx, id, compute.InstanceReport{Node: node, Generation: 1, State: compute.ReportReady, Power: compute.PowerRunning}, now); err != nil {
		t.Fatal(err)
	}

	s = reopen(s)
	defer s.Close()
	got, err = s.Instance(ctx, id)
	if err != nil || got.Status.Observed.Generation != 1 || got.Status.Report.State != compute.ReportReady || got.Status.AssignedNode != node {
		t.Fatalf("report after restart: %+v, %v", got.Status, err)
	}
	if n, err := s.Node(ctx, name); err != nil || n.Status.AgentVersion != "agent-1" || !n.Status.LastSync.Equal(now) {
		t.Fatalf("node after restart: %+v, %v", n.Status, err)
	}
	if done, err := s.Operation(ctx, op.ID); err != nil || done.State() != compute.OperationSucceeded {
		t.Fatalf("create operation after report: %v, %v", done.State(), err)
	}
	// The same request after the restart is the same operation.
	again, err := s.CommitDesired(ctx, store.DesiredChange{Op: op, Spec: ch.Spec, Write: ch.Write})
	if err != nil || again.ID != op.ID {
		t.Fatalf("replay = %v, %v", again.ID, err)
	}
}

// viewOf lets a test plan against a store without the controller.
type viewOf struct{ s *Store }

func (v viewOf) Instance(id compute.InstanceID) (compute.Instance, error) {
	return v.s.Instance(context.Background(), id)
}
func (v viewOf) Image(id compute.ImageID) (compute.Image, error) {
	return v.s.Image(context.Background(), id)
}
func (v viewOf) Node(n compute.NodeName) (compute.Node, error) {
	return v.s.Node(context.Background(), n)
}
func (v viewOf) KeyPairByName(n compute.KeyName) (compute.KeyPair, error) {
	return v.s.KeyPairByName(context.Background(), n)
}
