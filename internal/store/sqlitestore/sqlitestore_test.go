package sqlitestore

import (
	"context"
	"crypto/rand"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	if strings.Contains(strings.ToLower(schema), "private") {
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
}

func TestSecondOpenFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s := open(t, path)
	defer s.Close()
	if second, err := Open(context.Background(), path); err == nil {
		_ = second.Close()
		t.Fatal("a second Open on a held database succeeded")
	}
}
