package archtest

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVendoredProtoModule(t *testing.T) {
	root := repoRoot(t)
	if _, err := os.Stat(filepath.Join(root, "proto")); !os.IsNotExist(err) {
		t.Fatalf("local proto tree = %v, want absent", err)
	}
	mod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	const replace = "replace github.com/popododo0720/vws-proto => ./third_party/vws-proto"
	if !bytes.Contains(mod, []byte(replace)) {
		t.Fatalf("go.mod missing %s", replace)
	}
	vendored, err := os.ReadFile(filepath.Join(root, "third_party", "vws-proto", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(vendored, []byte("module github.com/popododo0720/vws-proto\n")) {
		t.Fatal("vendored go.mod is not github.com/popododo0720/vws-proto")
	}
}

// Built from escapes so this file does not contain what it forbids.
var forbiddenText = []string{
	"\u2122",
	"\u00ae",
	"\u00a9",
	"all rights " + "reserved",
}

func noticeViolations(root string) ([]string, error) {
	var bad []string
	err := walkFiles(root, func(rel string, data []byte) {
		if rel == "LICENSE" {
			return
		}
		lower := bytes.ToLower(data)
		for _, s := range forbiddenText {
			if bytes.Contains(lower, []byte(s)) {
				bad = append(bad, fmt.Sprintf("%s: contains %+q", rel, s))
			}
		}
	})
	return bad, err
}

func TestNoTrademarkNotices(t *testing.T) {
	probe := t.TempDir()
	writeFile(t, filepath.Join(probe, "README.md"), "Example Corp. All Rights "+"Reserved.\n")
	writeFile(t, filepath.Join(probe, "LICENSE"), "Trademarks \u00a9\n")
	got, err := noticeViolations(probe)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !strings.HasPrefix(got[0], "README.md:") {
		t.Fatalf("probe violations = %q, want exactly README.md", got)
	}

	got, err = noticeViolations(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range got {
		t.Errorf("trademark or rights notice: %s", v)
	}
}

func deployViolations(root string) ([]string, error) {
	var bad []string
	err := walkFiles(root, func(rel string, _ []byte) {
		switch base := filepath.Base(rel); {
		case base == "Dockerfile", strings.HasPrefix(base, "Dockerfile."),
			base == "docker-compose.yml", base == "docker-compose.yaml",
			base == "Chart.yaml":
			bad = append(bad, rel)
		}
	})
	return bad, err
}

func TestNoDeployManifests(t *testing.T) {
	probe := t.TempDir()
	writeFile(t, filepath.Join(probe, "deploy", "Chart.yaml"), "name: x\n")
	writeFile(t, filepath.Join(probe, "Dockerfile"), "FROM scratch\n")
	writeFile(t, filepath.Join(probe, "main.go"), "package main\n")
	got, err := deployViolations(probe)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"Dockerfile", "deploy/Chart.yaml"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("probe violations = %q, want %q", got, want)
	}

	got, err = deployViolations(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range got {
		t.Errorf("deploy manifest present: %s", v)
	}
}

func walkFiles(root string, visit func(rel string, data []byte)) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "third_party" {
				return filepath.SkipDir
			}
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		visit(filepath.ToSlash(rel), data)
		return nil
	})
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
