package archtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
)

const module = "github.com/popododo0720/vws-compute"

// testSupport lists non-test packages that exist only to serve tests. They
// are exempt from the production rules, and no other non-test package may
// import "testing".
var testSupport = []string{"internal/store/storetest", "internal/store/kubestore/kubetest"}

func isModule(imp string) bool { return imp == module || strings.HasPrefix(imp, module+"/") }

func under(dir string, roots ...string) bool {
	for _, r := range roots {
		if dir == r || strings.HasPrefix(dir, r+"/") {
			return true
		}
	}
	return false
}

func hasPrefix(prefixes ...string) func(string) bool {
	return func(s string) bool {
		for _, p := range prefixes {
			if s == p || strings.HasPrefix(s, p+"/") {
				return true
			}
		}
		return false
	}
}

// packageRules forbid imports inside one package directory.
var packageRules = []struct {
	dir    string
	forbid func(imp string) bool
	why    string
}{
	{"internal/hypervisor", isModule, "the agent contract imports nothing from this module"},
	{
		"internal/hypervisor/fakeagent",
		func(imp string) bool { return isModule(imp) && imp != module+"/internal/hypervisor" },
		"the fake agent sees only the agent contract",
	},
	{
		"internal/hypervisor/kubevirt",
		func(imp string) bool { return isModule(imp) && imp != module+"/internal/hypervisor" },
		"the KubeVirt agent sees only the agent contract",
	},
	{"internal/compute", func(imp string) bool {
		return isModule(imp) || hasPrefix("google.golang.org/grpc", "google.golang.org/protobuf",
			"google.golang.org/genproto", "database/sql", "modernc.org/sqlite")(imp)
	}, "the domain imports no transport, storage, or hypervisor"},
	{"internal/controller", hasPrefix(module + "/internal/hypervisor"), "Submit never reaches a hypervisor"},
}

// ownerRules confine an import to the directories that own it.
var ownerRules = []struct {
	match  func(imp string) bool
	owners []string
	why    string
}{
	{hasPrefix("database/sql", "modernc.org/sqlite"), nil, "no SQL: state lives in ConfigMaps"},
	{
		hasPrefix(
			"google.golang.org/protobuf",
			"google.golang.org/genproto",
			"github.com/popododo0720/vws-proto",
			"cloud.google.com/go/longrunning",
		),
		[]string{"internal/grpcapi"},
		"wire types stay in grpcapi",
	},
	{hasPrefix("google.golang.org/grpc"), []string{"internal/grpcapi", "cmd/vws-compute"}, "gRPC stays at the edge"},
	{hasPrefix("k8s.io", "sigs.k8s.io"), []string{"internal/hypervisor/kubevirt", "internal/store/kubestore"}, "Kubernetes client types stay in the KubeVirt agent and the ConfigMap store"},
	{func(imp string) bool { return imp == "testing" }, testSupport, "production code does not import testing"},
}

// selectorRules confine every reference to a method name, call or method
// value, to the directories allowed to use it.
var selectorRules = []struct {
	names  []string
	owners []string
	why    string
}{
	{[]string{"CommitDesired"}, []string{"internal/controller"}, "only the controller writes desired state"},
	{[]string{"CommitInstanceStatus", "CommitImageStatus"}, []string{"internal/reconciler"}, "only the reconciler writes status"},
	{[]string{"ConvergeMachine", "ConvergeImage"}, []string{"internal/reconciler"}, "only the reconciler calls the agent"},
	{[]string{"ReportInstanceStatus"}, []string{"internal/grpcapi", "internal/reconciler"}, "only the report RPC and the reconciler write controller-reported status"},
	{[]string{"RestoreOperation"}, []string{"internal/store/kubestore"}, "only storage rehydrates operations"},
}

// exportedTypes lists, per package, the only exported type names allowed.
var exportedTypes = map[string][]string{
	"internal/store/kubestore": {"Store", "Repaired"},
}

// constrainedDirs are the directories a rule constrains by name. Renaming
// one would silently disable its rule, so each must exist.
func constrainedDirs() []string {
	var dirs []string
	for _, r := range packageRules {
		dirs = append(dirs, r.dir)
	}
	for d := range exportedTypes {
		dirs = append(dirs, d)
	}
	return dirs
}

// archViolations applies every rule to the non-test files in pkgs, keyed by
// slash-separated directory relative to the module root.
func archViolations(pkgs map[string][]*ast.File) []string {
	var bad []string
	for dir, files := range pkgs {
		if under(dir, testSupport...) {
			continue
		}
		for _, f := range files {
			for _, spec := range f.Imports {
				imp, _ := strconv.Unquote(spec.Path.Value)
				for _, r := range packageRules {
					if dir == r.dir && r.forbid(imp) {
						bad = append(bad, fmt.Sprintf("%s imports %s: %s", dir, imp, r.why))
					}
				}
				for _, r := range ownerRules {
					if r.match(imp) && !under(dir, r.owners...) {
						bad = append(bad, fmt.Sprintf("%s imports %s: %s", dir, imp, r.why))
					}
				}
			}
			ast.Inspect(f, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				for _, r := range selectorRules {
					for _, name := range r.names {
						if sel.Sel.Name == name && !under(dir, r.owners...) {
							bad = append(bad, fmt.Sprintf("%s uses %s: %s", dir, name, r.why))
						}
					}
				}
				return true
			})
			if allowed, ok := exportedTypes[dir]; ok {
				for _, d := range f.Decls {
					g, ok := d.(*ast.GenDecl)
					if !ok || g.Tok != token.TYPE {
						continue
					}
					for _, s := range g.Specs {
						name := s.(*ast.TypeSpec).Name.Name
						if ast.IsExported(name) && !contains(allowed, name) {
							bad = append(bad, fmt.Sprintf("%s exports type %s: storage rows stay unexported", dir, name))
						}
					}
				}
			}
		}
	}
	sort.Strings(bad)
	return bad
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func loadProduction(t *testing.T, root string) map[string][]*ast.File {
	t.Helper()
	fset := token.NewFileSet()
	pkgs := map[string][]*ast.File{}
	err := walkFiles(root, func(rel string, data []byte) {
		if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") {
			return
		}
		f, err := parser.ParseFile(fset, rel, data, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		dir := filepath.ToSlash(filepath.Dir(rel))
		pkgs[dir] = append(pkgs[dir], f)
	})
	if err != nil {
		t.Fatal(err)
	}
	return pkgs
}

func TestArchitectureRules(t *testing.T) {
	probe := t.TempDir()
	for rel, src := range map[string]string{
		"internal/compute/c.go":             `package compute; import _ "database/sql"`,
		"internal/hypervisor/h.go":          `package hypervisor; import _ "` + module + `/internal/compute"`,
		"internal/controller/c.go":          `package controller; import _ "` + module + `/internal/hypervisor"; func f(s S) { s.CommitDesired() }`,
		"internal/grpcapi/s.go":             `package grpcapi; func f(s S) { g := s.CommitDesired; _ = g; s.ReportInstanceStatus() }`,
		"internal/reconciler/n.go":          `package reconciler; func g(s S) { s.ReportInstanceStatus() }`,
		"internal/reconciler/r.go":          `package reconciler; func f(s S, a A) { s.CommitImageStatus(); a.ConvergeMachine() }`,
		"internal/widget/k.go":              `package widget; import _ "k8s.io/apimachinery/pkg/types"`,
		"internal/hypervisor/kubevirt/k.go": `package kubevirt; import _ "` + module + `/internal/compute"`,
		"internal/widget/w.go":              `package widget; import "testing"; var _ testing.TB`,
		"internal/store/storetest/s.go":     `package storetest; import "testing"; func f(s S, _ *testing.T) { s.CommitInstanceStatus() }`,
		"internal/store/kubestore/r.go":     `package kubestore; type Store struct{}; type InstanceRow struct{}`,
		"internal/store/memstore/m_test.go": `package memstore; import "testing"`,
	} {
		writeFile(t, filepath.Join(probe, rel), src)
	}
	want := []string{
		"internal/compute imports database/sql: the domain imports no transport, storage, or hypervisor",
		"internal/compute imports database/sql: no SQL: state lives in ConfigMaps",
		"internal/controller imports " + module + "/internal/hypervisor: Submit never reaches a hypervisor",
		"internal/grpcapi uses CommitDesired: only the controller writes desired state",
		"internal/hypervisor imports " + module + "/internal/compute: the agent contract imports nothing from this module",
		"internal/store/kubestore exports type InstanceRow: storage rows stay unexported",
		"internal/widget imports k8s.io/apimachinery/pkg/types: Kubernetes client types stay in the KubeVirt agent and the ConfigMap store",
		"internal/hypervisor/kubevirt imports " + module + "/internal/compute: the KubeVirt agent sees only the agent contract",
		"internal/widget imports testing: production code does not import testing",
	}
	sort.Strings(want)
	if got := archViolations(loadProduction(t, probe)); !reflect.DeepEqual(got, want) {
		t.Fatalf("probe violations:\n got %q\nwant %q", got, want)
	}

	root := repoRoot(t)
	for _, dir := range constrainedDirs() {
		if _, err := os.Stat(filepath.Join(root, dir)); err != nil {
			t.Errorf("rule directory %s: %v", dir, err)
		}
	}
	for _, v := range archViolations(loadProduction(t, root)) {
		t.Error(v)
	}
}

func TestOperationStateIsUnexported(t *testing.T) {
	typ := reflect.TypeOf(compute.Operation{})
	guarded := map[reflect.Type]bool{
		reflect.TypeOf(compute.OperationPending): true,
		reflect.TypeOf(&compute.Failure{}):       true,
	}
	found := 0
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if guarded[f.Type] || f.Name == "settledAt" {
			found++
			if f.IsExported() {
				t.Errorf("Operation.%s is exported: only compute.Settle may change an operation's state", f.Name)
			}
		}
	}
	if found != 3 {
		t.Fatalf("found %d state fields on Operation, want state, failure, and settledAt", found)
	}
}
