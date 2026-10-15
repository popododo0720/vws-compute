package kubevirt

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// imageRole reads the image namespace Role from the supplied RBAC manifest.
func imageRole(t *testing.T) rbacv1.Role {
	t.Helper()
	f, err := os.Open(filepath.Join("..", "..", "..", "deploy", "rbac", "kubevirt-role.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	d := utilyaml.NewYAMLOrJSONDecoder(f, 4096)
	for {
		var r rbacv1.Role
		if err := d.Decode(&r); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if r.Kind == "Role" && r.Name == "vws-compute-images" {
			return r
		}
	}
	t.Fatal("deploy/rbac/kubevirt-role.yaml has no Role vws-compute-images")
	return rbacv1.Role{}
}

// TestImageRoleLetsTheAgentLabelAnExistingDataSource pins the review of
// vws-compute#93: with only the supplied image Role, the agent adopts an
// existing managed DataSource without owner labels and labels it, which
// needs patch on datasources.
func TestImageRoleLetsTheAgentLabelAnExistingDataSource(t *testing.T) {
	admin, ns, _ := newClient(t)
	ctx := context.Background()
	role := imageRole(t)
	role.ObjectMeta = metav1.ObjectMeta{Namespace: ns, Name: role.Name}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "vws-compute"}}
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: role.Name},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: role.Name},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: sa.Name, Namespace: ns}},
	}
	for _, o := range []client.Object{&role, sa, rb} {
		if err := admin.Create(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	// A DataSource and DataVolume an older release made: managed, no owner.
	spec := urlImage()
	if _, err := imageAgent(admin, ns).ConvergeImage(ctx, spec); err == nil {
		t.Fatal("want a transient error while CDI imports")
	}
	cdiFinishes(t, admin, ns, imageID, "1Gi")

	cfg := rest.CopyConfig(env.Config)
	cfg.Impersonate = rest.ImpersonationConfig{UserName: "system:serviceaccount:" + ns + ":vws-compute"}
	limited, err := client.New(cfg, client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := limited.List(ctx, &unstructured.UnstructuredList{Object: map[string]any{"apiVersion": "cdi.kubevirt.io/v1beta1", "kind": "DataSourceList"}}, client.InNamespace(ns)); err == nil {
		t.Skip("envtest does not enforce RBAC here (list was allowed without a grant)")
	}
	spec.Project, spec.Region, spec.Public = "a123456789012", "vws-1", true
	if _, err := imageAgent(limited, ns).ConvergeImage(ctx, spec); err != nil {
		t.Fatalf("labeling with the supplied Role: %v", err)
	}
	if l := dataSourceLabels(t, admin, ns); l[LabelProject] != "a123456789012" || l[LabelPublic] != "true" {
		t.Fatalf("labels %v", l)
	}
	spec.Public = false
	if _, err := imageAgent(limited, ns).ConvergeImage(ctx, spec); err != nil {
		t.Fatalf("unlabeling with the supplied Role: %v", err)
	}
	if l := dataSourceLabels(t, admin, ns); l[LabelPublic] != "" {
		t.Fatalf("public label kept: %v", l)
	}
}
