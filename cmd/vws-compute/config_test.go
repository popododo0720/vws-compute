package main

import (
	"io"
	"strings"
	"testing"
)

func envOf(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestParseConfigDefaults(t *testing.T) {
	c, err := parseConfig(nil, envOf(nil), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.hypervisor != "fake" || c.vmNamespace != "vws-vms" || c.imageNamespace != "vws-images" || c.namespace != "" {
		t.Fatalf("defaults = %+v", c)
	}
}

func TestParseConfigEnvAndFlagPrecedence(t *testing.T) {
	env := envOf(map[string]string{
		"VWS_HYPERVISOR": "kubevirt", "VWS_STORE_NAMESPACE": "store", "VWS_VM_NAMESPACE": "vms-env", "VWS_IMAGE_NAMESPACE": "img-env",
	})
	c, err := parseConfig(nil, env, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.hypervisor != "kubevirt" || c.namespace != "store" || c.vmNamespace != "vms-env" || c.imageNamespace != "img-env" {
		t.Fatalf("env = %+v", c)
	}
	c, err = parseConfig([]string{"-vm-namespace", "vms-flag", "-hypervisor", "fake"}, env, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.vmNamespace != "vms-flag" || c.hypervisor != "fake" || c.imageNamespace != "img-env" {
		t.Fatalf("flag did not win: %+v", c)
	}
}

func TestParseConfigErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		env  map[string]string
		want string
	}{
		"unknown hypervisor":       {[]string{"-hypervisor", "xen"}, nil, "unknown -hypervisor"},
		"unknown hypervisor, env":  {nil, map[string]string{"VWS_HYPERVISOR": "xen"}, "unknown -hypervisor"},
		"kubevirt without store":   {[]string{"-hypervisor", "kubevirt"}, nil, "needs -namespace"},
		"kubevirt empty vm ns":     {[]string{"-hypervisor", "kubevirt", "-namespace", "s", "-vm-namespace", ""}, nil, "must not be empty"},
		"kubevirt empty image ns":  {[]string{"-hypervisor", "kubevirt", "-namespace", "s", "-image-namespace", ""}, nil, "must not be empty"},
		"volume namespace removed": {[]string{"-volume-namespace", "x"}, nil, "not defined"},
		"stray argument":           {[]string{"extra"}, nil, "unexpected argument"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseConfig(tc.args, envOf(tc.env), io.Discard)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestNewAgentSelectsTheHypervisor(t *testing.T) {
	a, err := newAgent(config{hypervisor: hypervisorFake})
	if err != nil || a == nil {
		t.Fatalf("fake agent = %v, %v", a, err)
	}
	_, err = newAgent(config{hypervisor: hypervisorKubeVirt, kubeconfig: t.TempDir() + "/missing", vmNamespace: "vms", imageNamespace: "img"})
	if err == nil {
		t.Fatal("kubevirt agent opened without a kubeconfig")
	}
}

// TestParseConfigRejectsMalformedValues feeds hostile flag and environment
// values; each must fail before anything listens or calls Kubernetes.
func TestParseConfigRejectsMalformedValues(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		env  map[string]string
	}{
		"listen without port":   {args: []string{"-listen", "localhost"}},
		"namespace traversal":   {args: []string{"-namespace", "../kube-system"}},
		"namespace upper":       {args: []string{"-namespace", "Store"}},
		"namespace too long":    {args: []string{"-namespace", strings.Repeat("a", 64)}},
		"namespace NUL":         {args: []string{"-namespace", "ns\x00x"}},
		"vm namespace from env": {args: []string{"-hypervisor", "kubevirt", "-namespace", "s"}, env: map[string]string{"VWS_VM_NAMESPACE": "vms/../x"}},
	} {
		t.Run(name, func(t *testing.T) {
			getenv := func(k string) string { return tc.env[k] }
			if _, err := parseConfig(tc.args, getenv, io.Discard); err == nil {
				t.Fatalf("parseConfig(%q) accepted a malformed value", tc.args)
			}
		})
	}
}

func TestRecoveredTurnsAPanicIntoAnError(t *testing.T) {
	err := recovered(func() error { panic("boom") })
	if err == nil || !strings.Contains(err.Error(), "panic: boom") {
		t.Fatalf("recovered = %v", err)
	}
	if err := recovered(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestParseConfigLegacyImageScope(t *testing.T) {
	c, err := parseConfig([]string{"-legacy-image-scope", "projects/a123456789012/regions/vws-1"}, envOf(nil), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.legacyImageScope.Project() != "a123456789012" || c.legacyImageScope.Region() != "vws-1" {
		t.Fatalf("scope = %+v", c.legacyImageScope)
	}
	if c, err = parseConfig(nil, envOf(nil), io.Discard); err != nil || !c.legacyImageScope.IsZero() {
		t.Fatalf("default scope = %+v, %v", c.legacyImageScope, err)
	}
	for _, bad := range []string{"a123456789012", "projects/a/regions/", "projects/a/zones/b"} {
		if _, err := parseConfig([]string{"-legacy-image-scope", bad}, envOf(nil), io.Discard); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}
