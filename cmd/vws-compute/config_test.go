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
	if c.imageStorageClass != "ceph-block" || c.imageAccessMode != "ReadWriteOnce" {
		t.Fatalf("image storage defaults = %q, %q", c.imageStorageClass, c.imageAccessMode)
	}
}

func TestParseConfigEnvAndFlagPrecedence(t *testing.T) {
	env := envOf(map[string]string{
		"VWS_HYPERVISOR": "kubevirt", "VWS_STORE_NAMESPACE": "store", "VWS_VM_NAMESPACE": "vms-env", "VWS_IMAGE_NAMESPACE": "img-env",
		"VWS_IMAGE_STORAGE_CLASS": "fast.block", "VWS_IMAGE_ACCESS_MODE": "ReadWriteMany",
	})
	c, err := parseConfig(nil, env, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.hypervisor != "kubevirt" || c.namespace != "store" || c.vmNamespace != "vms-env" || c.imageNamespace != "img-env" {
		t.Fatalf("env = %+v", c)
	}
	if c.imageStorageClass != "fast.block" || c.imageAccessMode != "ReadWriteMany" {
		t.Fatalf("image storage env = %q, %q", c.imageStorageClass, c.imageAccessMode)
	}
	c, err = parseConfig([]string{
		"-vm-namespace", "vms-flag", "-hypervisor", "fake",
		"-image-storage-class", "slow", "-image-access-mode", "ReadOnlyMany",
	}, env, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.vmNamespace != "vms-flag" || c.hypervisor != "fake" || c.imageNamespace != "img-env" {
		t.Fatalf("flag did not win: %+v", c)
	}
	if c.imageStorageClass != "slow" || c.imageAccessMode != "ReadOnlyMany" {
		t.Fatalf("image storage flag did not win: %q, %q", c.imageStorageClass, c.imageAccessMode)
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
		"storage class upper":   {args: []string{"-image-storage-class", "Ceph-Block"}},
		"storage class empty":   {args: []string{"-image-storage-class", ""}},
		"storage class dot":     {args: []string{"-image-storage-class", "ceph..block"}},
		"storage class long":    {args: []string{"-image-storage-class", strings.Repeat("a", 254)}},
		"access mode unknown":   {args: []string{"-image-access-mode", "ReadWrite"}},
		"access mode empty":     {args: []string{"-image-access-mode", ""}},
	} {
		t.Run(name, func(t *testing.T) {
			getenv := func(k string) string { return tc.env[k] }
			if _, err := parseConfig(tc.args, getenv, io.Discard); err == nil {
				t.Fatalf("parseConfig(%q) accepted a malformed value", tc.args)
			}
		})
	}
}

// TestValidStorageClass checks the DNS subdomain shape a StorageClass name
// has. Kubernetes caps the whole name at 253 characters and does not cap a
// dot-separated part at 63.
func TestValidStorageClass(t *testing.T) {
	name253 := strings.Join([]string{
		strings.Repeat("a", 64), strings.Repeat("b", 64),
		strings.Repeat("c", 64), strings.Repeat("d", 58),
	}, ".")
	for name, tc := range map[string]struct {
		value string
		want  bool
	}{
		"simple":          {"ceph-block", true},
		"dotted":          {"fast.block", true},
		"part over 63":    {strings.Repeat("a", 64) + ".block", true},
		"253 characters":  {name253, true},
		"254 characters":  {strings.Repeat("a", 254), false},
		"empty":           {"", false},
		"empty part":      {"ceph..block", false},
		"leading dot":     {".ceph", false},
		"trailing dot":    {"ceph.", false},
		"uppercase":       {"Ceph-Block", false},
		"underscore":      {"ceph_block", false},
		"leading hyphen":  {"-ceph", false},
		"trailing hyphen": {"ceph-", false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := validStorageClass(tc.value); got != tc.want {
				t.Fatalf("validStorageClass(%q) = %v, want %v", tc.value, got, tc.want)
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

func TestParseConfigPublicImages(t *testing.T) {
	c, err := parseConfig(nil, envOf(map[string]string{"VWS_PUBLIC_IMAGES": "ami-0123456789abcdef0, ami-0ab0b1240a0000001"}), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.publicImages) != 2 {
		t.Fatalf("public = %v", c.publicImages)
	}
	if c, err = parseConfig(nil, envOf(nil), io.Discard); err != nil || len(c.publicImages) != 0 {
		t.Fatalf("default public = %v, %v", c.publicImages, err)
	}
	if _, err := parseConfig([]string{"-public-images", "not an id"}, envOf(nil), io.Discard); err == nil {
		t.Fatal("bad id accepted")
	}
}
