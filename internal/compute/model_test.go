package compute_test

import (
	"encoding/hex"
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
)

func TestFingerprintOf(t *testing.T) {
	run := func(typ string) compute.Fingerprint {
		return compute.FingerprintOf(ok(compute.NewRunInstance(imageID, typ, "deploy")))
	}
	first, retry := run("vws.small"), run("vws.small")
	if first != retry {
		t.Fatal("equal intents have different fingerprints")
	}
	if first == run("vws.medium") {
		t.Fatal("instance type does not change the fingerprint")
	}
	start := compute.FingerprintOf(ok(compute.NewStartInstance(instanceID)))
	stop := compute.FingerprintOf(ok(compute.NewStopInstance(instanceID)))
	if start == stop {
		t.Fatal("kind does not change the fingerprint")
	}
	withComment := compute.FingerprintOf(ok(compute.NewImportKeyPair("k", ed25519Line)))
	bare := compute.FingerprintOf(ok(compute.NewImportKeyPair("k", ed25519Bare)))
	if withComment != bare {
		t.Fatal("an authorized_keys comment changes the fingerprint")
	}
}

// Stored operations carry fingerprints, so changing the encoding breaks
// retries of requests admitted by an older binary.
func TestFingerprintIsStable(t *testing.T) {
	fp := compute.FingerprintOf(ok(compute.NewStopInstance(instanceID)))
	const want = "bc315f5ae58c5fd47a5370a0049c4763fbbf851dd0cde6c6fa354804591e85e7"
	if got := hex.EncodeToString(fp[:]); got != want {
		t.Fatalf("fingerprint = %s, want %s", got, want)
	}
}

func TestInstanceLifecycle(t *testing.T) {
	fail := &compute.Failure{Code: compute.CodeInternal, Message: "x"}
	cases := []struct {
		name    string
		power   compute.Power
		gen     int64
		obs     compute.Observed
		machine compute.MachineState
		want    compute.InstanceLifecycle
	}{
		{"never observed", compute.PowerRunning, 1, compute.Observed{}, compute.MachineUnknown, compute.InstancePending},
		{"running converged", compute.PowerRunning, 1, compute.Observed{Generation: 1}, compute.MachineOn, compute.InstanceRunning},
		{"reboot in flight", compute.PowerRunning, 3, compute.Observed{Generation: 2}, compute.MachineOn, compute.InstanceRunning},
		{"start in flight", compute.PowerRunning, 3, compute.Observed{Generation: 2}, compute.MachineOff, compute.InstancePending},
		{"stopping", compute.PowerStopped, 2, compute.Observed{Generation: 1}, compute.MachineOn, compute.InstanceStopping},
		{"stopped", compute.PowerStopped, 2, compute.Observed{Generation: 2}, compute.MachineOff, compute.InstanceStopped},
		{"shutting down", compute.PowerTerminated, 2, compute.Observed{Generation: 1}, compute.MachineOn, compute.InstanceShuttingDown},
		{"terminated", compute.PowerTerminated, 2, compute.Observed{Generation: 2}, compute.MachineAbsent, compute.InstanceTerminated},
		{"parked", compute.PowerRunning, 1, compute.Observed{Generation: 1, Failure: fail}, compute.MachineUnknown, compute.InstanceError},
		{"old failure, new generation", compute.PowerStopped, 2, compute.Observed{Generation: 1, Failure: fail}, compute.MachineUnknown, compute.InstanceStopping},
	}
	for _, c := range cases {
		i := compute.Instance{
			Spec:   compute.InstanceSpec{Generation: c.gen, Power: c.power},
			Status: compute.InstanceStatus{Observed: c.obs, Machine: c.machine},
		}
		if got := i.Lifecycle(); got != c.want {
			t.Errorf("%s: lifecycle = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestImageLifecycle(t *testing.T) {
	fail := &compute.Failure{Code: compute.CodeFailedPrecondition, Message: "404"}
	cases := []struct {
		name    string
		spec    compute.ImageSpec
		status  compute.ImageStatus
		want    compute.ImageLifecycle
		canLaun bool
	}{
		{"pending", compute.ImageSpec{Generation: 1}, compute.ImageStatus{}, compute.ImagePending, false},
		{"available", compute.ImageSpec{Generation: 1}, compute.ImageStatus{Observed: compute.Observed{Generation: 1}, Present: true}, compute.ImageAvailable, true},
		{"failed", compute.ImageSpec{Generation: 1}, compute.ImageStatus{Observed: compute.Observed{Generation: 1, Failure: fail}}, compute.ImageFailed, false},
		{"deregistering", compute.ImageSpec{Generation: 2, Deregistered: true}, compute.ImageStatus{Observed: compute.Observed{Generation: 1}, Present: true}, compute.ImagePending, false},
		{"deregistered", compute.ImageSpec{Generation: 2, Deregistered: true}, compute.ImageStatus{Observed: compute.Observed{Generation: 2}}, compute.ImageDeregistered, false},
		{"failed deregister", compute.ImageSpec{Generation: 2, Deregistered: true}, compute.ImageStatus{Observed: compute.Observed{Generation: 2, Failure: fail}}, compute.ImageFailed, false},
	}
	for _, c := range cases {
		img := compute.Image{Spec: c.spec, Status: c.status}
		if got := img.Lifecycle(); got != c.want || img.Available() != c.canLaun {
			t.Errorf("%s: lifecycle = %d available=%v, want %d/%v", c.name, got, img.Available(), c.want, c.canLaun)
		}
	}
}

func TestParseValues(t *testing.T) {
	valid := map[string]error{
		"request id":   errOf(compute.ParseRequestID("6f1c-retry_2")),
		"instance id":  errOf(compute.ParseInstanceID(instanceID)),
		"operation id": errOf(compute.ParseOperationID("op-0123456789abcdef0123")),
		"key name":     errOf(compute.ParseKeyName("deploy key #1")),
		"image url":    errOf(compute.ParseImageURL("https://images.example/a.qcow2")),
	}
	for name, err := range valid {
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	invalid := map[string]error{
		"empty request id":     errOf(compute.ParseRequestID("")),
		"long request id":      errOf(compute.ParseRequestID(string(make([]byte, 65)))),
		"request id with dot":  errOf(compute.ParseRequestID("a.b")),
		"uppercase hex id":     errOf(compute.ParseInstanceID("i-0123456789ABCDEF0")),
		"short id":             errOf(compute.ParseImageID("img-0123")),
		"control in key name":  errOf(compute.ParseKeyName("a\nb")),
		"url with credentials": errOf(compute.ParseImageURL("https://u:p@images.example/a")),
	}
	for name, err := range invalid {
		if compute.CodeOf(err) != compute.CodeInvalid {
			t.Errorf("%s: err = %v, want CodeInvalid", name, err)
		}
	}
	small := ok(compute.ParseInstanceType("vws.small"))
	if small.VCPUs() != 1 || small.MemoryMiB() != 1024 {
		t.Errorf("vws.small = %d/%d, want 1/1024", small.VCPUs(), small.MemoryMiB())
	}
}

func errOf[T any](_ T, err error) error { return err }
