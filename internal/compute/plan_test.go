package compute_test

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
)

const (
	instanceID = "i-0123456789abcdef0"
	imageID    = "img-0123456789abcdef0"
	keyPairID  = "key-0123456789abcdef0"
)

type view struct {
	instances map[compute.InstanceID]compute.Instance
	images    map[compute.ImageID]compute.Image
	keys      map[compute.KeyName]compute.KeyPair
}

func newView() *view {
	return &view{
		instances: map[compute.InstanceID]compute.Instance{},
		images:    map[compute.ImageID]compute.Image{},
		keys:      map[compute.KeyName]compute.KeyPair{},
	}
}

func (v *view) with(items ...any) *view {
	for _, it := range items {
		switch it := it.(type) {
		case compute.Instance:
			v.instances[it.Spec.ID] = it
		case compute.Image:
			v.images[it.Spec.ID] = it
		case compute.KeyPair:
			v.keys[it.Spec.Name] = it
		}
	}
	return v
}

func (v *view) Instance(id compute.InstanceID) (compute.Instance, error) {
	if i, ok := v.instances[id]; ok {
		return i, nil
	}
	return compute.Instance{}, compute.Errorf(compute.CodeNotFound, "instance %s not found", id)
}

func (v *view) Image(id compute.ImageID) (compute.Image, error) {
	if i, ok := v.images[id]; ok {
		return i, nil
	}
	return compute.Image{}, compute.Errorf(compute.CodeNotFound, "image %s not found", id)
}

func (v *view) KeyPairByName(n compute.KeyName) (compute.KeyPair, error) {
	if k, ok := v.keys[n]; ok {
		return k, nil
	}
	return compute.KeyPair{}, compute.Errorf(compute.CodeNotFound, "key pair %q not found", n)
}

func ok[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func mustRequest(_ *testing.T, s string) compute.RequestID {
	return ok(compute.ParseRequestID(s))
}

func availableImage() compute.Image {
	id, _ := compute.ParseImageID(imageID)
	name, _ := compute.ParseImageName("debian-13")
	u, _ := compute.ParseImageURL("https://images.example/debian-13.qcow2")
	return compute.Image{
		Spec:   compute.ImageSpec{ID: id, Generation: 1, Name: name, Source: compute.FromURL{URL: u}},
		Status: compute.ImageStatus{Observed: compute.Observed{Generation: 1}, Present: true},
	}
}

func runningInstance() compute.Instance {
	id, _ := compute.ParseInstanceID(instanceID)
	img, _ := compute.ParseImageID(imageID)
	typ, _ := compute.ParseInstanceType("vws.small")
	return compute.Instance{
		Spec:   compute.InstanceSpec{ID: id, Generation: 1, Image: img, Type: typ, Power: compute.PowerRunning},
		Status: compute.InstanceStatus{Observed: compute.Observed{Generation: 1}, Machine: compute.MachineOn},
	}
}

func instanceWith(power compute.Power, mutate func(*compute.Instance)) compute.Instance {
	i := runningInstance()
	i.Spec.Power = power
	if mutate != nil {
		mutate(&i)
	}
	return i
}

func keyPair(t *testing.T, name string, deleted bool) compute.KeyPair {
	return compute.KeyPair{Spec: compute.KeyPairSpec{
		ID:         ok(compute.ParseKeyPairID(keyPairID)),
		Generation: 1,
		Name:       ok(compute.ParseKeyName(name)),
		PublicKey:  ok(compute.ParsePublicKey(ed25519Line)),
		Deleted:    deleted,
	}}
}

var parked = &compute.Failure{Code: compute.CodeFailedPrecondition, Message: "agent refused"}

func plan(t *testing.T, v compute.View, in compute.Intent, err error) (compute.Change, error) {
	t.Helper()
	if err != nil {
		t.Fatalf("building intent: %v", err)
	}
	return compute.Plan(v, compute.Env{Now: t0, Rand: rand.Reader}, in)
}

func TestPlanRejects(t *testing.T) {
	failedImage := availableImage()
	failedImage.Status = compute.ImageStatus{Observed: compute.Observed{Generation: 1, Failure: parked}}
	pendingImage := availableImage()
	pendingImage.Status = compute.ImageStatus{}
	goneImage := availableImage()
	goneImage.Spec.Deregistered, goneImage.Spec.Generation, goneImage.Status.Observed.Generation = true, 2, 2
	terminated := instanceWith(compute.PowerTerminated, nil)
	stopped := instanceWith(compute.PowerStopped, nil)

	cases := []struct {
		name string
		v    *view
		in   func() (compute.Intent, error)
		want compute.Code
	}{
		{"run with missing image", newView(), func() (compute.Intent, error) { return compute.NewRunInstance(imageID, "vws.small", "") }, compute.CodeNotFound},
		{"run with pending image", newView().with(pendingImage), func() (compute.Intent, error) { return compute.NewRunInstance(imageID, "vws.small", "") }, compute.CodeFailedPrecondition},
		{"run with failed image", newView().with(failedImage), func() (compute.Intent, error) { return compute.NewRunInstance(imageID, "vws.small", "") }, compute.CodeFailedPrecondition},
		{"run with deregistered image", newView().with(goneImage), func() (compute.Intent, error) { return compute.NewRunInstance(imageID, "vws.small", "") }, compute.CodeFailedPrecondition},
		{"run with missing key", newView().with(availableImage()), func() (compute.Intent, error) { return compute.NewRunInstance(imageID, "vws.small", "nope") }, compute.CodeNotFound},
		{"run with deleted key", newView().with(availableImage(), keyPair(t, "old", true)), func() (compute.Intent, error) { return compute.NewRunInstance(imageID, "vws.small", "old") }, compute.CodeNotFound},
		{"start terminated", newView().with(terminated), func() (compute.Intent, error) { return compute.NewStartInstance(instanceID) }, compute.CodeFailedPrecondition},
		{"stop terminated", newView().with(terminated), func() (compute.Intent, error) { return compute.NewStopInstance(instanceID) }, compute.CodeFailedPrecondition},
		{"reboot terminated", newView().with(terminated), func() (compute.Intent, error) { return compute.NewRebootInstance(instanceID) }, compute.CodeFailedPrecondition},
		{"reboot stopped", newView().with(stopped), func() (compute.Intent, error) { return compute.NewRebootInstance(instanceID) }, compute.CodeFailedPrecondition},
		{"stop missing instance", newView(), func() (compute.Intent, error) { return compute.NewStopInstance(instanceID) }, compute.CodeNotFound},
		{"create image from terminated", newView().with(terminated), func() (compute.Intent, error) { return compute.NewCreateImage(instanceID, "snap-1") }, compute.CodeFailedPrecondition},
		{"create key with live name", newView().with(keyPair(t, "deploy", false)), func() (compute.Intent, error) { return compute.NewCreateKeyPair("deploy") }, compute.CodeAlreadyExists},
		{"import key with live name", newView().with(keyPair(t, "deploy", false)), func() (compute.Intent, error) {
			return compute.NewImportKeyPair("deploy", rsa2048Line, compute.Labels{})
		}, compute.CodeAlreadyExists},
		{"delete never-stored key", newView(), func() (compute.Intent, error) { return compute.NewDeleteKeyPair("ghost") }, compute.CodeNotFound},
		{"file over deregistered image", newView().with(goneImage), func() (compute.Intent, error) {
			return compute.NewRegisterFile(imageID, "/var/lib/vws/images/debian-13.qcow2", "debian-13")
		}, compute.CodeAlreadyExists},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in, err := c.in()
			_, err = plan(t, c.v, in, err)
			if got := compute.CodeOf(err); got != c.want {
				t.Fatalf("err = %v (%s), want %s", err, got, c.want)
			}
		})
	}
}

func TestIntentConstructorsValidate(t *testing.T) {
	cases := map[string]func() (compute.Intent, error){
		"missing instance type": func() (compute.Intent, error) { return compute.NewRunInstance(imageID, "", "") },
		"unknown instance type": func() (compute.Intent, error) { return compute.NewRunInstance(imageID, "vws.huge", "") },
		"bad image id":          func() (compute.Intent, error) { return compute.NewRunInstance("9debian", "vws.small", "") },
		"bad image path": func() (compute.Intent, error) {
			return compute.NewRegisterFile("debian-12", "images/debian-12.qcow2", "debian-12")
		},
		"bad copy source": func() (compute.Intent, error) {
			return compute.NewCopyImage("debian-12", testImageScope, nil, "", "", compute.Labels{})
		},
		"bad copy id": func() (compute.Intent, error) {
			vol := ok(compute.ParseVolumeRef("projects/demo/zones/demo/volumes/root"))
			return compute.NewCopyImage(
				"9debian", testImageScope, compute.FromVolume{Ref: vol}, "", "", compute.Labels{},
			)
		},
		"bad instance id": func() (compute.Intent, error) { return compute.NewStopInstance("i-XYZ") },
		"http image url": func() (compute.Intent, error) {
			return compute.NewRegisterImage("debian-13", "http://images.example/d.qcow2")
		},
		"short image name": func() (compute.Intent, error) { return compute.NewRegisterImage("d", "https://images.example/d.qcow2") },
		"empty key name":   func() (compute.Intent, error) { return compute.NewCreateKeyPair("") },
		"weak rsa import":  func() (compute.Intent, error) { return compute.NewImportKeyPair("weak", rsa1024Line, compute.Labels{}) },
	}
	for name, build := range cases {
		if _, err := build(); compute.CodeOf(err) != compute.CodeInvalid {
			t.Errorf("%s: err = %v, want CodeInvalid", name, err)
		}
	}
}

func TestPlanRunInstance(t *testing.T) {
	v := newView().with(availableImage(), keyPair(t, "deploy", false))
	ch, err := plan(t, v, ok(compute.NewRunInstance(imageID, "vws.medium", "deploy")), nil)
	if err != nil {
		t.Fatal(err)
	}
	spec, ok := ch.Spec.(compute.InstanceSpec)
	if !ok || !ch.Write {
		t.Fatalf("change = %+v, want an instance write", ch)
	}
	if spec.Generation != 1 || spec.RebootEpoch != 0 || spec.Power != compute.PowerRunning {
		t.Fatalf("spec gen=%d epoch=%d power=%d, want 1/0/running", spec.Generation, spec.RebootEpoch, spec.Power)
	}
	if spec.Type.Name() != "vws.medium" || spec.Type.VCPUs() != 2 || spec.Type.MemoryMiB() != 4096 {
		t.Fatalf("type = %s %d/%d", spec.Type.Name(), spec.Type.VCPUs(), spec.Type.MemoryMiB())
	}
	if spec.Key == nil || spec.Key.PublicKey.Fingerprint() != ed25519SSHKeyFP || spec.Key.Name.String() != "deploy" {
		t.Fatalf("key = %+v, want deploy's public key copied", spec.Key)
	}
	if spec.Image.String() != imageID || spec.ID.IsZero() {
		t.Fatalf("image=%s id=%s", spec.Image, spec.ID)
	}
}

func TestPlanTransitions(t *testing.T) {
	rebootPending := instanceWith(compute.PowerRunning, func(i *compute.Instance) {
		i.Spec.Generation, i.Spec.RebootEpoch = 2, 1
	})
	parkedRunning := instanceWith(compute.PowerRunning, func(i *compute.Instance) {
		i.Status.Observed.Failure = parked
	})
	stopped := instanceWith(compute.PowerStopped, func(i *compute.Instance) { i.Status.Machine = compute.MachineOff })
	terminated := instanceWith(compute.PowerTerminated, nil)
	deregistered := availableImage()
	deregistered.Spec.Deregistered, deregistered.Spec.Generation, deregistered.Status.Observed.Generation = true, 2, 2

	cases := []struct {
		name      string
		v         *view
		in        compute.Intent
		write     bool
		gen       int64
		bornState compute.OperationState
		check     func(t *testing.T, s compute.Spec)
	}{
		{"start running converged is born succeeded", newView().with(runningInstance()), ok(compute.NewStartInstance(instanceID)), false, 1, compute.OperationSucceeded, nil},
		{"stop stopped converged is born succeeded", newView().with(stopped), ok(compute.NewStopInstance(instanceID)), false, 1, compute.OperationSucceeded, nil},
		{"terminate terminated is born succeeded", newView().with(terminated), ok(compute.NewTerminateInstance(instanceID)), false, 1, compute.OperationSucceeded, nil},
		{"stop running bumps", newView().with(runningInstance()), ok(compute.NewStopInstance(instanceID)), true, 2, compute.OperationPending, func(t *testing.T, s compute.Spec) {
			if p := s.(compute.InstanceSpec).Power; p != compute.PowerStopped {
				t.Fatalf("power = %d", p)
			}
		}},
		{"start stopped bumps", newView().with(stopped), ok(compute.NewStartInstance(instanceID)), true, 2, compute.OperationPending, nil},
		{"terminate stopped bumps", newView().with(stopped), ok(compute.NewTerminateInstance(instanceID)), true, 2, compute.OperationPending, nil},
		{"start parked running retries with a bump", newView().with(parkedRunning), ok(compute.NewStartInstance(instanceID)), true, 2, compute.OperationPending, nil},
		{"reboot converged bumps epoch", newView().with(runningInstance()), ok(compute.NewRebootInstance(instanceID)), true, 2, compute.OperationPending, func(t *testing.T, s compute.Spec) {
			if e := s.(compute.InstanceSpec).RebootEpoch; e != 1 {
				t.Fatalf("epoch = %d, want 1", e)
			}
		}},
		{"reboot during an unapplied reboot bumps again", newView().with(rebootPending), ok(compute.NewRebootInstance(instanceID)), true, 3, compute.OperationPending, func(t *testing.T, s compute.Spec) {
			if e := s.(compute.InstanceSpec).RebootEpoch; e != 2 {
				t.Fatalf("epoch = %d, want 2", e)
			}
		}},
		{"deregister available bumps", newView().with(availableImage()), ok(compute.NewDeregisterImage(imageID)), true, 2, compute.OperationPending, nil},
		{"deregister deregistered is born succeeded", newView().with(deregistered), ok(compute.NewDeregisterImage(imageID)), false, 2, compute.OperationSucceeded, nil},
		{"create image from running", newView().with(runningInstance()), ok(compute.NewCreateImage(instanceID, "snap-1")), true, 1, compute.OperationPending, func(t *testing.T, s compute.Spec) {
			src, ok := s.(compute.ImageSpec).Source.(compute.FromInstance)
			if !ok || src.Instance.String() != instanceID {
				t.Fatalf("source = %#v", s.(compute.ImageSpec).Source)
			}
		}},
		{"delete live key tombs it", newView().with(keyPair(t, "deploy", false)), ok(compute.NewDeleteKeyPair("deploy")), true, 2, compute.OperationSucceeded, func(t *testing.T, s compute.Spec) {
			if !s.(compute.KeyPairSpec).Deleted {
				t.Fatal("not deleted")
			}
		}},
		{"delete tombstone is a no-op", newView().with(keyPair(t, "deploy", true)), ok(compute.NewDeleteKeyPair("deploy")), false, 1, compute.OperationSucceeded, nil},
		{"create key over tombstone", newView().with(keyPair(t, "deploy", true)), ok(compute.NewCreateKeyPair("deploy")), true, 1, compute.OperationSucceeded, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ch, err := plan(t, c.v, c.in, nil)
			if err != nil {
				t.Fatal(err)
			}
			if ch.Write != c.write || ch.Spec.Gen() != c.gen {
				t.Fatalf("write=%v gen=%d, want write=%v gen=%d", ch.Write, ch.Spec.Gen(), c.write, c.gen)
			}
			if c.check != nil {
				c.check(t, ch.Spec)
			}
			if got := bornState(t, c.v, c.in, ch); got != c.bornState {
				t.Fatalf("operation born %s, want %s", got, c.bornState)
			}
		})
	}
}

// bornState is the state CommitDesired would insert: Settle against the
// target's generation after the write and its observed state before it.
func bornState(t *testing.T, v *view, in compute.Intent, ch compute.Change) compute.OperationState {
	t.Helper()
	op := ok(compute.NewOperation(compute.Env{Now: t0, Rand: rand.Reader}, mustRequest(t, "r"), in, ch))
	var obs compute.Observed
	switch s := ch.Spec.(type) {
	case compute.InstanceSpec:
		obs = v.instances[s.ID].Status.Observed
	case compute.ImageSpec:
		obs = v.images[s.ID].Status.Observed
	case compute.KeyPairSpec:
		obs = compute.KeyPair{Spec: s}.Observed()
	}
	return compute.Settle(op, ch.Spec.Gen(), obs, t0.Add(time.Second)).State()
}

func TestPlanCopyAndFileImage(t *testing.T) {
	vol := ok(compute.ParseVolumeRef("projects/demo/zones/demo/volumes/root"))
	in := ok(compute.NewCopyImage(
		"debian-12", testImageScope, compute.FromVolume{Ref: vol}, "debian-12", "a disk", compute.Labels{},
	))
	ch, err := plan(t, unreadImages{}, in, nil)
	if err != nil {
		t.Fatal(err)
	}
	spec := ch.Spec.(compute.ImageSpec)
	src, isVol := spec.Source.(compute.FromVolume)
	if !ch.Write || !mintedImageID(spec.ID) || spec.ID.String() == "debian-12" || spec.Scope != testImageScope ||
		spec.Name.String() != "debian-12" || !isVol || src.Ref != vol {
		t.Fatalf("copy = %+v", spec)
	}
	if ch.Meta == nil || ch.Meta.Description.String() != "a disk" || ch.Meta.Revision != 0 {
		t.Fatalf("meta = %+v", ch.Meta)
	}

	held := availableImage()
	taken := ok(compute.NewCopyImage(
		held.Spec.ID.String(), testImageScope, compute.FromVolume{Ref: vol}, "debian-12", "", compute.Labels{},
	))
	again, err := plan(t, newView().with(held), taken, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := again.Spec.(compute.ImageSpec).ID; !mintedImageID(got) || got == held.Spec.ID {
		t.Fatalf("copy of a held id = %s", got)
	}

	file, err := plan(t, newView(), ok(compute.NewRegisterFile("debian-12", "/var/lib/vws/images/debian-12.qcow2", "debian-12")), nil)
	if err != nil {
		t.Fatal(err)
	}
	got := file.Spec.(compute.ImageSpec).Source.(compute.FromFile)
	if file.Spec.(compute.ImageSpec).ID.String() != "debian-12" ||
		got.Path.String() != "/var/lib/vws/images/debian-12.qcow2" || file.Meta != nil {
		t.Fatalf("file = %+v meta %+v", file.Spec, file.Meta)
	}
}

// unreadImages fails any image lookup. copyImage.plan must not call it.
type unreadImages struct{ compute.View }

func (unreadImages) Image(compute.ImageID) (compute.Image, error) {
	return compute.Image{}, compute.Errorf(compute.CodeInternal, "copy does not read images")
}

func mintedImageID(id compute.ImageID) bool {
	s := id.String()
	if len(s) != len("img-")+17 || s[:4] != "img-" {
		return false
	}
	for _, c := range s[4:] {
		if c < '0' || (c > '9' && c < 'a') || c > 'f' {
			return false
		}
	}
	return true
}

var testImageScope, _ = compute.NewImageScope("demo", "demo")
