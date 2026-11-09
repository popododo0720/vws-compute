package compute_test

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// TestDeleteOnTerminationChangeKeepsTheAttachment pins vws-compute#173: a
// change of DeleteOnTermination alone is a new generation that stores the
// new value and keeps the entry's AttachTime, since the volume never left.
// The same update again writes nothing.
func TestDeleteOnTerminationChangeKeepsTheAttachment(t *testing.T) {
	at := func(now time.Time) compute.Env { return compute.Env{Now: now, Rand: rand.Reader} }
	p := createParams()
	p.Volumes = append(p.Volumes, vol("data", "vdb"))
	spec := ok(compute.Plan(newView(), at(t0), ok(compute.NewCreateInstance(p)))).Spec.(compute.InstanceSpec)

	root, data := p.Volumes[0], p.Volumes[1]
	root.DeleteOnTermination = false
	data.DeleteOnTermination = true
	in := ok(
		compute.NewUpdateInstanceSpec(
			"web-1",
			compute.PowerRunning,
			[]compute.VolumeAttachment{root, data},
			compute.MetadataPatch{},
		),
	)
	ch, err := compute.Plan(newView().with(compute.Instance{Spec: spec}), at(t0.Add(time.Minute)), in)
	if err != nil || !ch.Write {
		t.Fatalf("flip = write %v err %v, want a write", ch.Write, err)
	}
	next := ch.Spec.(compute.InstanceSpec)
	if next.Generation != spec.Generation+1 {
		t.Fatalf("generation = %d, want %d", next.Generation, spec.Generation+1)
	}
	if next.Volumes[0].DeleteOnTermination || !next.Volumes[1].DeleteOnTermination {
		t.Fatalf("volumes = %+v, want root kept and data deleted on termination", next.Volumes)
	}
	if !attachTimeOf(t, next, "root").Equal(t0) || !attachTimeOf(t, next, "data").Equal(t0) {
		t.Fatalf("a DeleteOnTermination change moved attach times: %+v", next.Volumes)
	}
	again, err := compute.Plan(newView().with(compute.Instance{Spec: next}), at(t0.Add(2*time.Minute)), in)
	if err != nil || again.Write {
		t.Fatalf("repeat = write %v err %v, want no write", again.Write, err)
	}
}

// TestCreateDeleteOnTerminationIsPartOfTheRequest: a create that differs
// only in DeleteOnTermination has another fingerprint and is AlreadyExists
// on the live instance, while the EC2 default keeps the fingerprint every
// create had before the field (TestCreateFingerprintWithoutAnnotationsIsUnchanged).
func TestCreateDeleteOnTerminationIsPartOfTheRequest(t *testing.T) {
	plain := createParams()
	plain.Volumes = append(plain.Volumes, vol("data", "vdb"))
	kept := createParams()
	kept.Volumes = append(kept.Volumes, vol("data", "vdb"))
	kept.Volumes[1].DeleteOnTermination = true
	plainIn, keptIn := ok(compute.NewCreateInstance(plain)), ok(compute.NewCreateInstance(kept))
	if compute.FingerprintOf(plainIn) == compute.FingerprintOf(keptIn) {
		t.Fatal("DeleteOnTermination does not reach the create fingerprint")
	}
	spec := ok(compute.Plan(newView(), envNow(), plainIn)).Spec.(compute.InstanceSpec)
	if _, err := compute.Plan(newView().with(compute.Instance{Spec: spec}), envNow(), keptIn); compute.CodeOf(
		err,
	) != compute.CodeAlreadyExists {
		t.Fatalf("other DeleteOnTermination under the same id = %v, want AlreadyExists", err)
	}
	if ch, err := compute.Plan(newView().with(compute.Instance{Spec: spec}), envNow(), plainIn); err != nil ||
		ch.Write {
		t.Fatalf("replay = write %v err %v, want the stored instance", ch.Write, err)
	}
}

// The golden is this update's fingerprint on main 6f614b7, before the
// field, so a stored UpdateInstance request id with default values replays.
func TestUpdateFingerprintCoversDeleteOnTermination(t *testing.T) {
	const golden = "0284f95589b4cab02442bbd7688d769d2a856fd5710232569d1d0d1d7980597f"
	root := createParams().Volumes[0]
	fp := func(vols ...compute.VolumeAttachment) compute.Fingerprint {
		return compute.FingerprintOf(
			ok(compute.NewUpdateInstanceSpec("web-1", compute.PowerRunning, vols, compute.MetadataPatch{})),
		)
	}
	data := vol("data", "vdb")
	if got := fmt.Sprintf("%x", fp(root, data)); got != golden {
		t.Fatalf("default fingerprint = %s, want %s", got, golden)
	}
	deleted := data
	deleted.DeleteOnTermination = true
	if fp(root, data) == fp(root, deleted) {
		t.Fatal("DeleteOnTermination does not reach the update fingerprint")
	}
}
