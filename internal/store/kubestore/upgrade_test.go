package kubestore_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore"
)

// fixture is the ConfigMap set the 89f2ca3 build wrote (testdata/upgrade/89f2ca3/README.md).
const fixture = "../../../testdata/upgrade/89f2ca3/configmaps.json"

// rsa8192Line was imported by the 89f2ca3 build. The current import path caps
// ssh-rsa at 4096 bits (ParseImportPublicKey), so this key can only arrive in a
// stored record.
const rsa8192Line = "ssh-rsa " +
	"AAAAB3NzaC1yc2EAAAADAQABAAAEAQClMGuE0yIWc5dvrilBMw5vPA7Q9RcHQqjX7pkBxe0Pv57mLcNzJ5uZeF9pR3CZ9sinS2wo" +
	"3Z5MVWZn6sC1RdrP3VP1dDaJD7Z4paVlrbD5yzLNQyMya9jVaBy1itiyoLwGG1zfkoKnLnJOqpU8odp+thAEEQuOgOTqxsazcGrq" +
	"4clrlFrCoReKlbxzCjM4J3yENkmKhTOhJD1iEA90Mw3w2V3vpGExLGJItyB389eHRYBxClBxLEBKZI07GJgErndubHTxmlD2YhPQ" +
	"pN+TaaE9fvQD+EMhgKtCI9F5JYhrXMSu86PQkuCrF0S8dMEC1TWHhA17IB+eaCbBE8y2/mn95kJAjK18S7BOGNrT3fnwqiuu6Utf" +
	"BJNfgsycvgPBQ3WZ0DD+riK/A2Al4Gd/PKQepRIw9fA7tUOMO+J+9APRLWH4MsvdjjgQjqL5bVVebnORJzgDYwmqUo+XzVNHU4ia" +
	"V7x6c8oaX2D22whQ/v9MtLbpFmfymcWMF+UhJf9nrfxUbm28v1XoACTK6BHISMjPhqUQZDUSfDvbNMqnV66YcwQWvfPkA81XNlN1" +
	"FjicYU6IreJsxI6iJycH+Wt61LuapISfxKWSzzsLnIHFtLmNs3Au/Hhk0JOWlkmEWImKqNsI1OU5g03knDxSSSRsWt70TqxcoibA" +
	"A6Tv9SwMfrCO3KXd9RcSW3TGqsfHwm8rRbGR2kWUC/shE+TEXIAHp/B9ZQ3O0WXitQVorjDiBmoABPhGQOsqFgDYZ9yAgfb2boaq" +
	"xXo3S/CPzyCXRxe9UX8b4svSWomhXowvaqIYxVOsFVe8M1/raHkF3+sUJPwAh0RoER2gk0BL1nHDwqTplX0gdsiLfoaOK//h3Xv4" +
	"AD7L8AcS9IeFlNkB7dAXgg0BNCEdIykh5m+Eu30Imig1TBnj6cMufRPci89eI1MFlVFX0j1rtabnMigzR+SEAriT7uGtSvMbVdGf" +
	"2P2Pb/4HUxHI7ekKA9Otx3kYvOe01OuvESFez937VptwTvpBKCbfmidNEzG5e6DCcGTb0Gt+Cr3OBjCBIIB+ItDuL6n5INnMUspY" +
	"738gXzqX3GBplobG2cvZCZWqbQBscTCiA7DuhAbx7GMYgnk39n9JrpqwF0IU6f89Qrbxl8qzuvcn4TVbEWgT4n1QSlfsgxBIPlaK" +
	"HUe5IKdK/lBLnMDmw4UAxbA7+2U91roy0t3Sx2LZ66TecAVnY+ge8zlqNFv+wKHJZplXCMjOyUobh4HZNP9TYvLA0Ma/Cfu3xI+o" +
	"oKHgajPIoxDRJyaDJLfnKONxO1zOusBVSGDmXB0yV2CPlg6KzAy45Z+ojFw/Jo/7xywO3UQphQlfUi/q+Uf5m97xEbnMcJFJ"

// TestPreviousReleaseRecordsDecode pins vws-deploy#396 and vws-compute#112: the
// key pair and instance records the previous release wrote still decode through
// the store, and Repair over them changes nothing.
func TestPreviousReleaseRecordsDecode(t *testing.T) {
	raw, err := os.ReadFile(filepath.FromSlash(fixture))
	if err != nil {
		t.Fatalf("read the 89f2ca3 fixture: %v", err)
	}
	var list corev1.ConfigMapList
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("decode the 89f2ca3 fixture: %v", err)
	}
	objs := make([]client.Object, len(list.Items))
	for i := range list.Items {
		objs[i] = &list.Items[i]
	}
	c := fake.NewClientBuilder().WithObjects(objs...).Build()
	ctx := context.Background()
	s := kubestore.New(c, "vws")

	keyID := ok(compute.ParseKeyPairID("key-0123456789abcdef0"))
	kp, err := s.KeyPair(ctx, keyID)
	if err != nil {
		t.Fatalf("read the 89f2ca3 key pair: %v", err)
	}
	if got := kp.Spec.PublicKey.AuthorizedKey(); got != rsa8192Line {
		t.Fatalf("stored public key = %.40q, want the 8192-bit key the old build imported", got)
	}
	if kp.Spec.Name.String() != "legacy-rsa-8192" || kp.Spec.Generation != 1 {
		t.Fatalf("key pair = %s generation %d, want legacy-rsa-8192 generation 1", kp.Spec.Name, kp.Spec.Generation)
	}
	if _, err := s.KeyPairByName(ctx, kp.Spec.Name); err != nil {
		t.Fatalf("read the key pair by name: %v", err)
	}
	keys, err := s.KeyPairs(ctx, store.KeyPairQuery{})
	if err != nil || len(keys.Items) != 1 || keys.Items[0].Spec.ID != keyID {
		t.Fatalf("list key pairs = %d items, %v, want the one 89f2ca3 record", len(keys.Items), err)
	}

	inst, err := s.Instance(ctx, ok(compute.ParseInstanceID("i-0123456789abcdef0")))
	if err != nil {
		t.Fatalf("read the 89f2ca3 instance: %v", err)
	}
	if inst.Spec.Key == nil {
		t.Fatal("the 89f2ca3 instance lost its key")
	}
	if got := inst.Spec.Key.PublicKey.AuthorizedKey(); got != rsa8192Line {
		t.Fatalf("instance key = %.40q, want the 8192-bit key", got)
	}
	if inst.Spec.Image.String() != "img-0123456789abcdef0" || inst.Spec.Power != compute.PowerRunning {
		t.Fatalf("instance = image %s power %d, want img-0123456789abcdef0 running", inst.Spec.Image, inst.Spec.Power)
	}

	// A restart repairs the old record set and still reads both records.
	r, err := kubestore.New(c, "vws").Repair(ctx)
	if err != nil {
		t.Fatalf("repair the 89f2ca3 record set: %v", err)
	}
	if r.OrphanIndexes != 0 || r.RestoredIndexes != 0 || r.StaleClaims != 0 {
		t.Fatalf("repair = %+v, want no change to the 89f2ca3 record set", r)
	}
	if _, err := s.KeyPair(ctx, keyID); err != nil {
		t.Fatalf("read the key pair after repair: %v", err)
	}
}
