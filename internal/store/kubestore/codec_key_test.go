package kubestore

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// TestStoredRecordsReadAnOversizedRSAKey pins vws-compute#112: the import cap
// applies only to a new key pair, so a record written before it existed still
// decodes and Repair can read it after a restart.
func TestStoredRecordsReadAnOversizedRSAKey(t *testing.T) {
	kp := decodeRecord(t, compute.Ref{Kind: compute.KindKeyPair, ID: "key-0123456789abcdef0"}, recordJSON{
		KeyPair: &keyPairJSON{ID: "key-0123456789abcdef0", Generation: 1, Name: "legacy", PublicKey: rsa8192Line},
	})
	if got := kp.key.Spec.PublicKey.AuthorizedKey(); got != rsa8192Line {
		t.Fatalf("stored key = %.40q, want the 8192-bit key", got)
	}

	inst := decodeRecord(t, compute.Ref{Kind: compute.KindInstance, ID: "i-0123456789abcdef0"}, recordJSON{
		Instance: &instanceJSON{
			ID: "i-0123456789abcdef0", Generation: 1, Image: "img-0123456789abcdef0",
			Type: "vws.small", KeyName: "legacy", KeyPublic: rsa8192Line, Power: uint8(compute.PowerRunning),
		},
	})
	if inst.inst.Spec.Key == nil {
		t.Fatal("instance record lost its key")
	}
	if got := inst.inst.Spec.Key.PublicKey.AuthorizedKey(); got != rsa8192Line {
		t.Fatalf("instance key = %.40q, want the 8192-bit key", got)
	}
}

func decodeRecord(t *testing.T, ref compute.Ref, rec recordJSON) *entry {
	t.Helper()
	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	e, err := decode(ref, &corev1.ConfigMap{Data: map[string]string{recordKey: string(raw)}}, compute.ImageScope{})
	if err != nil {
		t.Fatalf("read stored %s record: %v", ref.Kind, err)
	}
	return e
}

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
