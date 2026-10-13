package kubevirt

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

// TestVolumeChangeToleratesForeignDiskShapes: a disk entry that is not an
// object (someone else edited the VirtualMachine) is kept as it is, and
// computing the change does not panic.
func TestVolumeChangeToleratesForeignDiskShapes(t *testing.T) {
	vm := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
		"domain": map[string]any{"devices": map[string]any{"disks": []any{
			map[string]any{"name": "rootdisk", "disk": map[string]any{"bus": "virtio"}},
			"not-a-disk",
		}}},
		"volumes": []any{map[string]any{"name": "rootdisk", "dataVolume": map[string]any{"name": "r"}}},
	}}}}}
	_, disks, changed := volumeChange(vm, []hypervisor.Volume{{Name: "vdb", Claim: "pvc-b"}})
	if !changed || len(disks) != 3 || disks[1] != "not-a-disk" {
		t.Fatalf("changed %v disks %v, want the foreign entry kept and vdb added", changed, disks)
	}
}
