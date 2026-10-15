// Package kubevirt is a hypervisor.Agent that runs machines as KubeVirt
// VirtualMachine objects.
//
// The Kubernetes API is the only store. A machine is one VirtualMachine named
// by its instance ID. Power is spec.runStrategy: Always for on, Halted for
// off. Placement, restart after a node loss, and the guest lifecycle belong
// to kube-scheduler, kubelet, and KubeVirt, so this package keeps no state
// and calling it again with the same spec changes nothing.
//
// Objects are unstructured on purpose. The package needs five fields of a
// type it does not own, and the KubeVirt Go API would pull in its whole tree.
package kubevirt

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

var _ hypervisor.Agent = (*Agent)(nil)

var (
	vmGVK  = schema.GroupVersionKind{Group: "kubevirt.io", Version: "v1", Kind: "VirtualMachine"}
	vmiGVK = schema.GroupVersionKind{Group: "kubevirt.io", Version: "v1", Kind: "VirtualMachineInstance"}
)

const (
	// LabelManaged marks a VirtualMachine this package owns.
	LabelManaged = "vws.io/managed"
	// AnnotationRebootEpoch holds the last reboot epoch applied to a machine.
	AnnotationRebootEpoch = "vws.io/reboot-epoch"
	// AnnotationLogicalSwitch is the Kube-OVN pod annotation that picks the
	// subnet of the pod's default interface. It is set on the VM template,
	// so the virt-launcher pod carries it.
	AnnotationLogicalSwitch = "ovn.kubernetes.io/logical_switch"
	// LabelInstance names the instance an object belongs to.
	LabelInstance = "vws.io/instance"

	runAlways = "Always"
	runHalted = "Halted"
)

// Config says where objects live.
type Config struct {
	// Namespace holds VirtualMachines.
	Namespace string
	// ImageNamespace holds the CDI DataVolumes and DataSources that root
	// disks clone from. Empty means Namespace.
	ImageNamespace string
	// ImportSize is the disk size requested for an image fetched from a URL,
	// as a Kubernetes quantity such as "20Gi". CDI cannot size an import by
	// itself. Clones take their size from the source. Empty rejects URL
	// images.
	ImportSize string
}

// Agent converges machines through the Kubernetes API. It is safe for
// concurrent use.
type Agent struct {
	c   client.Client
	cfg Config
}

func New(c client.Client, cfg Config) *Agent {
	if cfg.ImageNamespace == "" {
		cfg.ImageNamespace = cfg.Namespace
	}
	return &Agent{c: c, cfg: cfg}
}

func (a *Agent) ConvergeMachine(ctx context.Context, spec hypervisor.MachineSpec) error {
	if errs := validation.IsDNS1123Label(spec.Name); len(errs) > 0 {
		return hypervisor.Permanent(fmt.Errorf("machine %q: not a valid object name: %s", spec.Name, errs[0]))
	}
	switch spec.Power {
	case hypervisor.PowerAbsent:
		return a.destroy(ctx, spec.Name)
	case hypervisor.PowerOff, hypervisor.PowerOn:
	default:
		return hypervisor.Permanent(fmt.Errorf("machine %s: invalid power %d", spec.Name, spec.Power))
	}

	if err := a.converge(ctx, spec); err != nil {
		return err
	}
	if spec.Power == hypervisor.PowerOn {
		return a.running(ctx, spec.Name)
	}
	return nil
}

// running reports whether KubeVirt has the guest up: the VirtualMachineInstance
// exists, its phase is Running, and its Ready condition is True. Anything else
// is a hypervisor.NotReady error (transient), so the reconciler reports the instance as reconciling
// and checks again on the next sweep instead of reporting it ready. It only
// reads, so a converged machine is never written again.
func (a *Agent) running(ctx context.Context, name string) error {
	vmi := new(unstructured.Unstructured)
	vmi.SetGroupVersionKind(vmiGVK)
	err := a.c.Get(ctx, client.ObjectKey{Namespace: a.cfg.Namespace, Name: name}, vmi)
	if apierrors.IsNotFound(err) {
		return hypervisor.NotReady(fmt.Errorf("machine %s: no virtual machine instance yet", name))
	}
	if err != nil {
		return fmt.Errorf("machine %s: get virtual machine instance: %w", name, err)
	}
	phase, _, err := unstructured.NestedString(vmi.Object, "status", "phase")
	if err != nil {
		return fmt.Errorf("machine %s: read instance phase: %w", name, err)
	}
	if phase != "Running" {
		return hypervisor.NotReady(fmt.Errorf("machine %s: instance phase is %q, not Running", name, phase))
	}
	if ready := readyCondition(vmi); ready != "True" {
		return hypervisor.NotReady(fmt.Errorf("machine %s: instance is Running but its Ready condition is %q", name, ready))
	}
	return nil
}

// readyCondition is the status of the VMI's Ready condition, or "" when it has none.
func readyCondition(vmi *unstructured.Unstructured) string {
	conds, _, err := unstructured.NestedSlice(vmi.Object, "status", "conditions")
	if err != nil {
		return ""
	}
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if !ok || m["type"] != "Ready" {
			continue
		}
		s, ok := m["status"].(string)
		if !ok {
			return ""
		}
		return s
	}
	return ""
}

// converge creates or patches the VirtualMachine so it matches spec.
func (a *Agent) converge(ctx context.Context, spec hypervisor.MachineSpec) error {
	// A create racing another writer loses with AlreadyExists. One more pass
	// reads the winner and converges it.
	for range 2 {
		vm := new(unstructured.Unstructured)
		vm.SetGroupVersionKind(vmGVK)
		err := a.c.Get(ctx, client.ObjectKey{Namespace: a.cfg.Namespace, Name: spec.Name}, vm)
		if apierrors.IsNotFound(err) {
			err := a.create(ctx, spec)
			if apierrors.IsAlreadyExists(err) {
				continue
			}
			return err
		}
		if err != nil {
			return fmt.Errorf("machine %s: get VirtualMachine: %w", spec.Name, err)
		}
		return a.update(ctx, vm, spec)
	}
	return fmt.Errorf("machine %s: lost a create race twice", spec.Name)
}

func (a *Agent) destroy(ctx context.Context, name string) error {
	vm := new(unstructured.Unstructured)
	vm.SetGroupVersionKind(vmGVK)
	vm.SetNamespace(a.cfg.Namespace)
	vm.SetName(name)
	err := a.c.Delete(ctx, vm, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if err := client.IgnoreNotFound(err); err != nil {
		return fmt.Errorf("machine %s: delete VirtualMachine: %w", vm.GetName(), err)
	}
	if err := a.deleteCloneRoot(ctx, name); err != nil {
		return err
	}
	// The user-data and key Secrets go after the VM, so a retry after a
	// failure here deletes them then.
	if err := a.deleteCloudInitSecret(ctx, name); err != nil {
		return err
	}
	return a.deleteSecret(ctx, name, sshKeysSecret(name))
}

func (a *Agent) create(ctx context.Context, spec hypervisor.MachineSpec) error {
	if (spec.Image == "") == (spec.BootClaim == "") || spec.VCPUs <= 0 || spec.MemoryMiB <= 0 {
		return hypervisor.Permanent(fmt.Errorf("machine %s: need exactly one of an image or a boot claim, and vcpus and memory, to create", spec.Name))
	}
	for _, n := range []string{spec.Image, spec.BootClaim} {
		if errs := validation.IsDNS1123Subdomain(n); n != "" && len(errs) > 0 {
			return hypervisor.Permanent(fmt.Errorf("machine %s: root disk source %q is not a valid object name", spec.Name, n))
		}
	}
	if errs := validation.IsDNS1123Subdomain(spec.Subnet); spec.Subnet != "" && len(errs) > 0 {
		return hypervisor.Permanent(fmt.Errorf("machine %s: subnet %q is not a valid Kube-OVN Subnet name", spec.Name, spec.Subnet))
	}
	run := runHalted
	if spec.Power == hypervisor.PowerOn {
		run = runAlways
	}
	if spec.Image != "" {
		ds := a.object(dsGVK, spec.Image)
		err := a.c.Get(ctx, client.ObjectKeyFromObject(ds), ds)
		if apierrors.IsNotFound(err) {
			return hypervisor.ImageNotFound(fmt.Errorf("machine %s: image %s not found", spec.Name, spec.Image))
		}
		if err != nil {
			return fmt.Errorf("machine %s: get DataSource %s: %w", spec.Name, spec.Image, err)
		}
	}
	rootDisk := rootDiskName(spec.Name)
	disks := []any{map[string]any{"name": "rootdisk", "disk": map[string]any{"bus": "virtio"}}}
	volumes := []any{map[string]any{"name": "rootdisk", "dataVolume": map[string]any{"name": rootDisk}}}
	var templates []any
	if spec.Image != "" {
		templates = []any{map[string]any{
			"metadata": map[string]any{
				"name": rootDisk,
				"labels": map[string]any{
					LabelManaged:  "true",
					LabelInstance: spec.Name,
				},
			},
			"spec": map[string]any{
				"sourceRef": map[string]any{"kind": "DataSource", "name": spec.Image, "namespace": a.cfg.ImageNamespace},
				"storage":   map[string]any{},
			},
		}}
	} else {
		// Boot from the storage service's claim in the VM namespace. It is
		// not owned by the VirtualMachine, so deleting the machine leaves the
		// volume to storage. Not hotpluggable: the root disk never changes.
		if err := a.checkAttachable(ctx, spec.Name, &unstructured.Unstructured{Object: map[string]any{}},
			[]hypervisor.Volume{{Name: "rootdisk", Claim: spec.BootClaim}}, false); err != nil {
			return err
		}
		volumes = []any{map[string]any{"name": "rootdisk", "persistentVolumeClaim": map[string]any{"claimName": spec.BootClaim}}}
	}
	userData, err := renderUserData(spec.AuthorizedKeys, spec.UserData)
	if err != nil {
		return fmt.Errorf("machine %s: cloud-init: %w", spec.Name, err)
	}
	if userData != "" {
		// NoCloud reads the user data from a Secret, so up to 32 KiB of it
		// (more once base64 in a multipart) stays out of the VM object.
		if err := a.ensureCloudInitSecret(ctx, spec.Name, userData); err != nil {
			return err
		}
		disks = append(disks, map[string]any{"name": "cloudinitdisk", "disk": map[string]any{"bus": "virtio"}})
		volumes = append(volumes, map[string]any{
			"name":             "cloudinitdisk",
			"cloudInitNoCloud": map[string]any{"secretRef": map[string]any{"name": cloudInitSecret(spec.Name)}},
		})
	}
	var access []any
	if len(spec.AuthorizedKeys) > 0 {
		// The keys also go in the NoCloud meta-data as public-keys (EC2
		// delivers a key pair through instance meta-data). KubeVirt requires
		// a cloudInitNoCloud volume for this; the keys-only user data above
		// guarantees one.
		if err := a.ensureSecret(ctx, spec.Name, sshKeysSecret(spec.Name), keysData(spec.AuthorizedKeys)); err != nil {
			return err
		}
		access = []any{map[string]any{"sshPublicKey": map[string]any{
			"source":            map[string]any{"secret": map[string]any{"secretName": sshKeysSecret(spec.Name)}},
			"propagationMethod": map[string]any{"noCloud": map[string]any{}},
		}}}
	}
	podMeta := map[string]any{"labels": map[string]any{LabelInstance: spec.Name}}
	if spec.Subnet != "" {
		// KubeVirt copies template annotations to the virt-launcher pod,
		// and Kube-OVN puts the pod's default interface on this logical
		// switch instead of the cluster default subnet.
		podMeta["annotations"] = map[string]any{AnnotationLogicalSwitch: spec.Subnet}
	}
	vm := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"runStrategy": run,
			"template": map[string]any{
				"metadata": podMeta,
				"spec": map[string]any{
					"domain": map[string]any{
						"cpu":     map[string]any{"cores": int64(spec.VCPUs)},
						"memory":  map[string]any{"guest": memory(spec.MemoryMiB)},
						"devices": map[string]any{"disks": disks},
					},
					"volumes": volumes,
				},
			},
		},
	}}
	if access != nil {
		vm.Object["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["accessCredentials"] = access //nolint:errcheck // built above as maps
	}
	if templates != nil {
		vm.Object["spec"].(map[string]any)["dataVolumeTemplates"] = templates //nolint:errcheck // built above as a map
	}
	vm.SetGroupVersionKind(vmGVK)
	vm.SetNamespace(a.cfg.Namespace)
	vm.SetName(spec.Name)
	vm.SetLabels(map[string]string{LabelManaged: "true"})
	vm.SetAnnotations(map[string]string{AnnotationRebootEpoch: strconv.FormatUint(uint64(spec.RebootEpoch), 10)})
	if err := a.c.Create(ctx, vm); err != nil {
		return fmt.Errorf("machine %s: create VirtualMachine: %w", spec.Name, err)
	}
	return nil
}

func rootDiskName(instance string) string { return instance + "-root" }

func (a *Agent) deleteCloneRoot(ctx context.Context, instance string) error {
	name := rootDiskName(instance)
	dvUID, err := a.deleteIfOwned(ctx, dvGVK, name, instance, "")
	if err != nil {
		return err
	}
	_, err = a.deleteIfOwned(ctx, pvcGVK, name, instance, dvUID)
	return err
}

func (a *Agent) deleteIfOwned(
	ctx context.Context, gvk schema.GroupVersionKind, name, instance string, dvUID types.UID,
) (types.UID, error) {
	var seen types.UID
	for range 2 {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		err := a.c.Get(ctx, client.ObjectKey{Namespace: a.cfg.Namespace, Name: name}, u)
		if apierrors.IsNotFound(err) {
			return seen, nil
		}
		if err != nil {
			return "", fmt.Errorf("get %s %s: %w", gvk.Kind, name, err)
		}
		if !cloneOwned(u, instance, dvUID) {
			return seen, nil
		}
		if seen == "" {
			seen = u.GetUID()
		}
		uid := u.GetUID()
		err = a.c.Delete(ctx, u, client.Preconditions{UID: &uid})
		if apierrors.IsNotFound(err) {
			return seen, nil
		}
		if apierrors.IsConflict(err) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("delete %s %s: %w", gvk.Kind, name, err)
		}
		return seen, nil
	}
	return seen, fmt.Errorf("delete %s %s: ownership changed", gvk.Kind, name)
}

func cloneOwned(obj metav1.Object, instance string, dvUID types.UID) bool {
	labels := obj.GetLabels()
	if labels[LabelManaged] == "true" && labels[LabelInstance] == instance {
		return true
	}
	if dvUID == "" {
		return false
	}
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Kind == dvGVK.Kind && ref.Name == rootDiskName(instance) && ref.UID == dvUID {
			return true
		}
	}
	return false
}

func memory(mib int) string { return strconv.Itoa(mib) + "Mi" }

// update brings an existing VirtualMachine to spec with at most one patch.
// Image, root disk, and authorized keys are fixed at creation, as on EC2.
// Data volumes are not: they attach and detach while the machine runs.
func (a *Agent) update(ctx context.Context, vm *unstructured.Unstructured, spec hypervisor.MachineSpec) error {
	if vm.GetLabels()[LabelManaged] != "true" {
		return hypervisor.Permanent(fmt.Errorf("machine %s: a VirtualMachine with that name exists and is not managed here", spec.Name))
	}
	if vm.GetDeletionTimestamp() != nil {
		return fmt.Errorf("machine %s: previous VirtualMachine is still being deleted", spec.Name)
	}
	if have := rootClaim(vm); spec.BootClaim != have && spec.Image == "" {
		return hypervisor.Permanent(fmt.Errorf("machine %s: boot claim is %q and cannot change to %q", spec.Name, have, spec.BootClaim))
	}
	patch := map[string]any{}
	set := func(path []string, v any) {
		m := patch
		for _, k := range path[:len(path)-1] {
			next, ok := m[k].(map[string]any)
			if !ok {
				next = map[string]any{}
				m[k] = next
			}
			m = next
		}
		m[path[len(path)-1]] = v
	}

	wantRun := runHalted
	if spec.Power == hypervisor.PowerOn {
		wantRun = runAlways
	}
	haveRun, _, _ := unstructured.NestedString(vm.Object, "spec", "runStrategy") //nolint:errcheck // a value of another type is overwritten below like a missing one
	applied := appliedEpoch(vm)
	reboot := false
	switch {
	case haveRun != wantRun:
		set([]string{"spec", "runStrategy"}, wantRun)
		if wantRun == runAlways {
			// Booting applies the epoch without an extra reboot.
			set([]string{"metadata", "annotations", AnnotationRebootEpoch}, epoch(spec.RebootEpoch))
		}
	case wantRun == runAlways && applied < spec.RebootEpoch:
		reboot = true
		set([]string{"metadata", "annotations", AnnotationRebootEpoch}, epoch(spec.RebootEpoch))
	}

	if spec.VCPUs > 0 {
		have, _, _ := unstructured.NestedInt64(vm.Object, "spec", "template", "spec", "domain", "cpu", "cores") //nolint:errcheck // a value of another type is overwritten below like a missing one
		if have != int64(spec.VCPUs) {
			set([]string{"spec", "template", "spec", "domain", "cpu", "cores"}, int64(spec.VCPUs))
		}
	}
	if spec.MemoryMiB > 0 {
		have, _, _ := unstructured.NestedString(vm.Object, "spec", "template", "spec", "domain", "memory", "guest") //nolint:errcheck // a value of another type is overwritten below like a missing one
		if q, err := resource.ParseQuantity(have); err != nil || q.Cmp(resource.MustParse(memory(spec.MemoryMiB))) != 0 {
			set([]string{"spec", "template", "spec", "domain", "memory", "guest"}, memory(spec.MemoryMiB))
		}
	}

	if err := validateVolumes(spec.Name, spec.Volumes); err != nil {
		return err
	}
	if volumes, disks, changed := volumeChange(vm, spec.Volumes); changed {
		if err := a.checkAttachable(ctx, spec.Name, vm, spec.Volumes, true); err != nil {
			return err
		}
		set([]string{"spec", "template", "spec", "volumes"}, volumes)
		set([]string{"spec", "template", "spec", "domain", "devices", "disks"}, disks)
		// A merge patch replaces both lists whole, so it must not land on a
		// VirtualMachine that changed after we read it.
		set([]string{"metadata", "resourceVersion"}, vm.GetResourceVersion())
	}

	if reboot {
		// The VirtualMachine stays Always, so KubeVirt starts a fresh
		// instance. A crash between this delete and the patch below repeats
		// the reboot once, which is the cheaper failure than losing it.
		vmi := new(unstructured.Unstructured)
		vmi.SetGroupVersionKind(vmiGVK)
		vmi.SetNamespace(a.cfg.Namespace)
		vmi.SetName(spec.Name)
		if err := client.IgnoreNotFound(a.c.Delete(ctx, vmi)); err != nil {
			return fmt.Errorf("machine %s: delete VirtualMachineInstance: %w", spec.Name, err)
		}
	}
	if len(patch) == 0 {
		return nil
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		return hypervisor.Permanent(err)
	}
	if err := a.c.Patch(ctx, vm, client.RawPatch(types.MergePatchType, raw)); err != nil {
		return fmt.Errorf("machine %s: patch VirtualMachine: %w", spec.Name, err)
	}
	return nil
}

func epoch(n uint32) string { return strconv.FormatUint(uint64(n), 10) }

func appliedEpoch(vm *unstructured.Unstructured) uint32 {
	n, err := strconv.ParseUint(vm.GetAnnotations()[AnnotationRebootEpoch], 10, 32)
	if err != nil {
		return 0
	}
	return uint32(n)
}

// rootClaim returns the claim the root disk is a plain PVC volume of, or ""
// when the root disk is a DataVolume.
func rootClaim(vm *unstructured.Unstructured) string {
	for _, v := range templateSlice(vm, "spec", "template", "spec", "volumes") {
		m, _ := v.(map[string]any) //nolint:errcheck // an entry of another shape is not the root disk
		if m["name"] != "rootdisk" {
			continue
		}
		pvc, _ := m["persistentVolumeClaim"].(map[string]any) //nolint:errcheck // a DataVolume root has no claim here
		claim, _ := pvc["claimName"].(string)                 //nolint:errcheck // absent reads as ""
		return claim
	}
	return ""
}
