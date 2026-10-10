package grpcapi

import (
	"context"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

func (s *server) GetInstance(ctx context.Context, r *computev1.GetInstanceRequest) (*computev1.Instance, error) {
	inst, name, err := s.liveInstance(ctx, r.GetName())
	if err != nil {
		return nil, err
	}
	return instancePB(inst, name), nil
}

func (s *server) ListInstances(ctx context.Context, r *computev1.ListInstancesRequest) (*computev1.ListInstancesResponse, error) {
	project, zone, err := parseZonalParent(r.GetParent())
	if err != nil {
		return nil, err
	}
	var matched []compute.Instance
	after := ""
	for {
		page, err := s.read.Instances(ctx, store.InstanceQuery{PageSize: store.MaxPageSize, After: after})
		if err != nil {
			return nil, toStatus(err)
		}
		for _, inst := range page.Items {
			if inst.Lifecycle() == compute.InstanceTerminated {
				continue
			}
			matched = append(matched, inst)
		}
		if page.Next == "" {
			break
		}
		after = page.Next
	}
	page, next, err := cutPage(matched, r.GetPageSize(), r.GetPageToken(), func(i compute.Instance) string {
		return instanceResourceName(project, zone, i.Spec.ID.String())
	})
	if err != nil {
		return nil, err
	}
	out := make([]*computev1.Instance, 0, len(page))
	for _, inst := range page {
		out = append(out, instancePB(inst, instanceResourceName(project, zone, inst.Spec.ID.String())))
	}
	return &computev1.ListInstancesResponse{Instances: out, NextPageToken: next}, nil
}

func (s *server) CreateInstance(ctx context.Context, r *computev1.CreateInstanceRequest) (*longrunningpb.Operation, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	project, zone, err := parseZonalParent(r.GetParent())
	if err != nil {
		return nil, err
	}
	if err := validateResourceID(r.GetInstanceId()); err != nil {
		return nil, err
	}
	name := instanceResourceName(project, zone, r.GetInstanceId())
	if err := validateCreateInstance(zone, name, r.GetInstance()); err != nil {
		return nil, err
	}
	vol := r.GetInstance().GetSpec().GetVolumes()[0].GetVolume()
	return nil, dependencyMissing("volume " + vol + " is not registered in this process")
}

func (s *server) UpdateInstance(ctx context.Context, r *computev1.UpdateInstanceRequest) (*longrunningpb.Operation, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	body := r.GetInstance()
	if body == nil {
		return nil, invalidSpec("instance is required")
	}
	project, _, id, err := parseInstanceName(body.GetName())
	if err != nil {
		return nil, err
	}
	paths := effectiveMask(r.GetUpdateMask().GetPaths())
	if err := checkMask(paths); err != nil {
		return nil, err
	}
	patch, err := metadataPatch(paths, body.GetLabels(), nil, maskIncludes(paths, "spec"))
	if err != nil {
		return nil, toStatus(err)
	}
	existing, name, err := s.liveInstance(ctx, body.GetName())
	if err != nil {
		if r.GetAllowMissing() && statusNotFound(err) {
			return nil, dependencyMissing("instance " + body.GetName() + " is absent and this process cannot create one without a volume")
		}
		return nil, err
	}
	var in compute.Intent
	if maskIncludes(paths, "spec") {
		spec := body.GetSpec()
		if spec == nil {
			return nil, invalidSpec("instance.spec is required")
		}
		if err := instanceSpecDelta(spec, existing); err != nil {
			return nil, err
		}
		var power compute.Power
		switch spec.GetPower() {
		case computev1.InstancePower_INSTANCE_POWER_RUNNING:
			power = compute.PowerRunning
		case computev1.InstancePower_INSTANCE_POWER_STOPPED:
			power = compute.PowerStopped
		default:
			return nil, invalidSpec("instance.spec.power must be RUNNING or STOPPED")
		}
		in, err = compute.NewSetPower(id, power, patch)
	} else {
		in, err = compute.NewUpdateInstanceMetadata(id, patch)
	}
	if r.GetValidateOnly() {
		if err != nil {
			return nil, toStatus(err)
		}
		if err := checkEtag(body.GetEtag(), etagOf(existing.Spec.Generation, existing.Meta.Revision)); err != nil {
			return nil, err
		}
		return doneOperation(project, name, commonv1.OperationVerb_OPERATION_VERB_UPDATE, r.GetRequestId(), nil)
	}
	rc, err := s.submit(ctx, r.GetRequestId(), in, err, func() error {
		cur, _, err := s.liveInstance(ctx, body.GetName())
		if err != nil {
			return err
		}
		return checkEtag(body.GetEtag(), etagOf(cur.Spec.Generation, cur.Meta.Revision))
	})
	if err != nil {
		return nil, err
	}
	return s.operationPB(ctx, project, name, rc.Operation)
}

func (s *server) DeleteInstance(ctx context.Context, r *computev1.DeleteInstanceRequest) (*longrunningpb.Operation, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	project, _, id, err := parseInstanceName(r.GetName())
	if err != nil {
		return nil, err
	}
	if _, _, err = s.liveInstance(ctx, r.GetName()); err != nil {
		if r.GetAllowMissing() && statusNotFound(err) {
			return doneOperation(project, r.GetName(), commonv1.OperationVerb_OPERATION_VERB_DELETE, r.GetRequestId(), nil)
		}
		return nil, err
	}
	in, err := compute.NewTerminateInstance(id)
	rc, err := s.submit(ctx, r.GetRequestId(), in, err, func() error {
		cur, _, err := s.liveInstance(ctx, r.GetName())
		if err != nil {
			return err
		}
		return checkEtag(r.GetEtag(), etagOf(cur.Spec.Generation, cur.Meta.Revision))
	})
	if err != nil {
		return nil, err
	}
	return s.operationPB(ctx, project, r.GetName(), rc.Operation)
}

func (s *server) ReportInstanceStatus(ctx context.Context, r *computev1.ReportInstanceStatusRequest) (*computev1.ReportInstanceStatusResponse, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	if r.GetStatus() == nil || r.GetStatus().GetObserved() == nil {
		return nil, invalidSpec("status.observed is required")
	}
	inst, name, err := s.liveInstance(ctx, r.GetName())
	if err != nil {
		return nil, err
	}
	if r.GetStatus().GetObserved().GetGeneration() > inst.Spec.Generation {
		return nil, aborted("GENERATION_CONFLICT", "observed generation is ahead of the stored generation")
	}
	// The in-process reconciler is the node that commits status. A report that
	// is not ahead of the stored generation changes nothing.
	return &computev1.ReportInstanceStatusResponse{Instance: instancePB(inst, name)}, nil
}

func (s *server) liveInstance(ctx context.Context, name string) (compute.Instance, string, error) {
	_, _, id, err := parseInstanceName(name)
	if err != nil {
		return compute.Instance{}, "", err
	}
	iid, err := compute.ParseInstanceID(id)
	if err != nil {
		return compute.Instance{}, name, notFound("instance " + name + " not found")
	}
	inst, err := s.read.Instance(ctx, iid)
	if err != nil {
		return compute.Instance{}, name, toStatus(err)
	}
	if inst.Lifecycle() == compute.InstanceTerminated {
		return compute.Instance{}, name, notFound("instance " + name + " not found")
	}
	return inst, name, nil
}

func validateCreateInstance(zone, name string, inst *computev1.Instance) error {
	if inst == nil || inst.GetSpec() == nil {
		return invalidSpec("instance.spec is required")
	}
	if inst.GetName() != "" && inst.GetName() != name {
		return invalidID("instance.name does not match parent and instance_id")
	}
	if inst.GetUid() != "" || inst.GetGeneration() != 0 || inst.GetEtag() != "" || inst.GetStatus() != nil ||
		inst.GetCreateTime() != nil || inst.GetUpdateTime() != nil || inst.GetDeleteTime() != nil {
		return invalidSpec("output-only fields must be unset")
	}
	if _, err := compute.ParseLabels(inst.GetLabels()); err != nil {
		return toStatus(err)
	}
	spec := inst.GetSpec()
	if !machineTypePattern.MatchString(spec.GetMachineType()) {
		return invalidSpec("instance.spec.machine_type is invalid")
	}
	if spec.GetPower() != computev1.InstancePower_INSTANCE_POWER_RUNNING && spec.GetPower() != computev1.InstancePower_INSTANCE_POWER_STOPPED {
		return invalidSpec("instance.spec.power must be RUNNING or STOPPED")
	}
	if spec.GetKeyPair() != "" {
		if _, _, _, err := parseKeyPairName(spec.GetKeyPair()); err != nil {
			return err
		}
	}
	if len(spec.GetUserData()) > 32768 {
		return invalidSpec("instance.spec.user_data exceeds 32768 bytes")
	}
	if err := validateNICs(spec.GetNetworkInterfaces()); err != nil {
		return err
	}
	return validateVolumes(zone, spec.GetVolumes())
}

func validateNICs(nics []*computev1.NetworkInterface) error {
	seen := map[string]bool{}
	for _, nic := range nics {
		if nic.GetDeviceName() == "" || seen[nic.GetDeviceName()] {
			return invalidSpec("network interface device names must be unique and non-empty")
		}
		seen[nic.GetDeviceName()] = true
		if err := parseSubnetName(nic.GetSubnet()); err != nil {
			return err
		}
		for _, g := range nic.GetSecurityGroups() {
			if err := parseSecurityGroupName(g); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateVolumes(zone string, vols []*computev1.VolumeAttachment) error {
	seen := map[string]bool{}
	boot := 0
	for _, v := range vols {
		if v.GetDeviceName() == "" || seen[v.GetDeviceName()] {
			return invalidSpec("volume device names must be unique and non-empty")
		}
		seen[v.GetDeviceName()] = true
		_, volZone, _, err := parseVolumeName(v.GetVolume())
		if err != nil {
			return err
		}
		if volZone != zone {
			return invalidSpec("volume " + v.GetVolume() + " is not in the instance zone")
		}
		if v.GetBoot() {
			boot++
		}
	}
	if boot != 1 {
		return invalidSpec("volumes must include exactly one boot entry")
	}
	return nil
}

func instanceSpecDelta(spec *computev1.InstanceSpec, cur compute.Instance) error {
	if spec.GetMachineType() != "" && spec.GetMachineType() != cur.Spec.Type.Name() {
		return invalidSpec("machine_type is immutable")
	}
	key := ""
	if cur.Spec.Key != nil {
		key = cur.Spec.Key.Name.String()
	}
	if spec.GetKeyPair() != key && spec.GetKeyPair() != "" {
		return invalidSpec("key_pair is immutable")
	}
	if spec.GetUserData() != "" || len(spec.GetNetworkInterfaces()) > 0 || len(spec.GetVolumes()) > 0 {
		return invalidSpec("network interfaces, volumes, and user_data are not mutable")
	}
	return nil
}
