package grpcapi_test

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// TestDeleteOnTerminationFollowsTheAttach pins vws-compute#173 on the edge:
// an attach through UpdateInstance stores the delete_on_termination it
// sends, Get and List return it, a later update may flip it without moving
// attach_time, and a replay of that update's request id is the same
// operation. Unset is the EC2 default, also on an update.
func TestDeleteOnTerminationFollowsTheAttach(t *testing.T) {
	ctx := context.Background()
	st := &fakeStorage{vols: map[string]grpcapi.VolumeInfo{
		volBase + "root-1": {ClaimName: "pvc-root-1"},
		volBase + "data":   {ClaimName: "pvc-data"},
	}}
	h := serveRepo(t, memstore.New(), grpcapi.WithVolumes(st))
	name := zoneParent + "/instances/web-1"
	create := createReq("web-1", "91919191-9191-4191-8191-919191919191", "root-1")
	if _, err := h.inst.CreateInstance(ctx, create); err != nil {
		t.Fatal(err)
	}
	flags := func(i *computev1.Instance) map[string]*bool {
		m := map[string]*bool{}
		for _, v := range i.GetSpec().GetVolumes() {
			m[v.GetVolume()] = v.DeleteOnTermination
		}
		return m
	}
	// want checks Get and List. A nil data means no data volume attached.
	want := func(what string, data *bool) *computev1.Instance {
		t.Helper()
		got, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		list, err := h.inst.ListInstances(ctx, &computev1.ListInstancesRequest{Parent: zoneParent})
		if err != nil || len(list.GetInstances()) != 1 {
			t.Fatalf("list = %v, %v", list, err)
		}
		for _, i := range []*computev1.Instance{got, list.GetInstances()[0]} {
			f := flags(i)
			if r := f[volBase+"root-1"]; r == nil || !*r {
				t.Fatalf("%s: root delete_on_termination = %v, want true", what, r)
			}
			d, attached := f[volBase+"data"]
			switch {
			case data == nil && attached:
				t.Fatalf("%s: data volume attached, want none", what)
			case data != nil && (d == nil || *d != *data):
				t.Fatalf("%s: data delete_on_termination = %v, want %v", what, d, *data)
			}
		}
		return got
	}
	want("create", nil)

	update := func(req string, data *bool) string {
		t.Helper()
		op, err := h.inst.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
			RequestId: req,
			Instance: &computev1.Instance{Name: name, Spec: &computev1.InstanceSpec{
				Power: computev1.InstancePower_INSTANCE_POWER_RUNNING,
				Volumes: []*computev1.VolumeAttachment{
					{Volume: volBase + "root-1", DeviceName: "vda", Boot: true},
					{Volume: volBase + "data", DeviceName: "vdb", DeleteOnTermination: data},
				},
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		return op.GetName()
	}
	update("92929292-9292-4292-8292-929292929292", proto.Bool(true))
	attached := want("attach with true", proto.Bool(true))
	var at int64
	for _, v := range attached.GetSpec().GetVolumes() {
		if v.GetVolume() == volBase+"data" {
			at = v.GetAttachTime().AsTime().UnixNano()
		}
	}

	flip := "93939393-9393-4393-8393-939393939393"
	first := update(flip, proto.Bool(false))
	flipped := want("flip to false", proto.Bool(false))
	for _, v := range flipped.GetSpec().GetVolumes() {
		if v.GetVolume() == volBase+"data" && v.GetAttachTime().AsTime().UnixNano() != at {
			t.Fatalf("flipping delete_on_termination moved attach_time to %v", v.GetAttachTime().AsTime())
		}
	}
	if again := update(flip, proto.Bool(false)); again != first {
		t.Fatalf("replay = operation %s, want %s", again, first)
	}

	update("94949494-9494-4494-8494-949494949494", proto.Bool(true))
	want("set true again", proto.Bool(true))
	update("95959595-9595-4595-8595-959595959595", nil)
	want("unset on update", proto.Bool(false))
}
