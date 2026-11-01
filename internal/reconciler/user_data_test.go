package reconciler_test

import (
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// TestDrivenUserDataReachesTheAgent pins vws-compute#148: user_data_raw goes
// to the agent byte for byte, as user_data does.
func TestDrivenUserDataReachesTheAgent(t *testing.T) {
	const raw = "\xff\xfe#!/bin/sh\x00\x80echo hi\n"
	for _, tc := range []struct {
		name      string
		text, raw string
		want      string
	}{
		{"user_data", "#cloud-config\n", "", "#cloud-config\n"},
		{"user_data_raw", "", raw, raw},
		{"neither", "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eachStore(t, func(h *harness) {
				drivenHarness(h)
				h.submit("create", ok(compute.NewCreateInstance(compute.CreateInstanceParams{
					ID: "web", Project: "p", Zone: "z", MachineType: "vws.small", Power: compute.PowerRunning,
					UserData: tc.text, UserDataRaw: tc.raw,
					Volumes: []compute.VolumeAttachment{{
						Volume: ok(compute.ParseVolumeRef(bootVolume)), DeviceName: "vda", Boot: true,
					}},
				})))
				h.sweep()
				m, found := h.agent.Machine("web")
				if !found || m.UserData != tc.want {
					h.t.Fatalf("agent user data = %q (machine found %v), want %q", m.UserData, found, tc.want)
				}
			})
		})
	}
}
