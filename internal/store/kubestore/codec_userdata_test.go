package kubestore

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// TestInstanceRecordKeepsUserDataRaw pins vws-compute#148: user_data_raw is
// stored base64 in the record, so bytes that are not UTF-8 survive JSON, and
// a record written before the field existed decodes with it unset.
func TestInstanceRecordKeepsUserDataRaw(t *testing.T) {
	const raw = "\xff\xfe#!/bin/sh\x00\x80echo hi\n"
	ref := compute.Ref{Kind: compute.KindInstance, ID: "web-1"}
	base := instanceJSON{
		ID: "web-1", Generation: 1, Type: "vws.small", Power: uint8(compute.PowerRunning), Project: "p", Zone: "z",
		Volumes: []volumeJSON{{Volume: "projects/p/zones/z/volumes/root", Device: "vda", Boot: true}},
	}

	in := base
	in.UserDataRaw = []byte(raw)
	got := decodeRecord(t, ref, recordJSON{Instance: &in})
	if got.inst.Spec.UserDataRaw != raw || got.inst.Spec.UserData != "" {
		t.Fatalf("decoded user data = %q raw %q, want raw %q", got.inst.Spec.UserData, got.inst.Spec.UserDataRaw, raw)
	}
	again := instanceTo(got.inst)
	if string(again.UserDataRaw) != raw {
		t.Fatalf("re-encoded raw user data = %q, want %q", again.UserDataRaw, raw)
	}
	b, err := json.Marshal(again)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(b) || !strings.Contains(string(b), `"user_data_raw":"//4jIS9iaW4vc2gAgGVjaG8gaGkK"`) {
		t.Fatalf("record = %s, want user_data_raw as base64", b)
	}

	// The record an older build wrote: user_data text and no user_data_raw key.
	legacy := `{"instance":{"id":"web-1","generation":1,"created_at":0,"changed_at":0,"type":"vws.small","power":1,` +
		`"project":"p","zone":"z","user_data":"#cloud-config",` +
		`"volumes":[{"volume":"projects/p/zones/z/volumes/root","device":"vda","boot":true}],` +
		`"observed":{},"retry":{},"report":{},"meta":{}}}`
	var rec recordJSON
	if err := json.Unmarshal([]byte(legacy), &rec); err != nil {
		t.Fatal(err)
	}
	old := decodeRecord(t, ref, rec)
	if old.inst.Spec.UserData != "#cloud-config" || old.inst.Spec.UserDataRaw != "" {
		t.Fatalf("legacy record user data = %q raw %q", old.inst.Spec.UserData, old.inst.Spec.UserDataRaw)
	}
	if enc := instanceTo(old.inst); enc.UserDataRaw != nil {
		t.Fatalf("legacy record gained user_data_raw %q on re-encode", enc.UserDataRaw)
	}
}
