package sqlitestore

import (
	"encoding/json"
	"errors"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// The controller-driven instance columns are canonical JSON. Every value is
// re-validated through compute's parsers on read.

type volumeJSON struct {
	Volume string `json:"volume"`
	Device string `json:"device"`
	Boot   bool   `json:"boot,omitempty"`
}

type nicJSON struct {
	Device         string   `json:"device"`
	Subnet         string   `json:"subnet"`
	PrivateIPv4    string   `json:"private_ipv4,omitempty"`
	SecurityGroups []string `json:"security_groups,omitempty"`
}

type reportJSON struct {
	Node       string          `json:"node,omitempty"`
	State      uint8           `json:"state,omitempty"`
	Message    string          `json:"message,omitempty"`
	Power      uint8           `json:"power,omitempty"`
	Interfaces []observedNICJS `json:"interfaces,omitempty"`
}

type observedNICJS struct {
	Device      string `json:"device"`
	Subnet      string `json:"subnet,omitempty"`
	PrivateIPv4 string `json:"private_ipv4,omitempty"`
	PublicIPv4  string `json:"public_ipv4,omitempty"`
}

func volumesJSON(vs []compute.VolumeAttachment) (string, error) {
	out := make([]volumeJSON, len(vs))
	for i, v := range vs {
		out[i] = volumeJSON{Volume: v.Volume.String(), Device: v.DeviceName, Boot: v.Boot}
	}
	b, err := json.Marshal(out)
	return string(b), err
}

func parseVolumesJSON(s string) ([]compute.VolumeAttachment, error) {
	var in []volumeJSON
	if err := json.Unmarshal([]byte(s), &in); err != nil {
		return nil, err
	}
	var out []compute.VolumeAttachment
	var errs []error
	for _, v := range in {
		ref, err := compute.ParseVolumeRef(v.Volume)
		errs = append(errs, err)
		out = append(out, compute.VolumeAttachment{Volume: ref, DeviceName: v.Device, Boot: v.Boot})
	}
	return out, errors.Join(errs...)
}

func nicsJSON(ns []compute.NetworkInterface) (string, error) {
	out := make([]nicJSON, len(ns))
	for i, n := range ns {
		out[i] = nicJSON{Device: n.DeviceName, Subnet: n.Subnet, PrivateIPv4: n.PrivateIPv4, SecurityGroups: n.SecurityGroups}
	}
	b, err := json.Marshal(out)
	return string(b), err
}

func parseNICsJSON(s string) ([]compute.NetworkInterface, error) {
	var in []nicJSON
	if err := json.Unmarshal([]byte(s), &in); err != nil {
		return nil, err
	}
	var out []compute.NetworkInterface
	for _, n := range in {
		out = append(out, compute.NetworkInterface{DeviceName: n.Device, Subnet: n.Subnet, PrivateIPv4: n.PrivateIPv4, SecurityGroups: n.SecurityGroups})
	}
	return out, nil
}

func reportToJSON(r compute.Report) (string, error) {
	out := reportJSON{Node: r.Node, State: uint8(r.State), Message: r.Message, Power: uint8(r.Power)}
	for _, i := range r.Interfaces {
		out.Interfaces = append(out.Interfaces, observedNICJS{Device: i.DeviceName, Subnet: i.Subnet, PrivateIPv4: i.PrivateIPv4, PublicIPv4: i.PublicIPv4})
	}
	b, err := json.Marshal(out)
	return string(b), err
}

func parseReportJSON(s string) (compute.Report, error) {
	var in reportJSON
	if err := json.Unmarshal([]byte(s), &in); err != nil {
		return compute.Report{}, err
	}
	if in.State > uint8(compute.ReportError) || in.Power > uint8(compute.PowerTerminated) {
		return compute.Report{}, errors.New("report out of range")
	}
	out := compute.Report{Node: in.Node, State: compute.ReportState(in.State), Message: in.Message, Power: compute.Power(in.Power)}
	for _, i := range in.Interfaces {
		out.Interfaces = append(out.Interfaces, compute.ObservedInterface{DeviceName: i.Device, Subnet: i.Subnet, PrivateIPv4: i.PrivateIPv4, PublicIPv4: i.PublicIPv4})
	}
	return out, nil
}
