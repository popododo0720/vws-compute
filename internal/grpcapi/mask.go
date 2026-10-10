package grpcapi

import (
	"strings"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// metadataPatch is the metadata change paths select from a request body.
//
// labels is parsed even when paths leave it alone, so a bad map is
// INVALID_SPEC on every update. "labels" replaces the whole map and
// subsumes any labels.<key> path. "labels.<key>" sets that key, or deletes
// it when the body has no such entry.
//
// description is applied only when non-nil. Callers pass it for an image
// when paths name spec or spec.description. spec.family does not.
//
// keepStoredOnEmptyReplace is set for an update that also names spec. An
// empty label map is what an older client sends for "no labels", and a
// power change must keep the StopInstance fingerprint those clients stored.
// A labels-only mask with an empty map still clears.
func metadataPatch(paths []string, labels map[string]string, description *string, keepStoredOnEmptyReplace bool) (compute.MetadataPatch, error) {
	parsed, err := compute.ParseLabels(labels)
	if err != nil {
		return compute.MetadataPatch{}, err
	}
	var patch compute.MetadataPatch
	replace, keys := labelPaths(paths)
	switch {
	case replace && (!keepStoredOnEmptyReplace || parsed.Len() != 0):
		patch = patch.ReplaceLabels(parsed)
	case !replace && len(keys) > 0:
		set := map[string]string{}
		var remove []string
		for _, k := range keys {
			if v, ok := labels[k]; ok {
				set[k] = v
			} else {
				remove = append(remove, k)
			}
		}
		setLabels, err := compute.ParseLabels(set)
		if err != nil {
			return compute.MetadataPatch{}, err
		}
		patch, err = patch.EditLabels(setLabels, remove)
		if err != nil {
			return compute.MetadataPatch{}, err
		}
	}
	if description != nil {
		d, err := compute.ParseDescription(*description)
		if err != nil {
			return compute.MetadataPatch{}, err
		}
		patch = patch.SetDescription(d)
	}
	return patch, nil
}

func labelPaths(paths []string) (replace bool, keys []string) {
	for _, p := range paths {
		switch {
		case p == "labels":
			replace = true
		case strings.HasPrefix(p, "labels."):
			keys = append(keys, strings.TrimPrefix(p, "labels."))
		}
	}
	if replace {
		return true, nil
	}
	return false, keys
}

// imageDescription returns the description to store when paths select it.
// A whole spec replaces it. spec.description replaces it. spec.family does
// not, and neither does a mask that leaves spec alone.
func imageDescription(paths []string, spec *computev1.ImageSpec) *string {
	selected := false
	for _, p := range paths {
		if p == "spec" || p == "spec.description" {
			selected = true
			break
		}
	}
	if !selected {
		return nil
	}
	d := ""
	if spec != nil {
		d = spec.GetDescription()
	}
	return &d
}
