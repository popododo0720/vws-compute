package grpcapi

import (
	"fmt"
	"regexp"
	"strings"
)

// ResourceId is the path-segment grammar from vws.common.v1.ResourceId.
var resourceIDPattern = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$`)

// machineTypePattern is InstanceSpec.machine_type.
var machineTypePattern = regexp.MustCompile(`^[a-z]([a-z0-9.-]{0,61}[a-z0-9])?$`)

// placeholderLocation is the zone or region GetOperation uses. That RPC
// knows the project and not the resource's parent, and instances and images
// are not stored under a location. Mutation responses echo the caller's name
// instead.
const placeholderLocation = "default"

// uuid4Pattern is the request_id format declared on every mutation.
var uuid4Pattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func validateResourceID(id string) error {
	if !resourceIDPattern.MatchString(id) {
		return invalidID(fmt.Sprintf("resource id %q does not match vws.common.v1.ResourceId", id))
	}
	return nil
}

func checkRequestID(id string) error {
	if id == "" || uuid4Pattern.MatchString(id) {
		return nil
	}
	return invalidSpec("request_id must be a UUID")
}

// parseScopedName splits projects/{project}/{scopeKind}/{scope}/{collection}/{id}.
func parseScopedName(name, scopeKind, collection string) (project, scope, id string, err error) {
	parts := strings.Split(name, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != scopeKind || parts[4] != collection {
		return "", "", "", invalidID(fmt.Sprintf("name %q is not projects/*/%s/*/%s/*", name, scopeKind, collection))
	}
	if err := validateResourceID(parts[1]); err != nil {
		return "", "", "", err
	}
	if err := validateResourceID(parts[3]); err != nil {
		return "", "", "", err
	}
	if err := validateResourceID(parts[5]); err != nil {
		return "", "", "", err
	}
	return parts[1], parts[3], parts[5], nil
}

func parseInstanceName(name string) (project, zone, id string, err error) {
	return parseScopedName(name, "zones", "instances")
}

func parseImageName(name string) (project, region, id string, err error) {
	return parseScopedName(name, "regions", "images")
}

func parseKeyPairName(name string) (project, region, id string, err error) {
	return parseScopedName(name, "regions", "keyPairs")
}

func parseVolumeName(name string) (project, zone, id string, err error) {
	return parseScopedName(name, "zones", "volumes")
}

func parseSnapshotName(name string) (project, region, id string, err error) {
	return parseScopedName(name, "regions", "snapshots")
}

func parseSubnetName(name string) error {
	parts := strings.Split(name, "/")
	if len(parts) != 8 || parts[0] != "projects" || parts[2] != "regions" || parts[4] != "vpcs" || parts[6] != "subnets" {
		return invalidID(fmt.Sprintf("subnet %q is not projects/*/regions/*/vpcs/*/subnets/*", name))
	}
	for _, i := range []int{1, 3, 5, 7} {
		if err := validateResourceID(parts[i]); err != nil {
			return err
		}
	}
	return nil
}

func parseSecurityGroupName(name string) error {
	parts := strings.Split(name, "/")
	if len(parts) != 8 || parts[0] != "projects" || parts[2] != "regions" || parts[4] != "vpcs" || parts[6] != "securityGroups" {
		return invalidID(fmt.Sprintf("security group %q is not projects/*/regions/*/vpcs/*/securityGroups/*", name))
	}
	for _, i := range []int{1, 3, 5, 7} {
		if err := validateResourceID(parts[i]); err != nil {
			return err
		}
	}
	return nil
}

func parseZonalParent(parent string) (project, zone string, err error) {
	parts := strings.Split(parent, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "zones" {
		return "", "", invalidID(fmt.Sprintf("parent %q is not projects/*/zones/*", parent))
	}
	if err := validateResourceID(parts[1]); err != nil {
		return "", "", err
	}
	if err := validateResourceID(parts[3]); err != nil {
		return "", "", err
	}
	return parts[1], parts[3], nil
}

func parseRegionalParent(parent string) (project, region string, err error) {
	parts := strings.Split(parent, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "regions" {
		return "", "", invalidID(fmt.Sprintf("parent %q is not projects/*/regions/*", parent))
	}
	if err := validateResourceID(parts[1]); err != nil {
		return "", "", err
	}
	if err := validateResourceID(parts[3]); err != nil {
		return "", "", err
	}
	return parts[1], parts[3], nil
}

func parseOperationName(name string) (project, id string, err error) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "operations" {
		return "", "", invalidID(fmt.Sprintf("name %q is not projects/*/operations/*", name))
	}
	if err := validateResourceID(parts[1]); err != nil {
		return "", "", err
	}
	if err := validateResourceID(parts[3]); err != nil {
		return "", "", err
	}
	return parts[1], parts[3], nil
}

func instanceResourceName(project, zone, id string) string {
	return "projects/" + project + "/zones/" + zone + "/instances/" + id
}

func imageResourceName(project, region, id string) string {
	return "projects/" + project + "/regions/" + region + "/images/" + id
}

func keyPairResourceName(project, region, id string) string {
	return "projects/" + project + "/regions/" + region + "/keyPairs/" + id
}

func operationResourceName(project, id string) string {
	return "projects/" + project + "/operations/" + id
}
