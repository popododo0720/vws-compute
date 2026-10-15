package kubevirt

import (
	"context"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

// LabelProject, LabelRegion and LabelPublic are the authorization labels of
// an image DataSource. vws-storage clones a DataSource only for the project
// and region on them, or for every project in the region when it carries
// vws.io/public=true (vws-storage#89). AuthorizeImages is their only writer.
const (
	LabelProject = "vws.io/project"
	LabelRegion  = "vws.io/region"
	LabelPublic  = "vws.io/public"
)

var authKeys = []string{LabelProject, LabelRegion, LabelPublic}

// authLabels are the labels auth stands for. The zero ImageAuth has none.
func authLabels(auth hypervisor.ImageAuth) map[string]string {
	if auth.Project == "" || auth.Region == "" {
		return map[string]string{}
	}
	l := map[string]string{LabelProject: auth.Project, LabelRegion: auth.Region}
	if auth.Public {
		l[LabelPublic] = "true"
	}
	return l
}

// AuthorizeImages lists every managed DataSource in the image namespace and
// brings its authorization labels to auth(name) with at most one patch each.
// It does not look at the DataVolume, the source or any VirtualMachine, so no
// image state can skip it. DataSources not managed here are left alone.
func (a *Agent) AuthorizeImages(ctx context.Context, auth func(name string) hypervisor.ImageAuth) error {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(dsGVK.GroupVersion().WithKind(dsGVK.Kind + "List"))
	if err := a.c.List(ctx, list, client.InNamespace(a.cfg.ImageNamespace), client.MatchingLabels{LabelManaged: "true"}); err != nil {
		return fmt.Errorf("list image DataSources: %w", err)
	}
	var errs []error
	for i := range list.Items {
		ds := &list.Items[i]
		if err := a.authorize(ctx, ds, authLabels(auth(ds.GetName()))); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (a *Agent) authorize(ctx context.Context, ds *unstructured.Unstructured, want map[string]string) error {
	cur := ds.GetLabels()
	stale := false
	for _, k := range authKeys {
		v, has := cur[k]
		w, wanted := want[k]
		if has != wanted || v != w {
			stale = true
		}
	}
	if !stale {
		return nil
	}
	patch := client.MergeFrom(ds.DeepCopy())
	for _, k := range authKeys {
		if w, wanted := want[k]; wanted {
			cur[k] = w
		} else {
			delete(cur, k)
		}
	}
	ds.SetLabels(cur)
	if err := a.c.Patch(ctx, ds, patch); err != nil {
		return fmt.Errorf("image %s: authorize DataSource: %w", ds.GetName(), err)
	}
	return nil
}
