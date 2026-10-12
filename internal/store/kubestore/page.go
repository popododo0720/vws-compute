package kubestore

import (
	"slices"
	"strings"

	"github.com/popododo0720/vws-compute/internal/store"
)

// pageOf selects the entries whose key is greater than after, restricted to
// only when only is non-empty, orders them by key, and cuts one page.
func pageOf[T any](es []*entry, only []string, after string, size int, read func(*entry) (string, T)) store.Page[T] {
	type item struct {
		key string
		val T
	}
	var items []item
	for _, e := range es {
		k, v := read(e)
		if k <= after || (len(only) > 0 && !slices.Contains(only, k)) {
			continue
		}
		items = append(items, item{k, v})
	}
	slices.SortFunc(items, func(a, b item) int { return strings.Compare(a.key, b.key) })
	limit := store.PageLimit(size)
	var p store.Page[T]
	if len(items) > limit {
		items = items[:limit]
		p.Next = items[limit-1].key
	}
	p.Items = make([]T, 0, len(items))
	for _, it := range items {
		p.Items = append(p.Items, it.val)
	}
	return p
}
