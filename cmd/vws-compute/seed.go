package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/store"
)

// seedDoc is the -seed-images file. images may be empty. Each entry's id is
// the resource path segment clients use, family is spec.family, and path is
// an absolute disk image file on the agent's host.
type seedDoc struct {
	Images []seedImage `json:"images"`
}

type seedImage struct {
	ID     string `json:"id"`
	Family string `json:"family"`
	Path   string `json:"path"`
}

// seedEntry is one validated image and the request id that turns a second
// registration of the same content into a replay.
type seedEntry struct {
	id   compute.ImageID
	path string
	req  compute.RequestID
	in   compute.Intent
}

// readSeed reads and validates path. An empty path is no seed. Every path
// is statted before any image is registered.
func readSeed(path string) ([]seedEntry, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("seed images: %w", err)
	}
	defer f.Close()
	entries, err := parseSeed(f)
	if err != nil {
		return nil, fmt.Errorf("seed images %s: %w", path, err)
	}
	for _, e := range entries {
		info, err := os.Stat(e.path)
		if err != nil {
			return nil, fmt.Errorf("seed image %s: %w", e.id, err)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("seed image %s: %s is not a regular file", e.id, e.path)
		}
	}
	return entries, nil
}

// parseSeed decodes one document. Unknown fields, trailing data, a repeated
// id, and a value the domain rejects are errors, and nothing is submitted.
func parseSeed(r io.Reader) ([]seedEntry, error) {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	var doc seedDoc
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	if err := dec.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("trailing data after the document")
		}
		return nil, err
	}
	seen := map[compute.ImageID]bool{}
	entries := make([]seedEntry, 0, len(doc.Images))
	for i, img := range doc.Images {
		id, err := compute.ParseImageID(img.ID)
		if err != nil {
			return nil, fmt.Errorf("images[%d]: %w", i, err)
		}
		if seen[id] {
			return nil, fmt.Errorf("images[%d]: id %s repeats", i, id)
		}
		seen[id] = true
		in, err := compute.NewRegisterFile(img.ID, img.Path, img.Family)
		if err != nil {
			return nil, fmt.Errorf("images[%d]: %w", i, err)
		}
		req, err := seedRequestID(id)
		if err != nil {
			return nil, fmt.Errorf("images[%d]: %w", i, err)
		}
		entries = append(entries, seedEntry{id: id, path: img.Path, req: req, in: in})
	}
	return entries, nil
}

// seedRequestID is "seed-" plus 32 hex characters of sha256(id). It fits a
// request id, it is stable across restarts, and it does not depend on the
// path, so a changed path is a fingerprint mismatch rather than a new image.
func seedRequestID(id compute.ImageID) (compute.RequestID, error) {
	sum := sha256.Sum256([]byte(id.String()))
	return compute.ParseRequestID("seed-" + hex.EncodeToString(sum[:16]))
}

// applySeed registers every entry through ctl and waits until each operation
// settles. A replay writes nothing. A changed entry, a taken id, a deleted
// image, or a failed copy stops startup.
func applySeed(ctx context.Context, ctl *controller.Controller, r store.Reader, entries []seedEntry) error {
	ops := make([]compute.Operation, len(entries))
	for i, e := range entries {
		rc, err := ctl.Submit(ctx, e.req, e.in)
		switch {
		case compute.CodeOf(err) == compute.CodeRequestMismatch:
			return fmt.Errorf("seed image %s changed since it was registered; register it under a new id", e.id)
		case compute.CodeOf(err) == compute.CodeAlreadyExists:
			return fmt.Errorf("seed image %s already exists", e.id)
		case err != nil:
			return fmt.Errorf("seed image %s: %w", e.id, err)
		}
		ops[i] = rc.Operation
	}
	for i, e := range entries {
		op, err := settle(ctx, r, ops[i].ID)
		if err != nil {
			return fmt.Errorf("seed image %s: %w", e.id, err)
		}
		if op.State() != compute.OperationSucceeded {
			msg := op.State().String()
			if op.Failure() != nil {
				msg = op.Failure().Message
			}
			return fmt.Errorf("seed image %s: %s", e.id, msg)
		}
		img, err := r.Image(ctx, e.id)
		if err != nil {
			return fmt.Errorf("seed image %s: %w", e.id, err)
		}
		if img.Lifecycle() == compute.ImageDeregistered {
			return fmt.Errorf("seed image %s was deleted; register it under a new id", e.id)
		}
		if !img.Available() {
			return fmt.Errorf("seed image %s is not available", e.id)
		}
		slog.Info("seed image ready", "id", e.id)
	}
	return nil
}

const settlePoll = 20 * time.Millisecond

// settle polls r until operation id is terminal or ctx ends.
func settle(ctx context.Context, r store.Reader, id compute.OperationID) (compute.Operation, error) {
	ticker := time.NewTicker(settlePoll)
	defer ticker.Stop()
	for {
		op, err := r.Operation(ctx, id)
		if err != nil || op.State().Terminal() {
			return op, err
		}
		select {
		case <-ctx.Done():
			return compute.Operation{}, ctx.Err()
		case <-ticker.C:
		}
	}
}
