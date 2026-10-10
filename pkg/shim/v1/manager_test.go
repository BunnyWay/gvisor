// Copyright 2026 The gVisor Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/containerd/containerd/v2/pkg/namespaces"
	"gvisor.dev/gvisor/pkg/shim/v1/runsc"

	typeurl "github.com/containerd/typeurl/v2"
	"github.com/opencontainers/runtime-spec/specs-go/features"

	"testing"
)

func TestManagerStopBoundsWedgedRuntime(t *testing.T) {
	old := stopRunscTimeout
	stopRunscTimeout = 200 * time.Millisecond
	t.Cleanup(func() { stopRunscTimeout = old })
	dir := t.TempDir()
	t.Chdir(dir)
	fake := filepath.Join(dir, "fake-runsc")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexec >/dev/null 2>&1\nexec sleep 60\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	st := runsc.State{Options: runsc.Options{BinaryName: fake, Root: dir}}
	if err := st.Save(dir); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	status, err := (&manager{}).Stop(namespaces.WithNamespace(context.Background(), "test"), "test")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop error = %v, want deadline failure", err)
	}
	if !status.ExitedAt.IsZero() || status.ExitStatus != 0 {
		t.Fatalf("failed Stop reported success: %+v", status)
	}
	if elapsed := time.Since(start); elapsed < stopRunscTimeout || elapsed > 2*time.Second {
		t.Fatalf("Stop returned after %v, want bounded runtime timeout", elapsed)
	}
}

// TestManagerInfo verifies that the Info() call returns the expected information
// about the runtime.
func TestManagerInfo(t *testing.T) {
	m := NewShimManager("io.containerd.runsc.v1")
	info, err := m.Info(context.Background(), bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("Standalone ShimManager::Info call returned unexpected error: %v", err)
	}
	if info == nil {
		t.Fatalf("ShimManager::Info got nil, want non-nil RuntimeInfo")
	}
	if info.Features == nil {
		t.Fatalf("ShimManager::Info got nil, want populated Features struct from specutils")
	}

	decoded, err := typeurl.UnmarshalAny(info.Features)
	if err != nil {
		t.Fatalf("Failed to deserialize info.Features Any proto: %v", err)
	}

	_, ok := decoded.(*features.Features)
	if !ok {
		t.Fatalf("Decoded features is type %T, want *features.Features", decoded)
	}

}

func TestManagerStopBoundsBlockedUnmountAndReportsFailure(t *testing.T) {
	oldTimeout, oldUnmount := stopRunscTimeout, unmountRootfs
	stopRunscTimeout = 200 * time.Millisecond
	dir := t.TempDir()
	t.Chdir(dir)
	fake := filepath.Join(dir, "successful-runsc")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	st := runsc.State{Options: runsc.Options{BinaryName: fake, Root: dir}, Rootfs: dir}
	if err := st.Save(dir); err != nil {
		t.Fatal(err)
	}
	release, finished := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var finishOnce sync.Once
	unmountRootfs = func(path string, flags int) error {
		calls.Add(1)
		defer finishOnce.Do(func() { close(finished) })
		if path != dir || flags != 0 {
			return fmt.Errorf("unmount(%q, %d), want (%q, 0)", path, flags, dir)
		}
		<-release
		return nil
	}
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(func() { unblock(); <-finished; stopRunscTimeout, unmountRootfs = oldTimeout, oldUnmount })
	ctx := namespaces.WithNamespace(context.Background(), "test")
	for range 2 {
		start := time.Now()
		status, err := (&manager{}).Stop(ctx, "test")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Stop error = %v, want unmount timeout", err)
		}
		if !status.ExitedAt.IsZero() || status.ExitStatus != 0 {
			t.Fatalf("failed Stop reported success: %+v", status)
		}
		if time.Since(start) > 2*time.Second {
			t.Fatal("Stop waited indefinitely for unmount")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("Stop retries started %d blocked workers, want one", calls.Load())
	}
	unblock()
	<-finished
	if _, err := (&manager{}).Stop(ctx, "test"); err != nil {
		t.Fatalf("Stop retry after unmount recovery: %v", err)
	}
}
