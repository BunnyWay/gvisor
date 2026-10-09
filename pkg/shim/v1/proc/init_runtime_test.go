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

package proc

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/pkg/protobuf/types"
	"github.com/containerd/containerd/v2/pkg/stdio"
	"golang.org/x/sys/unix"
	"gvisor.dev/gvisor/pkg/shim/v1/runsccmd"
)

func newWedgedInit(t *testing.T) *Init {
	t.Helper()
	old := runscTimeout
	runscTimeout = 200 * time.Millisecond
	t.Cleanup(func() { runscTimeout = old })
	fake := filepath.Join(t.TempDir(), "fake-runsc")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexec >/dev/null 2>&1\nexec sleep 60\n"), 0o755); err != nil {
		t.Fatalf("write fake runsc: %v", err)
	}
	p := New("test", &runsccmd.Runsc{Command: fake}, stdio.Stdio{})
	p.initState = &runningState{p: p}
	return p
}

func requireBounded(t *testing.T, start time.Time) {
	t.Helper()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("operation returned after %v, want under 2s", elapsed)
	}
}

func TestRuntimeCallsAreBounded(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*Init) error
	}{
		{"Status", func(p *Init) error { _, err := p.Status(context.Background()); return err }},
		{"Stats", func(p *Init) error { _, err := p.Stats(context.Background(), "test"); return err }},
		{"Kill", func(p *Init) error { return p.Kill(context.Background(), uint32(unix.SIGTERM), false) }},
		{"Update", func(p *Init) error { return p.Update(context.Background(), &types.Any{Value: []byte(`{}`)}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newWedgedInit(t)
			start := time.Now()
			if err := tc.call(p); err == nil {
				t.Fatalf("%s succeeded against a wedged runsc", tc.name)
			}
			requireBounded(t, start)
		})
	}
}

func TestCanceledCallerDoesNotWaitBehindContainerLock(t *testing.T) {
	p := newWedgedInit(t)
	p.mu.Lock()
	defer p.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if _, err := p.Status(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Status error = %v, want context.Canceled", err)
	}
	requireBounded(t, start)
}

func TestDeleteTimeoutKillsOnlyPinnedSandboxProcess(t *testing.T) {
	sandbox := exec.Command("sleep", "60")
	if err := sandbox.Start(); err != nil {
		t.Fatalf("start sandbox: %v", err)
	}
	t.Cleanup(func() {
		_ = sandbox.Process.Kill()
		_ = sandbox.Wait()
	})
	neighbor := exec.Command("sleep", "60")
	if err := neighbor.Start(); err != nil {
		t.Fatalf("start neighbor: %v", err)
	}
	t.Cleanup(func() {
		_ = neighbor.Process.Kill()
		_ = neighbor.Wait()
	})
	pidfd, err := unix.PidfdOpen(sandbox.Process.Pid, 0)
	if err != nil {
		t.Fatalf("pidfd_open: %v", err)
	}

	p := newWedgedInit(t)
	p.Sandbox = true
	p.sandboxPidfd = pidfd
	p.initState = &stoppedState{process: p}
	start := time.Now()
	if err := p.Delete(context.Background()); err == nil {
		t.Fatal("Delete succeeded against a wedged runsc")
	}
	requireBounded(t, start)

	done := make(chan error, 1)
	go func() { done <- sandbox.Wait() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sandbox survived its own timed-out Delete")
	}
	if err := neighbor.Process.Signal(unix.Signal(0)); err != nil {
		t.Fatalf("neighbor was killed with the timed-out sandbox: %v", err)
	}
}

func TestDeleteTimeoutBehindRuntimeLockKillsPinnedSandbox(t *testing.T) {
	sandbox := exec.Command("sleep", "60")
	if err := sandbox.Start(); err != nil {
		t.Fatalf("start sandbox: %v", err)
	}
	t.Cleanup(func() {
		_ = sandbox.Process.Kill()
		_ = sandbox.Wait()
	})
	pidfd, err := unix.PidfdOpen(sandbox.Process.Pid, 0)
	if err != nil {
		t.Fatalf("pidfd_open: %v", err)
	}

	p := newWedgedInit(t)
	p.Sandbox = true
	p.sandboxPidfd = pidfd
	p.initState = &stoppedState{process: p}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.Delete(context.Background()); !errors.Is(err, errRunscTimeout) {
		t.Fatalf("Delete error = %v, want shim timeout", err)
	}

	done := make(chan error, 1)
	go func() { done <- sandbox.Wait() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sandbox survived Delete timing out behind a wedged runtime call")
	}
}

func TestDeleteTimeoutDoesNotKillAWorkloadProcess(t *testing.T) {
	process := exec.Command("sleep", "60")
	if err := process.Start(); err != nil {
		t.Fatalf("start process: %v", err)
	}
	t.Cleanup(func() {
		_ = process.Process.Kill()
		_ = process.Wait()
	})
	pidfd, err := unix.PidfdOpen(process.Process.Pid, 0)
	if err != nil {
		t.Fatalf("pidfd_open: %v", err)
	}

	p := newWedgedInit(t)
	p.Sandbox = false
	p.sandboxPidfd = pidfd
	p.initState = &stoppedState{process: p}
	_ = p.Delete(context.Background())
	p.closeSandboxPidfd()
	if err := process.Process.Signal(unix.Signal(0)); err != nil {
		t.Fatalf("workload process was killed: %v", err)
	}
}

func TestExplicitSandboxKillTimeoutKillsPinnedSandbox(t *testing.T) {
	sandbox := exec.Command("sleep", "60")
	if err := sandbox.Start(); err != nil {
		t.Fatalf("start sandbox: %v", err)
	}
	t.Cleanup(func() {
		_ = sandbox.Process.Kill()
		_ = sandbox.Wait()
	})
	pidfd, err := unix.PidfdOpen(sandbox.Process.Pid, 0)
	if err != nil {
		t.Fatalf("pidfd_open: %v", err)
	}

	p := newWedgedInit(t)
	p.Sandbox = true
	p.sandboxPidfd = pidfd
	if err := p.Kill(context.Background(), uint32(unix.SIGKILL), true); err == nil {
		t.Fatal("Kill succeeded against a wedged runsc")
	}

	done := make(chan error, 1)
	go func() { done <- sandbox.Wait() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sandbox survived explicit SIGKILL --all timing out")
	}
}

func TestSandboxKillTimeoutDoesNotEscalateNonDestructiveSignal(t *testing.T) {
	sandbox := exec.Command("sleep", "60")
	if err := sandbox.Start(); err != nil {
		t.Fatalf("start sandbox: %v", err)
	}
	t.Cleanup(func() {
		_ = sandbox.Process.Kill()
		_ = sandbox.Wait()
	})
	pidfd, err := unix.PidfdOpen(sandbox.Process.Pid, 0)
	if err != nil {
		t.Fatalf("pidfd_open: %v", err)
	}

	p := newWedgedInit(t)
	p.Sandbox = true
	p.sandboxPidfd = pidfd
	_ = p.Kill(context.Background(), uint32(unix.SIGTERM), true)
	if err := sandbox.Process.Signal(unix.Signal(0)); err != nil {
		t.Fatalf("sandbox was killed for a timed-out SIGTERM: %v", err)
	}
	p.closeSandboxPidfd()
}

func TestAutomaticKillAllTimeoutDoesNotKillSandbox(t *testing.T) {
	sandbox := exec.Command("sleep", "60")
	if err := sandbox.Start(); err != nil {
		t.Fatalf("start sandbox: %v", err)
	}
	t.Cleanup(func() {
		_ = sandbox.Process.Kill()
		_ = sandbox.Wait()
	})
	pidfd, err := unix.PidfdOpen(sandbox.Process.Pid, 0)
	if err != nil {
		t.Fatalf("pidfd_open: %v", err)
	}

	p := newWedgedInit(t)
	p.Sandbox = true
	p.sandboxPidfd = pidfd
	p.KillAll(context.Background())
	if err := sandbox.Process.Signal(unix.Signal(0)); err != nil {
		t.Fatalf("automatic KillAll killed the whole sandbox: %v", err)
	}
	p.closeSandboxPidfd()
}

func TestDeleteCallerDeadlineDoesNotKillSandbox(t *testing.T) {
	sandbox := exec.Command("sleep", "60")
	if err := sandbox.Start(); err != nil {
		t.Fatalf("start sandbox: %v", err)
	}
	t.Cleanup(func() {
		_ = sandbox.Process.Kill()
		_ = sandbox.Wait()
	})
	pidfd, err := unix.PidfdOpen(sandbox.Process.Pid, 0)
	if err != nil {
		t.Fatalf("pidfd_open: %v", err)
	}

	p := newWedgedInit(t)
	p.Sandbox = true
	p.sandboxPidfd = pidfd
	p.initState = &stoppedState{process: p}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := p.Delete(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Delete error = %v, want context deadline", err)
	}
	if err := sandbox.Process.Signal(unix.Signal(0)); err != nil {
		t.Fatalf("sandbox was killed for its caller's deadline: %v", err)
	}
	p.closeSandboxPidfd()
}
