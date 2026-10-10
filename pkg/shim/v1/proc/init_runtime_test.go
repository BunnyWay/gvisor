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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/console"
	"github.com/containerd/containerd/v2/pkg/protobuf/types"
	"github.com/containerd/containerd/v2/pkg/stdio"
	runc "github.com/containerd/go-runc"
	"golang.org/x/sys/unix"
	"gvisor.dev/gvisor/pkg/shim/v1/runsccmd"
)

func testPidfd(t *testing.T, pid int) int {
	t.Helper()
	fd, err := unix.PidfdOpen(pid, 0)
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) {
		t.Skipf("pidfd unavailable: %v", err)
	}
	if err != nil {
		t.Fatalf("pidfd_open: %v", err)
	}
	if err := unix.PidfdSendSignal(fd, 0, nil, 0); err != nil {
		_ = unix.Close(fd)
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) {
			t.Skipf("pidfd signals unavailable: %v", err)
		}
		t.Fatalf("pidfd_send_signal: %v", err)
	}
	return fd
}

type testProcessMonitor struct{}

func (testProcessMonitor) Subscribe() chan runc.Exit  { return make(chan runc.Exit) }
func (testProcessMonitor) Unsubscribe(chan runc.Exit) {}

func newWedgedInit(t *testing.T) *Init {
	t.Helper()
	old, oldLong := runscTimeout, runscLongOperationTimeout
	runscTimeout = 200 * time.Millisecond
	runscLongOperationTimeout = runscTimeout
	t.Cleanup(func() { runscTimeout, runscLongOperationTimeout = old, oldLong })
	fake := filepath.Join(t.TempDir(), "fake-runsc")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexec >/dev/null 2>&1\nexec sleep 60\n"), 0o755); err != nil {
		t.Fatalf("write fake runsc: %v", err)
	}
	p := New("test", &runsccmd.Runsc{Command: fake, Root: t.TempDir()}, stdio.Stdio{})
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

func TestCreateWedgedRunscIsBounded(t *testing.T) {
	p := newWedgedInit(t)
	p.Bundle = t.TempDir()
	t.Cleanup(p.closeIO)
	start := time.Now()
	if err := p.Create(context.Background(), &CreateConfig{ID: p.id, Bundle: p.Bundle}); err == nil {
		t.Fatal("Create succeeded against a wedged runsc")
	}
	requireBounded(t, start)
}

func TestExecRuntimeCallsAreBounded(t *testing.T) {
	for _, name := range []string{"Start", "Kill"} {
		t.Run(name, func(t *testing.T) {
			p := newWedgedInit(t)
			p.Monitor = testProcessMonitor{}
			e := &execProcess{parent: p, path: t.TempDir(), id: "exec", internalPid: 1, waitBlock: make(chan struct{})}
			e.execState = &execCreatedState{p: e}
			start := time.Now()
			var err error
			if name == "Start" {
				err = e.Start(context.Background())
			} else {
				err = e.Kill(context.Background(), uint32(unix.SIGKILL), true)
			}
			if err == nil {
				t.Fatalf("exec %s succeeded against a wedged runsc", name)
			}
			requireBounded(t, start)
			_ = e.delete(context.Background())
		})
	}
}

func TestExecCanceledCallerDoesNotWaitBehindLock(t *testing.T) {
	e := &execProcess{}
	e.mu.Lock()
	defer e.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, call := range []func(context.Context) error{e.Start, e.Delete, func(ctx context.Context) error {
		return e.Kill(ctx, uint32(unix.SIGKILL), true)
	}} {
		start := time.Now()
		if err := call(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("exec error = %v, want cancellation", err)
		}
		requireBounded(t, start)
	}
}

func TestExecDeleteBoundsIOWaitAndCanRetry(t *testing.T) {
	_ = newWedgedInit(t)
	e := &execProcess{}
	e.execState = &execStoppedState{p: e}
	e.wg.Add(1)
	start := time.Now()
	err := e.Delete(context.Background())
	e.wg.Done()
	if !errors.Is(err, errRunscTimeout) {
		t.Fatalf("Delete error = %v, want shim timeout", err)
	}
	requireBounded(t, start)
	if err := e.Delete(context.Background()); err != nil {
		t.Fatalf("Delete retry: %v", err)
	}
}

func TestDeleteClosesPidfdBeforeFailedUnmount(t *testing.T) {
	p := newWedgedInit(t)
	p.Sandbox = true
	p.sandboxPidfd = testPidfd(t, os.Getpid())
	t.Cleanup(p.closeSandboxPidfd)
	p.initState = &stoppedState{process: p}
	fake := filepath.Join(t.TempDir(), "successful-runsc")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	p.runtime.Command = fake
	old := unmountRootfs
	unmountRootfs = func(string, int) error { return unix.EBUSY }
	t.Cleanup(func() { unmountRootfs = old })
	if err := p.Delete(context.Background()); !errors.Is(err, unix.EBUSY) {
		t.Fatalf("Delete error = %v, want unmount failure", err)
	}
	if p.sandboxPidfd != -1 {
		t.Fatal("successful runtime deletion retained its pidfd")
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
	pidfd := testPidfd(t, sandbox.Process.Pid)

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
	pidfd := testPidfd(t, sandbox.Process.Pid)

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
	pidfd := testPidfd(t, process.Process.Pid)

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
	pidfd := testPidfd(t, sandbox.Process.Pid)

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
	pidfd := testPidfd(t, sandbox.Process.Pid)

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
	pidfd := testPidfd(t, sandbox.Process.Pid)

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
	pidfd := testPidfd(t, sandbox.Process.Pid)

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

func TestMain(m *testing.M) {
	if os.Getenv("GVISOR_SHIM_TEST_RUNTIME_HELPER") == "1" {
		if filepath.Base(os.Args[0]) == "runsc-sandbox" {
			gate := os.NewFile(3, "sandbox-gate")
			token := make([]byte, 1)
			if _, err := gate.Read(token); err == nil && token[0] == 'E' {
				path, err := exec.LookPath("sleep")
				if err != nil || unix.Exec(path, []string{"sleep", "60"}, os.Environ()) != nil {
					os.Exit(2)
				}
			}
			_ = gate.Close()
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func sandboxIdentityProcess(t *testing.T, bundle string, spoof bool) (*exec.Cmd, *os.File) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	process := exec.Command(exe, "--bundle="+bundle)
	if spoof {
		process = exec.Command("/bin/sh", "-c", "read token <&3", "--bundle="+bundle)
	}
	process.Args[0] = "runsc-sandbox"
	process.ExtraFiles = []*os.File{r}
	if err := process.Start(); err != nil {
		r.Close()
		w.Close()
		t.Fatal(err)
	}
	r.Close()
	t.Cleanup(func() {
		_ = w.Close()
		_ = process.Process.Kill()
		_ = process.Wait()
	})
	return process, w
}

func TestCreatePinsOnlyMatchingSandbox(t *testing.T) {
	probe := testPidfd(t, os.Getpid())
	_ = unix.Close(probe)
	for _, name := range []string{"Matching", "MatchingSymlink", "MatchingPATH", "WrongBundle", "UnrelatedProcess", "SpoofedExecutable", "FailedIO"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("GVISOR_SHIM_TEST_RUNTIME_HELPER", "1")
			p := newWedgedInit(t)
			runscLongOperationTimeout = 5 * time.Second
			p.Sandbox = true
			p.Bundle = t.TempDir()
			t.Cleanup(p.closeIO)
			t.Cleanup(p.closeSandboxPidfd)
			bundle := p.Bundle
			if name == "WrongBundle" {
				bundle = t.TempDir()
			}
			process, _ := sandboxIdentityProcess(t, bundle, name == "SpoofedExecutable")
			pid := process.Process.Pid
			if name == "UnrelatedProcess" {
				pid = os.Getpid()
			}
			if err := os.WriteFile(filepath.Join(p.Bundle, "init.pid"), []byte(strconv.Itoa(pid)), 0o600); err != nil {
				t.Fatal(err)
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			p.runtime.Command = exe
			if name == "MatchingSymlink" || name == "MatchingPATH" {
				dir := t.TempDir()
				alias := filepath.Join(dir, "test-runsc")
				if err := os.Symlink(exe, alias); err != nil {
					t.Fatal(err)
				}
				p.runtime.Command = alias
				if name == "MatchingPATH" {
					t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
					p.runtime.Command = "test-runsc"
				}
			}
			cfg := &CreateConfig{ID: p.id, Bundle: p.Bundle}
			if name == "FailedIO" {
				cfg.Stdin = filepath.Join(t.TempDir(), "missing-fifo")
			}
			err = p.Create(context.Background(), cfg)
			if name == "FailedIO" {
				if err == nil {
					t.Fatal("Create succeeded with missing stdin FIFO")
				}
			} else if err != nil {
				t.Fatalf("Create: %v", err)
			}
			want := name == "Matching" || name == "MatchingSymlink" || name == "MatchingPATH"
			if got := p.sandboxPidfd >= 0; got != want {
				t.Fatalf("Create retained pidfd = %v, want %v", got, want)
			}
		})
	}
}

func TestPinnedSandboxRemainsOwnedAcrossExec(t *testing.T) {
	t.Setenv("GVISOR_SHIM_TEST_RUNTIME_HELPER", "1")
	p := newWedgedInit(t)
	runscLongOperationTimeout = 5 * time.Second
	wedgedCommand := p.runtime.Command
	p.Sandbox = true
	p.Bundle = t.TempDir()
	p.Platform = backportAuditPlatform{}
	t.Cleanup(p.closeIO)
	t.Cleanup(p.closeSandboxPidfd)
	sandbox, gate := sandboxIdentityProcess(t, p.Bundle, false)
	if err := os.WriteFile(filepath.Join(p.Bundle, "init.pid"), []byte(strconv.Itoa(sandbox.Process.Pid)), 0o600); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p.runtime.Command = exe
	if err := p.Create(context.Background(), &CreateConfig{ID: p.id, Bundle: p.Bundle}); err != nil {
		t.Fatal(err)
	}
	if p.sandboxPidfd < 0 {
		t.Fatal("Create did not retain the matching sandbox pidfd")
	}
	neighbor := exec.Command("sleep", "60")
	if err := neighbor.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = neighbor.Process.Kill(); _ = neighbor.Wait() })
	sleepPath, err := exec.LookPath("sleep")
	if err != nil {
		t.Fatal(err)
	}
	sleepExe, err := os.Stat(sleepPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Write([]byte{'E'}); err != nil {
		t.Fatal(err)
	}
	changed := false
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		actual, err := os.Stat(fmt.Sprintf("/proc/%d/exe", p.pid))
		if err == nil && os.SameFile(sleepExe, actual) {
			changed = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !changed {
		t.Fatal("sandbox did not exec sleep with its original PID")
	}
	poll := []unix.PollFd{{Fd: int32(p.sandboxPidfd), Events: unix.POLLIN}}
	if n, err := unix.Poll(poll, 0); err != nil || n != 0 {
		t.Fatalf("pidfd lost the live sandbox across exec: %d, %v", n, err)
	}
	p.SetExited(internalErrorCode)
	p.runtime.Command = wedgedCommand
	if err := p.Delete(context.Background()); err == nil {
		t.Fatal("Delete succeeded against wedged runsc")
	}
	done := make(chan error, 1)
	go func() { done <- sandbox.Wait() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("explicit Delete failed to terminate its pinned sandbox after exec")
	}
	if err := neighbor.Process.Signal(unix.Signal(0)); err != nil {
		t.Fatalf("neighbor was affected by sandbox teardown: %v", err)
	}
}

func TestBackportStoppedMetadataKeepsDeleteRecovery(t *testing.T) {
	sandbox := exec.Command("sleep", "60")
	if err := sandbox.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sandbox.Process.Kill(); _ = sandbox.Wait() })
	p := newWedgedInit(t)
	p.Sandbox = true
	p.Platform = backportAuditPlatform{}
	p.sandboxPidfd = testPidfd(t, sandbox.Process.Pid)
	t.Cleanup(p.closeSandboxPidfd)
	fd := p.sandboxPidfd
	p.SetExited(internalErrorCode)
	if p.sandboxPidfd != fd {
		t.Fatal("stopped task metadata discarded the live sentry identity needed by Delete")
	}
	if err := sandbox.Process.Signal(unix.Signal(0)); err != nil {
		t.Fatalf("sentry unexpectedly died at SetExited: %v", err)
	}
	if err := p.Delete(context.Background()); err == nil {
		t.Fatal("Delete succeeded against wedged runsc")
	}
	if p.sandboxPidfd != -1 {
		t.Fatal("escalation retained the pidfd")
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("pidfd after recovery: %v, want closed", err)
	}
	done := make(chan error, 1)
	go func() { done <- sandbox.Wait() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Delete failed to terminate the live sentry after SetExited")
	}
}

type backportAuditPlatform struct{ stdio.Platform }

func (backportAuditPlatform) ShutdownConsole(context.Context, console.Console) error { return nil }

func TestDeleteBoundsBlockedUnmountAndReusesWorker(t *testing.T) {
	p := newWedgedInit(t)
	p.Rootfs = t.TempDir()
	p.initState = &stoppedState{process: p}
	fake := filepath.Join(t.TempDir(), "successful-runsc")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	p.runtime.Command = fake
	old := unmountRootfs
	release, finished := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var finishOnce sync.Once
	unmountRootfs = func(path string, flags int) error {
		calls.Add(1)
		defer finishOnce.Do(func() { close(finished) })
		if path != p.Rootfs || flags != 0 {
			return fmt.Errorf("unmount(%q, %d), want (%q, 0)", path, flags, p.Rootfs)
		}
		<-release
		return nil
	}
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(func() { unblock(); <-finished; unmountRootfs = old })
	for range 2 {
		start := time.Now()
		if err := p.Delete(context.Background()); !errors.Is(err, errRunscTimeout) {
			t.Fatalf("Delete error = %v, want bounded unmount failure", err)
		}
		requireBounded(t, start)
	}
	if calls.Load() != 1 {
		t.Fatalf("Delete retries started %d blocked workers, want one", calls.Load())
	}
	unblock()
	<-finished
	if err := p.Delete(context.Background()); err != nil {
		t.Fatalf("Delete retry after unmount recovery: %v", err)
	}
}

func TestDeleteTimeoutAfterSuccessfulKill(t *testing.T) {
	for _, sandboxTask := range []bool{true, false} {
		t.Run(fmt.Sprintf("Sandbox=%t", sandboxTask), func(t *testing.T) {
			p := newWedgedInit(t)
			p.initState = &stoppedState{process: p}
			p.Sandbox = sandboxTask
			sandbox := exec.Command("sleep", "60")
			if err := sandbox.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sandbox.Process.Kill(); _ = sandbox.Wait() })
			p.sandboxPidfd = testPidfd(t, sandbox.Process.Pid)
			t.Cleanup(p.closeSandboxPidfd)
			neighbor := exec.Command("sleep", "60")
			if err := neighbor.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = neighbor.Process.Kill(); _ = neighbor.Wait() })
			dir := t.TempDir()
			fake := filepath.Join(dir, "delete-wedged-runsc")
			marker := filepath.Join(dir, "delete-reached")
			script := "#!/bin/sh\nfor arg do\nif [ \"$arg\" = delete ]; then\necho reached > " + strconv.Quote(marker) + "\nexec >/dev/null 2>&1\nexec sleep 60\nfi\ndone\nexit 0\n"
			if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			p.runtime.Command = fake
			start := time.Now()
			if err := p.Delete(context.Background()); err == nil {
				t.Fatal("Delete succeeded against wedged delete command")
			}
			requireBounded(t, start)
			if _, err := os.Stat(marker); err != nil {
				t.Fatalf("successful kill never reached Delete: %v", err)
			}
			if sandboxTask {
				done := make(chan error, 1)
				go func() { done <- sandbox.Wait() }()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Fatal("Delete timeout did not terminate its sandbox")
				}
			} else if err := sandbox.Process.Signal(unix.Signal(0)); err != nil {
				t.Fatalf("workload Delete killed sandbox: %v", err)
			}
			if err := neighbor.Process.Signal(unix.Signal(0)); err != nil {
				t.Fatalf("neighbor was affected: %v", err)
			}
		})
	}
}
