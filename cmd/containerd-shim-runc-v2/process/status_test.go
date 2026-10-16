//go:build !windows

/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package process

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/containerd/console"

	google_protobuf "github.com/containerd/containerd/v2/pkg/protobuf/types"
)

// accessorsReturnPromptly asserts that the read-only status accessors of a
// process answer while the process's mu is held by a Delete in flight: draining
// stdio, exec'ing runc delete, closing the IO and unmounting the rootfs can
// each take seconds, and the task State RPC has a two second deadline.
// See https://github.com/containerd/containerd/issues/14379.
func accessorsReturnPromptly(t *testing.T, status func(context.Context) (string, error), exitStatus func() int, exitedAt func() time.Time) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := status(context.Background()); err != nil {
			t.Errorf("Status returned error: %v", err)
		}
		_ = exitStatus()
		_ = exitedAt()
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("status accessors blocked while the process mutex was held")
	}
}

// statusDoesNotReturnEarly asserts that Status does not answer while the
// process's mu is held by a non-Delete lifecycle operation.
func statusDoesNotReturnEarly(t *testing.T, status func(context.Context) (string, error)) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = status(context.Background())
	}()

	select {
	case <-done:
		t.Fatal("Status returned while a lifecycle operation held mu")
	case <-time.After(100 * time.Millisecond):
	}

	// The caller releases mu; the pending Status must then complete.
	t.Cleanup(func() { <-done })
}

// gatedDeleteState is an initState test double whose Delete reports entry and
// blocks until released, so a test can hold a Delete inside its cleanup phase
// without a runtime. A non-nil err makes the cleanup fail, which is how an
// overlapping-Delete test models a first Delete that errors out.
type gatedDeleteState struct {
	entered chan struct{}
	release chan struct{}
	err     error
}

func newGatedDeleteState() *gatedDeleteState {
	return &gatedDeleteState{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *gatedDeleteState) Start(context.Context) error { return errors.New("not implemented") }

func (s *gatedDeleteState) Delete(context.Context) error {
	close(s.entered)
	<-s.release
	return s.err
}

func (s *gatedDeleteState) Pause(context.Context) error { return errors.New("not implemented") }

func (s *gatedDeleteState) Resume(context.Context) error { return errors.New("not implemented") }

func (s *gatedDeleteState) Update(context.Context, *google_protobuf.Any) error {
	return errors.New("not implemented")
}

func (s *gatedDeleteState) Checkpoint(context.Context, *CheckpointConfig) error {
	return errors.New("not implemented")
}

func (s *gatedDeleteState) Exec(context.Context, string, *ExecConfig) (Process, error) {
	return nil, errors.New("not implemented")
}

func (s *gatedDeleteState) Kill(context.Context, uint32, bool) error {
	return errors.New("not implemented")
}

func (s *gatedDeleteState) SetExited(int) {}

func (s *gatedDeleteState) Status(context.Context) (string, error) { return "created", nil }

// gatedExecDeleteState is the execState counterpart of gatedDeleteState.
type gatedExecDeleteState struct {
	entered chan struct{}
	release chan struct{}
	err     error
}

func newGatedExecDeleteState() *gatedExecDeleteState {
	return &gatedExecDeleteState{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *gatedExecDeleteState) Resize(console.WinSize) error {
	return errors.New("not implemented")
}

func (s *gatedExecDeleteState) Start(context.Context) error { return errors.New("not implemented") }

func (s *gatedExecDeleteState) Delete(context.Context) error {
	close(s.entered)
	<-s.release
	return s.err
}

func (s *gatedExecDeleteState) Kill(context.Context, uint32, bool) error {
	return errors.New("not implemented")
}

func (s *gatedExecDeleteState) SetExited(int) {}

func (s *gatedExecDeleteState) Status(context.Context) (string, error) { return "created", nil }

// TestInitStatusAccessorsRemainResponsiveDuringDelete drives a real Delete
// into its cleanup phase — mu held across the drain, the runc delete exec and
// the rootfs unmount — and asserts the read-only accessors answer from the
// observable state instead of queueing behind the cleanup.
func TestInitStatusAccessorsRemainResponsiveDuringDelete(t *testing.T) {
	p := &Init{}
	gate := newGatedDeleteState()
	p.setInitState(gate)

	deleteDone := make(chan error, 1)
	go func() { deleteDone <- p.Delete(context.Background()) }()

	<-gate.entered // the Delete owns mu and is cleaning up
	accessorsReturnPromptly(t, p.Status, p.ExitStatus, p.ExitedAt)

	close(gate.release)
	if err := <-deleteDone; err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
}

// TestExecStatusAccessorsRemainResponsiveDuringDelete is the execProcess
// counterpart of TestInitStatusAccessorsRemainResponsiveDuringDelete.
func TestExecStatusAccessorsRemainResponsiveDuringDelete(t *testing.T) {
	parent := &Init{}
	parent.setInitState(&createdState{p: parent})

	e := &execProcess{parent: parent}
	gate := newGatedExecDeleteState()
	e.setExecState(gate)

	deleteDone := make(chan error, 1)
	go func() { deleteDone <- e.Delete(context.Background()) }()

	<-gate.entered // the Delete owns mu and is cleaning up
	accessorsReturnPromptly(t, e.Status, e.ExitStatus, e.ExitedAt)

	close(gate.release)
	if err := <-deleteDone; err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
}

// TestInitStatusBypassesDeleteStartingAfterTheCheck pins the admission
// window: whether Status observes the absence of an active Delete before or
// after a Delete acquires mu, it must not end up queued behind the cleanup.
// Both interleavings are exercised by starting the reader and the Delete
// concurrently.
func TestInitStatusBypassesDeleteStartingAfterTheCheck(t *testing.T) {
	p := &Init{}
	gate := newGatedDeleteState()
	p.setInitState(gate)

	statusDone := make(chan struct{})
	go func() {
		defer close(statusDone)
		if _, err := p.Status(context.Background()); err != nil {
			t.Errorf("Status returned error: %v", err)
		}
	}()
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- p.Delete(context.Background()) }()

	select {
	case <-statusDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Status queued behind a Delete that acquired mu after the reader's check")
	}

	close(gate.release)
	if err := <-deleteDone; err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
}

// TestExecStatusBypassesDeleteStartingAfterTheCheck is the execProcess
// counterpart of TestInitStatusBypassesDeleteStartingAfterTheCheck.
func TestExecStatusBypassesDeleteStartingAfterTheCheck(t *testing.T) {
	parent := &Init{}
	parent.setInitState(&createdState{p: parent})

	e := &execProcess{parent: parent}
	gate := newGatedExecDeleteState()
	e.setExecState(gate)

	statusDone := make(chan struct{})
	go func() {
		defer close(statusDone)
		if _, err := e.Status(context.Background()); err != nil {
			t.Errorf("Status returned error: %v", err)
		}
	}()
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- e.Delete(context.Background()) }()

	select {
	case <-statusDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Status queued behind a Delete that acquired mu after the reader's check")
	}

	close(gate.release)
	if err := <-deleteDone; err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
}

// TestInitStatusSerializesWithInFlightLifecycleOperations pins the boundary of
// the Delete exemption: Status may only skip mu while a Delete is cleaning up.
// While any other operation holds mu — a Start publishing `running` only after
// the runtime start returns, or a SetExited whose transition follows the wait
// notification — Status must keep waiting, or State could report a stale state
// to a recovery pass or reject a Delete that follows a Wait.
func TestInitStatusSerializesWithInFlightLifecycleOperations(t *testing.T) {
	p := &Init{}
	p.setInitState(&createdState{p: p})

	p.mu.Lock()
	statusDoesNotReturnEarly(t, p.Status)
	p.mu.Unlock()
}

func TestExecStatusSerializesWithInFlightLifecycleOperations(t *testing.T) {
	parent := &Init{}
	parent.setInitState(&createdState{p: parent})

	e := &execProcess{parent: parent}
	e.setExecState(&execCreatedState{p: e})

	e.mu.Lock()
	statusDoesNotReturnEarly(t, e.Status)
	e.mu.Unlock()
}

// settleQueuedDelete gives a Delete goroutine time to reach the point where
// it waits for mu. With the fix the exemption is published inside the mu
// critical section, so a queued Delete never publishes and the assertions
// below hold regardless of scheduling; the settle only makes the buggy
// announce-before-lock ordering fail deterministically instead of as a
// scheduling lottery.
func settleQueuedDelete() {
	time.Sleep(50 * time.Millisecond)
}

// TestInitStatusSerializesWithQueuedDelete pins the exemption to the active
// cleanup owner: a Delete that is merely queued behind another lifecycle
// operation must not let Status skip mu, or State could report `created`
// while a Start is still publishing `running` — the window a daemon recovery
// pass reads as an interrupted start.
func TestInitStatusSerializesWithQueuedDelete(t *testing.T) {
	p := &Init{}
	gate := newGatedDeleteState()
	p.setInitState(gate)

	// A non-Delete lifecycle operation (a Start) holds mu.
	p.mu.Lock()

	// A Delete queues behind it. The queued Delete has not published the
	// exemption, so Status must keep waiting for the Start.
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- p.Delete(context.Background()) }()
	settleQueuedDelete()
	statusDoesNotReturnEarly(t, p.Status)

	// The Start completes and the queued Delete becomes the active owner;
	// only now may the accessors answer from the observable state.
	p.mu.Unlock()
	<-gate.entered
	accessorsReturnPromptly(t, p.Status, p.ExitStatus, p.ExitedAt)

	close(gate.release)
	if err := <-deleteDone; err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
}

// TestExecStatusSerializesWithQueuedDelete is the execProcess counterpart of
// TestInitStatusSerializesWithQueuedDelete.
func TestExecStatusSerializesWithQueuedDelete(t *testing.T) {
	parent := &Init{}
	parent.setInitState(&createdState{p: parent})

	e := &execProcess{parent: parent}
	gate := newGatedExecDeleteState()
	e.setExecState(gate)

	// A non-Delete lifecycle operation (a Start) holds mu.
	e.mu.Lock()

	// A Delete queues behind it. The queued Delete has not published the
	// exemption, so Status must keep waiting for the Start.
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- e.Delete(context.Background()) }()
	settleQueuedDelete()
	statusDoesNotReturnEarly(t, e.Status)

	// The Start completes and the queued Delete becomes the active owner;
	// only now may the accessors answer from the observable state.
	e.mu.Unlock()
	<-gate.entered
	accessorsReturnPromptly(t, e.Status, e.ExitStatus, e.ExitedAt)

	close(gate.release)
	if err := <-deleteDone; err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
}

// TestInitStatusSurvivesOverlappingDeletes scopes the publication to the
// active owner: when a first Delete fails mid-cleanup and a second Delete
// takes over, the exemption must cover the second cleanup as well. A shared
// boolean cleared by the first Delete's return path would leave readers
// queued behind the second cleanup.
func TestInitStatusSurvivesOverlappingDeletes(t *testing.T) {
	p := &Init{}

	first := newGatedDeleteState()
	first.err = errors.New("first delete failed")
	p.setInitState(first)

	firstDone := make(chan error, 1)
	go func() { firstDone <- p.Delete(context.Background()) }()
	<-first.entered // the first Delete owns mu and is cleaning up

	// The second Delete queues behind the first.
	second := newGatedDeleteState()
	p.setInitState(second)
	secondDone := make(chan error, 1)
	go func() { secondDone <- p.Delete(context.Background()) }()
	settleQueuedDelete()

	// Readers are exempt while the first cleanup runs.
	accessorsReturnPromptly(t, p.Status, p.ExitStatus, p.ExitedAt)

	// The first Delete fails. Its publication must be cleared with mu still
	// held, so it cannot clobber the second Delete's.
	close(first.release)
	if err := <-firstDone; err == nil {
		t.Fatal("expected the first Delete to fail")
	}
	<-second.entered // the second Delete is now the active owner

	// Let the first Delete's return path run to completion. With the fix the
	// publication was cleared while mu was still held, so the second cleanup
	// stays covered; with a shared boolean cleared after unlocking, this is
	// where the exemption would be lost and readers would queue behind the
	// second cleanup.
	time.Sleep(50 * time.Millisecond)

	// Readers must still be exempt during the second cleanup.
	accessorsReturnPromptly(t, p.Status, p.ExitStatus, p.ExitedAt)

	close(second.release)
	if err := <-secondDone; err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
}

// TestExecStatusSurvivesOverlappingDeletes is the execProcess counterpart of
// TestInitStatusSurvivesOverlappingDeletes.
func TestExecStatusSurvivesOverlappingDeletes(t *testing.T) {
	parent := &Init{}
	parent.setInitState(&createdState{p: parent})

	e := &execProcess{parent: parent}

	first := newGatedExecDeleteState()
	first.err = errors.New("first delete failed")
	e.setExecState(first)

	firstDone := make(chan error, 1)
	go func() { firstDone <- e.Delete(context.Background()) }()
	<-first.entered // the first Delete owns mu and is cleaning up

	// The second Delete queues behind the first.
	second := newGatedExecDeleteState()
	e.setExecState(second)
	secondDone := make(chan error, 1)
	go func() { secondDone <- e.Delete(context.Background()) }()
	settleQueuedDelete()

	// Readers are exempt while the first cleanup runs.
	accessorsReturnPromptly(t, e.Status, e.ExitStatus, e.ExitedAt)

	// The first Delete fails. Its publication must be cleared with mu still
	// held, so it cannot clobber the second Delete's.
	close(first.release)
	if err := <-firstDone; err == nil {
		t.Fatal("expected the first Delete to fail")
	}
	<-second.entered // the second Delete is now the active owner

	// Let the first Delete's return path run to completion. With the fix the
	// publication was cleared while mu was still held, so the second cleanup
	// stays covered; with a shared boolean cleared after unlocking, this is
	// where the exemption would be lost and readers would queue behind the
	// second cleanup.
	time.Sleep(50 * time.Millisecond)

	// Readers must still be exempt during the second cleanup.
	accessorsReturnPromptly(t, e.Status, e.ExitStatus, e.ExitedAt)

	close(second.release)
	if err := <-secondDone; err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
}
