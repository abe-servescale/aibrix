/*
Copyright 2026 The Aibrix Team.

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

package utils

import (
	"context"
	"sync"
)

// OwnedTasks admits background work until Close or lifetime cancellation.
// Tasks must honor Context; Close cancels them and awaits their return.
type OwnedTasks struct {
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	closing bool
	wg      sync.WaitGroup
}

// NewOwnedTasks derives a cancellable lifetime without starting a goroutine.
// The caller must call Close to await all work admitted through Start.
func NewOwnedTasks(lifetime context.Context) *OwnedTasks {
	ctx, cancel := context.WithCancel(lifetime)
	return &OwnedTasks{ctx: ctx, cancel: cancel}
}

// Context is canceled when the parent lifetime ends or Close begins.
func (o *OwnedTasks) Context() context.Context { return o.ctx }

// Start launches task if the owner is live and returns whether it was admitted.
// An admitted task must stop on Context cancellation; rejected work is not run.
func (o *OwnedTasks) Start(task func()) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closing || o.ctx.Err() != nil {
		return false
	}
	o.wg.Add(1)
	go func() { defer o.wg.Done(); task() }()
	return true
}

// Close stops admission, cancels Context, and awaits every admitted task.
// It is safe to call concurrently and must not be called by an owned task.
func (o *OwnedTasks) Close() {
	o.mu.Lock()
	o.closing = true
	o.cancel()
	o.mu.Unlock()
	o.wg.Wait()
}

type taskOwnerKey struct{}

// WithTaskOwner carries gateway ownership through request-context copies,
// including context.WithoutCancel. It does not change request cancellation.
func WithTaskOwner(ctx context.Context, owner *OwnedTasks) context.Context {
	return context.WithValue(ctx, taskOwnerKey{}, owner)
}

// ContextWithOwnerLifetime retains ctx's values and cancellation and also
// cancels when its task owner ends. Call cancel to release the lifetime hook.
func ContextWithOwnerLifetime(ctx context.Context) (context.Context, context.CancelFunc) {
	linked, cancel := context.WithCancel(ctx)
	if owner, ok := ctx.Value(taskOwnerKey{}).(*OwnedTasks); ok && owner != nil {
		stop := context.AfterFunc(owner.Context(), cancel)
		if owner.Context().Err() != nil {
			cancel()
		}
		return linked, func() { stop(); cancel() }
	}
	return linked, cancel
}

// GoWithOwner starts work under the owner carried by ctx and links its I/O to
// that lifetime. It preserves ctx cancellation: callers that need to outlive
// client cancellation pass context.WithoutCancel(ctx). Without an owner it
// retains legacy goroutine behavior. False means shutdown rejected the task.
func GoWithOwner(ctx context.Context, task func(context.Context)) bool {
	run := func() { linked, cancel := ContextWithOwnerLifetime(ctx); defer cancel(); task(linked) }
	if owner, ok := ctx.Value(taskOwnerKey{}).(*OwnedTasks); ok && owner != nil {
		return owner.Start(run)
	}
	go run()
	return true
}
