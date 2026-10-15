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

package routingalgorithms

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/types"
)

func TestNamespaceRouterInitializationCancelsHeldManagerLock(t *testing.T) {
	rm := NewRouterManager()
	rm.routerMu.Lock()
	defer rm.routerMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- rm.InitContext(ctx) }()
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("router manager ignored cancellation")
	}
}

func TestNamespaceFallbackInitializationCancellation(t *testing.T) {
	rm := NewRouterManager()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, rm.SetFallbackContext(ctx, &FallbackRouter{}, RouterLeastRequest), context.Canceled)
}

func TestNamespaceQueueOwnsLifetime(t *testing.T) {
	startup, stopStartup := context.WithCancel(context.Background())
	lifetime, stopLifetime := context.WithCancel(context.Background())
	c := cache.NewForTest()
	router, err := NewSLORouterWithContext(startup, lifetime, c, "model")
	require.NoError(t, err)
	stopStartup()
	queue := router.(*queueRouter)
	select {
	case <-queue.done:
		t.Fatal("startup completion stopped the live queue")
	default:
	}
	stopLifetime()
	finished := make(chan struct{})
	go func() { queue.Close(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("queue cleanup did not await its goroutine")
	}
}

func TestManagedConstructorRunsOutsideManagerLock(t *testing.T) {
	rm := NewRouterManager()
	rm.RegisterContext(RouterRandom, func(ctx, _ context.Context) (types.Router, error) {
		require.True(t, rm.routerMu.TryLock())
		rm.routerMu.Unlock()
		return &randomRouter{}, ctx.Err()
	})
	require.NoError(t, rm.InitWithLifetimeContext(context.Background(), context.Background()))
	rm.Close()
}

type blockedContextFallback struct {
	FallbackRouter
	manager *RouterManager
	entered chan struct{}
}

func (r *blockedContextFallback) SetFallbackContext(ctx context.Context, _ types.RoutingAlgorithm, _ types.RouterProviderFunc) error {
	if !r.manager.routerMu.TryLock() {
		return context.DeadlineExceeded
	}
	r.manager.routerMu.Unlock()
	close(r.entered)
	<-ctx.Done()
	return ctx.Err()
}

func TestManagedFallbackCancelsOutsideManagerLock(t *testing.T) {
	rm := NewRouterManager()
	rm.RegisterContext(RouterRandom, func(context.Context, context.Context) (types.Router, error) { return &randomRouter{}, nil })
	require.NoError(t, rm.InitWithLifetimeContext(context.Background(), context.Background()))
	defer rm.Close()
	r := &blockedContextFallback{manager: rm, entered: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- rm.SetFallbackContext(ctx, r, RouterRandom) }()
	select {
	case <-r.entered:
	case <-time.After(time.Second):
		t.Fatal("fallback ran under the manager lock or did not start")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("fallback ignored cancellation")
	}
}

type blockedSessionRedisHook struct {
	entered chan struct{}
	exited  chan struct{}
}

func (h *blockedSessionRedisHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}
func (h *blockedSessionRedisHook) ProcessHook(_ redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, _ redis.Cmder) error {
		close(h.entered)
		<-ctx.Done()
		close(h.exited)
		return ctx.Err()
	}
}
func (h *blockedSessionRedisHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestManagedSessionCleanupCancelsAndAwaitsRequestWorker(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "unused:6379"})
	defer func() { _ = client.Close() }()
	hook := &blockedSessionRedisHook{make(chan struct{}), make(chan struct{})}
	client.AddHook(hook)
	r := &sessionAffinityRouter{}
	r.StartWithContext(context.Background(), client)
	defer func() { _ = r.Cleanup() }()
	r.startTask(func() { r.persistSessionKeyToRedis("key", "pod", writeClaim, "") })
	select {
	case <-hook.entered:
	case <-time.After(time.Second):
		t.Fatal("session write did not enter Redis")
	}
	done := make(chan struct{})
	go func() { _ = r.Cleanup(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("session cleanup did not cancel its active write")
	}
	select {
	case <-hook.exited:
	default:
		t.Fatal("session cleanup abandoned its write")
	}
}
