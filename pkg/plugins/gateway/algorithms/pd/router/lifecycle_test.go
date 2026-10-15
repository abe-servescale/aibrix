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

package router

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vllm-project/aibrix/pkg/cache"
)

func TestManagedPDCleanupAwaitsWorkersAndQueuedPrefixUpdate(t *testing.T) {
	created, err := NewPDRouterWithContext(context.Background(), context.Background(), cache.NewForTest(), nil)
	require.NoError(t, err)
	router := created.(*pdRouter)
	entered := make(chan struct{})
	require.True(t, router.tasks.Start(func() { close(entered); <-router.tasks.Context().Done() }))
	<-entered
	router.enqueuePrefixUpdate([]uint64{1}, "model", "models/pod")
	done := make(chan struct{})
	go func() { _ = router.Cleanup(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("manager did not await PD work")
	}
	require.ErrorIs(t, router.tasks.Context().Err(), context.Canceled)
	require.False(t, router.tasks.Start(func() { t.Error("PD cleanup admitted another task") }))
}
