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

package prefixcache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vllm-project/aibrix/pkg/cache"
)

func TestManagedInitializationCancelsActualPrefixMetricsLock(t *testing.T) {
	st := cache.NewForTest()
	prefixCacheMetricsMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := NewPrefixCacheRouterWithOptionsContext(ctx, context.Background(), st, nil)
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		prefixCacheMetricsMu.Unlock()
		t.Fatal("real prefix constructor ignored cancellation")
	}
	prefixCacheMetricsMu.Unlock()
}

func TestPrefixCleanupStopsTokenizerWorker(t *testing.T) {
	t.Setenv("AIBRIX_PREFIX_CACHE_USE_REMOTE_TOKENIZER", "true")
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	router, err := NewPrefixCacheRouterWithOptionsContext(context.Background(), lifetime, cache.NewForTest(), nil)
	require.NoError(t, err)
	pool := router.(prefixCacheRouter).tokenizerPool.(*TokenizerPool)
	require.NoError(t, router.(prefixCacheRouter).Cleanup())
	select {
	case <-pool.checkerDone:
	case <-time.After(time.Second):
		t.Fatal("tokenizer worker was abandoned")
	}
}
