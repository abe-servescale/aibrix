//go:build zmq

// Copyright 2026 AIBrix Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vllm-project/aibrix/pkg/cache/discovery"
	"github.com/vllm-project/aibrix/pkg/constants"
	"github.com/vllm-project/aibrix/pkg/kvevent"
	"github.com/vllm-project/aibrix/pkg/utils"
	"k8s.io/client-go/rest"
)

type blockedKVProvider struct {
	check   func(context.Context)
	entered chan struct{}
}

func (p *blockedKVProvider) GetSyncIndexer(ctx context.Context) (kvevent.SyncIndexer, error) {
	p.check(ctx)
	close(p.entered)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestEnabledKVCompletionReleasesMembershipAndHealthBeforeIO(t *testing.T) {
	t.Setenv(constants.EnvPrefixCacheKVEventSyncEnabled, "true")
	t.Setenv(constants.EnvPrefixCacheUseRemoteTokenizer, "true")
	st := NewForTest()
	st.ownedTasks = utils.NewOwnedTasks(context.Background())
	st.lifetimeCtx = st.ownedTasks.Context()
	defer st.Close()
	p, err := discovery.NewKubernetesProviderForNamespace(&rest.Config{Host: "http://127.0.0.1:1"}, "models")
	require.NoError(t, err)
	p.WithModelListHealth(time.Minute).WithEventCompletion(st.notifyKVEvent)
	st.modelListHealth = p.ModelListHealth()
	provider := &blockedKVProvider{entered: make(chan struct{}), check: func(ctx context.Context) {
		require.True(t, st.mu.TryLock(), "KV I/O retained membership lock")
		st.mu.Unlock()
		probe, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
		defer cancel()
		_, healthy, err := st.modelListHealth.ModelsIfHealthyContext(probe, func(context.Context) ([]string, error) { return nil, nil })
		require.NoError(t, err, "KV I/O retained health lock")
		require.False(t, healthy, "in-flight completion must block health publication")
	}}
	manager := &KVEventManager{Manager: kvevent.NewManagerWithContext(st.lifetimeCtx, nil, provider)}
	st.kvEventManager = manager
	st.publishedKV.Store(manager)
	result := make(chan error, 1)
	st.startOwnedTask(func() {
		_, err := p.DispatchNamespaced(st.lifetimeCtx, discovery.WatchEvent{Type: discovery.EventDelete, Object: lifecyclePod()}, true, func(ctx context.Context, ev discovery.WatchEvent) (bool, error) {
			return handleDiscoveryObjectContext(ctx, st, ev)
		})
		result <- err
	})
	select {
	case <-provider.entered:
	case <-time.After(time.Second):
		t.Fatal("enabled KV completion did not enter I/O")
	}
	done := make(chan struct{})
	go func() { st.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Store.Close did not cancel and await the blocked KV callback")
	}
	require.ErrorIs(t, <-result, context.Canceled)
}
