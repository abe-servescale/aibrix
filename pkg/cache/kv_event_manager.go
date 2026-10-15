//go:build !zmq

/*
Copyright 2025 The Aibrix Team.

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

package cache

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
)

// KVEventManager stub implementation when ZMQ is not available
type KVEventManager struct {
	store   *Store
	enabled bool
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewKVEventManager creates a stub KV event manager
func NewKVEventManager(store *Store) *KVEventManager {
	ctx, cancel := context.WithCancel(store.runtimeContext())
	return &KVEventManager{
		store:   store,
		enabled: false,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// validateConfiguration validates the manager configuration
func (m *KVEventManager) validateConfiguration() error {
	// KV event sync requires ZMQ support which is not available in this build
	return fmt.Errorf("KV event sync requires ZMQ support (build with -tags=zmq)")
}

// Start is a no-op
func (m *KVEventManager) Start() error {
	return nil
}

// Stop cancels the stub lifecycle; there is no worker to await.
func (m *KVEventManager) Stop() {
	m.cancel()
}

// OnPodAdd is a no-op
func (m *KVEventManager) OnPodAdd(pod *v1.Pod) {}

// OnPodUpdate is a no-op
func (m *KVEventManager) OnPodUpdate(oldPod, newPod *v1.Pod) {}

// OnPodDelete is a no-op
func (m *KVEventManager) OnPodDelete(pod *v1.Pod) {}

// CheckSleepStateBackstop is a no-op: the metric-driven prefix-cache backstop
// this delegates to in the zmq build (kvevent.Manager.CheckSleepStateBackstop)
// requires the sync indexer KV event sync provides, which this build does not
// have.
func (m *KVEventManager) CheckSleepStateBackstop(ctx context.Context, podKey, modelName string, loraID int64, awake float64) {
}

// OnPodAddContext returns caller cancellation without creating a subscription
// in this build. It starts no continuing work and needs no event cleanup.
func (m *KVEventManager) OnPodAddContext(ctx context.Context, pod *v1.Pod) error { return ctx.Err() }

// OnPodUpdateContext returns caller cancellation and otherwise leaves this
// unsupported subscription state unchanged, without starting work.
func (m *KVEventManager) OnPodUpdateContext(ctx context.Context, oldPod, newPod *v1.Pod) error {
	return ctx.Err()
}

// OnPodDeleteContext returns caller cancellation. No subscriber or indexed
// state exists to remove in this build.
func (m *KVEventManager) OnPodDeleteContext(ctx context.Context, pod *v1.Pod) error { return ctx.Err() }
