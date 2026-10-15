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

package cache

import (
	"context"
	"fmt"
	"sync/atomic"

	"k8s.io/klog/v2"

	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/cache/discovery"
	"github.com/vllm-project/aibrix/pkg/types"
	"github.com/vllm-project/aibrix/pkg/utils"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
)

var gatewayPublished atomic.Pointer[Store]

// InitGatewayWithOptions initializes confined Kubernetes discovery and returns
// errors after canceling and awaiting owned discovery and router work. Failed
// state is never published as the process singleton. Reuse requires the same
// namespace and a live lifetime context. Call Close during gateway shutdown.
// startupCtx remains the caller's responsibility through readiness publication.
func InitGatewayWithOptions(startupCtx, lifetimeCtx context.Context, config *rest.Config, opts InitOptions) (_ *Store, err error) {
	if err = discovery.ValidateNamespace(opts.InstallationNamespace); err != nil {
		return nil, err
	}
	if !opts.IsGateway {
		return nil, fmt.Errorf("gateway initialization requires IsGateway")
	}
	if opts.EnableKVSync && opts.RedisClient == nil {
		return nil, fmt.Errorf("KV event synchronization requires Redis")
	}
	if opts.ModelListMaxAge < 0 {
		return nil, fmt.Errorf("model-list discovery maximum age must be nonnegative")
	}
	if err = utils.LockContext(startupCtx, gatewayInitMu.TryLock, gatewayInitMu.Unlock); err != nil {
		return nil, err
	}
	defer gatewayInitMu.Unlock()
	selected, err := gatewayDiscoveryProvider(config, opts)
	if err != nil {
		return nil, err
	}
	provider, ok := selected.(*discovery.KubernetesProvider)
	if !ok {
		return nil, fmt.Errorf("gateway requires Kubernetes discovery")
	}
	if opts.ModelRouterProvider != nil {
		return nil, fmt.Errorf("gateway router construction requires ModelRouterProviderWithContext")
	}
	if previous := gatewayPublished.Load(); previous != nil {
		if previous.installationNamespace != opts.InstallationNamespace || previous.lifetimeCtx == nil || previous.lifetimeCtx.Err() != nil {
			return nil, fmt.Errorf("cannot reuse cache with a different or absent installation namespace")
		}
		if err = startupCtx.Err(); err != nil {
			return nil, err
		}
		return previous, lifetimeCtx.Err()
	}
	if store.initialized {
		return nil, fmt.Errorf("cannot reuse an unconfined cache for the Kubernetes gateway")
	}
	runCtx, cancel := context.WithCancel(lifetimeCtx)
	stopOnDeadline := context.AfterFunc(startupCtx, cancel)
	defer stopOnDeadline()
	st := newStore(opts.RedisClient, nil, nil)
	st.installationNamespace = opts.InstallationNamespace
	st.lifetimeCtx = runCtx
	st.cancelLifetime = cancel
	st.contextRouterProvider = opts.ModelRouterProviderWithContext
	st.redisClient = opts.RedisClient
	st.disableModelClaimBindings = opts.DisableModelClaimPodBindings
	st.gatewayCleanup = func() { cancel(); provider.Shutdown() }
	provider.WithEventPreparation(st.prepareEventRouters).WithEventCompletion(st.notifyKVEvent)
	defer func() {
		if err != nil {
			st.Close()
		}
	}()
	if opts.ModelListMaxAge > 0 {
		provider.WithModelListHealth(opts.ModelListMaxAge)
	}
	if err = provider.WatchNamespaced(startupCtx, runCtx, func(ctx context.Context, ev discovery.WatchEvent) (bool, error) {
		return handleDiscoveryObjectContext(ctx, st, ev)
	}); err != nil {
		return nil, err
	}
	st.modelListHealth = provider.ModelListHealth()
	st.prometheusApi, err = initPrometheusAPIWithContext(startupCtx, config)
	if err != nil {
		return nil, fmt.Errorf("initialize gateway metrics: %w", err)
	}
	if err = startupCtx.Err(); err != nil {
		return nil, err
	}
	if err = runCtx.Err(); err != nil {
		return nil, err
	}
	gatewayPublished.Store(st)
	return st, nil
}

type preparedModelRouter struct{ router types.QueueRouter }

func (s *Store) prepareEventRouters(ctx context.Context, ev discovery.WatchEvent) error {
	if s.contextRouterProvider == nil || ev.Type == discovery.EventDelete {
		return ctx.Err()
	}
	var models []string
	switch obj := ev.Object.(type) {
	case *v1.Pod:
		if isWorkerPod(obj) {
			return nil
		}
		if name, ok := getModelNameFromPod(obj); ok {
			models = append(models, name)
		}
		for name, binding := range s.modelClaimBindingsFromPod(obj) {
			if binding.Port > 0 {
				models = append(models, name)
			}
		}
	case *modelv1alpha1.ModelAdapter:
		if len(obj.Status.Instances) > 0 {
			models = append(models, obj.Name)
		}
	}
	for _, name := range models {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, exists := s.preparedRouters.Load(name); exists {
			continue
		}
		if err := s.prepareModelRouter(ctx, name); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (s *Store) prepareModelRouter(ctx context.Context, name string) error {
	if err := utils.LockContext(ctx, s.prepareRouterMu.TryLock, s.prepareRouterMu.Unlock); err != nil {
		return err
	}
	defer s.prepareRouterMu.Unlock()
	if _, exists := s.preparedRouters.Load(name); exists {
		return ctx.Err()
	}
	router, err := s.contextRouterProvider(ctx, s.lifetimeCtx, s, name)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		if closer, ok := router.(interface{ Close() }); ok {
			closer.Close()
		}
		return err
	}
	s.preparedRouters.Store(name, &preparedModelRouter{router: router})
	return nil
}

func handleDiscoveryObjectContext(ctx context.Context, st *Store, ev discovery.WatchEvent) (bool, error) {
	var err error
	switch obj := ev.Object.(type) {
	case *v1.Pod:
		switch ev.Type {
		case discovery.EventAdd:
			err = st.addPodContext(ctx, obj)
		case discovery.EventDelete:
			err = st.deletePodContext(ctx, obj)
		case discovery.EventUpdate:
			old, ok := ev.OldObject.(*v1.Pod)
			if !ok || old == nil {
				return false, fmt.Errorf("invalid previous Pod in discovery update")
			}
			err = st.updatePodContext(ctx, old, obj)
		}
	case *modelv1alpha1.ModelAdapter:
		switch ev.Type {
		case discovery.EventAdd:
			err = st.addModelAdapterContext(ctx, obj)
		case discovery.EventDelete:
			err = st.deleteModelAdapterContext(ctx, obj)
		case discovery.EventUpdate:
			old, ok := ev.OldObject.(*modelv1alpha1.ModelAdapter)
			if !ok || old == nil {
				return false, fmt.Errorf("invalid previous ModelAdapter in discovery update")
			}
			err = st.updateModelAdapterContext(ctx, old, obj)
		}
	case *modelv1alpha1.ModelClaim:
		if ev.Type == discovery.EventDelete {
			err = st.deleteModelClaimContext(ctx, obj)
		} else {
			err = st.setModelClaimContext(ctx, obj)
		}
	default:
		return false, fmt.Errorf("unexpected discovery object %T", ev.Object)
	}
	return err == nil, err
}

// StartGatewayBackground starts existing metric/profile/routing synchronization
// after guarded readiness publication. Discovery has already established the
// model set; these ongoing tasks belong to the gateway lifetime.
func (s *Store) StartGatewayBackground(opts InitOptions) error {
	if s.lifetimeCtx == nil {
		return nil
	}
	if err := s.lifetimeCtx.Err(); err != nil {
		return err
	}
	if opts.EnableKVSync && opts.RedisClient == nil {
		return fmt.Errorf("KV event synchronization requires Redis")
	}
	s.backgroundOnce.Do(func() {
		for worker := 0; worker < s.podMetricsWorkerCount; worker++ {
			s.startOwnedTask(func() { s.workerContext(s.lifetimeCtx, s.podMetricsJobs) })
		}
		initMetricsCache(s, s.lifetimeCtx.Done())
		if s.enableProfileCaching {
			initProfileCache(s, s.lifetimeCtx.Done(), false)
		}
		if s.enableTracing && s.redisClient != nil {
			initTraceCache(s, s.lifetimeCtx.Done())
		}
		if s.redisClient != nil {
			initGatewaySnapshotSync(s, s.lifetimeCtx.Done())
			initRunningRequestsLiveness(s, s.lifetimeCtx.Done())
		}
		if opts.EnableKVSync {
			if err := s.initKVEventSync(); err != nil {
				klog.ErrorS(err, "Initialize KV event synchronization")
			}
		}
	})
	return s.lifetimeCtx.Err()
}

func (s *Store) startOwnedTask(task func()) bool {
	s.ownedMu.Lock()
	defer s.ownedMu.Unlock()
	if s.closing {
		return false
	}
	if s.lifetimeCtx == nil {
		go task()
		return true
	}
	s.ownedTasks.Add(1)
	go func() { defer s.ownedTasks.Done(); task() }()
	return true
}

func (s *Store) runtimeContext() context.Context {
	if s != nil && s.lifetimeCtx != nil {
		return s.lifetimeCtx
	}
	return context.Background()
}

func (s *Store) notifyKVEvent(ctx context.Context, ev discovery.WatchEvent) error {
	manager := s.kvManager()
	if manager == nil {
		return ctx.Err()
	}
	pod, ok := ev.Object.(*v1.Pod)
	if !ok {
		return ctx.Err()
	}
	switch ev.Type {
	case discovery.EventAdd:
		return manager.OnPodAddContext(ctx, pod)
	case discovery.EventUpdate:
		old, ok := ev.OldObject.(*v1.Pod)
		if !ok {
			return fmt.Errorf("invalid previous Pod in KV notification")
		}
		return manager.OnPodUpdateContext(ctx, old, pod)
	case discovery.EventDelete:
		return manager.OnPodDeleteContext(ctx, pod)
	}
	return ctx.Err()
}

func (s *Store) kvManager() *KVEventManager {
	if s.lifetimeCtx != nil {
		return s.publishedKV.Load()
	}
	return s.kvEventManager
}
