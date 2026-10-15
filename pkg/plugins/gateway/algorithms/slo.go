/*
Copyright 2024 The Aibrix Team.

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

	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms/least"
	"github.com/vllm-project/aibrix/pkg/plugins/gateway/queue"
	"github.com/vllm-project/aibrix/pkg/types"
)

const (
	// Default routing algorithm for slo-family algorithms, setting to RouterSLOLeastLoadPulling.
	RouterSLO types.RoutingAlgorithm = "slo"

	// SLO-aware routing algorithm that using SLOQueue and packLoadRouter as the backend.
	RouterSLOPackLoad types.RoutingAlgorithm = "slo-pack-load"

	// SLO-aware routing algorithm that using SLOQueue and leastLoadRouter (push mode) as the backend.
	RouterSLOLeastLoad types.RoutingAlgorithm = "slo-least-load"

	// SLO-aware routing algorithm that using SLOQueue and leastLoadRouter (pull mode) as the backend.
	RouterSLOLeastLoadPulling types.RoutingAlgorithm = "slo-least-load-pulling"
)

var (
	routerSLOProvider types.RouterProviderFunc = func(ctx *types.RoutingContext) (types.Router, error) {
		c, err := cache.Get()
		if err != nil {
			return nil, err
		}

		return c.GetRouter(ctx)
	}
)

func init() {
	RegisterProvider(RouterSLO, routerSLOProvider)
	RegisterProvider(RouterSLOPackLoad, routerSLOProvider)
	RegisterProvider(RouterSLOLeastLoad, routerSLOProvider)
	RegisterProvider(RouterSLOLeastLoadPulling, routerSLOProvider)
}

// SLORouter is a router that add FallbackRouter mechanism to the queue.
type SLORouter struct {
	FallbackRouter
	*queue.SLOQueue
}

func (r *SLORouter) Route(ctx *types.RoutingContext, pods types.PodList) (string, error) {
	// Ctx is not routed if no profiles is found during Peek.
	if !ctx.HasRouted() && r.LastError() == nil {
		return r.FallbackRouter.Route(ctx, pods)
	}
	return r.SLOQueue.Route(ctx, pods)
}

func NewSLORouter(modelName string) (types.QueueRouter, error) {
	loadProvider, err := cache.NewPendingLoadProvider()
	if err != nil {
		return nil, err
	}

	c, err := cache.Get()
	if err != nil {
		return nil, err
	}
	return newSLORouterWithContext(context.Background(), context.Background(), c, loadProvider, modelName)
}

// NewSLORouterWithContext bounds construction by startup and owns its queue
// under lifetime. All dependencies use st before singleton publication.
func NewSLORouterWithContext(startup, lifetime context.Context, st *cache.Store, modelName string) (types.QueueRouter, error) {
	return newSLORouterWithContext(startup, lifetime, st, cache.NewPendingLoadProviderWithCache(st), modelName)
}

func newSLORouterWithContext(startup, lifetime context.Context, c cache.Cache, loadProvider *cache.PendingLoadProvider, modelName string) (types.QueueRouter, error) {
	if err := startup.Err(); err != nil {
		return nil, err
	}
	// Each SLORouter instance owns a private RouterManager for its internal sub-routing.
	// We intentionally use this local rm — not defaultRM — for SetFallback below.
	//
	// Why: NewSLORouter is called per-model from the Kubernetes informer
	// (cache.addPodAndModelMappingLocked), which can fire before routing.Init() has
	// been called on defaultRM. The global SetFallback blocks on defaultRM.routerInited
	// (5 s timeout) and fails with "router did not initialized" if Init() hasn't run yet.
	// Using the local rm avoids that race; rm.Init() is called synchronously below, so
	// the fallback lookup succeeds immediately.
	//
	// TODO: A cleaner design would either (a) guarantee routing.Init() runs before any
	// model router is created, or (b) allow fallback routers to reference defaultRM's
	// already-registered providers without waiting on Init(). Either change would let
	// this fallback delegate back to defaultRM's shared least-request policy instead of
	// each SLO router instance carrying its own copy.
	rm := NewRouterManager()
	defaultRouter, _ := least.NewLeastLoadPullingRouter(loadProvider)
	defaultProvider := func(_ *types.RoutingContext) (types.Router, error) { return defaultRouter, nil }
	rm.RegisterProvider(RouterSLO, defaultProvider)
	rm.RegisterProvider(RouterSLOLeastLoadPulling, defaultProvider)
	rm.registerLocal(RouterSLOPackLoad, func() (types.Router, error) { return NewPackLoadRouter(loadProvider) })
	rm.registerLocal(RouterSLOLeastLoad, func() (types.Router, error) { return least.NewLeastLoadRouter(loadProvider) })
	// RouterLeastRequest must be registered on the local rm because rm.SetFallback
	// resolves the provider from rm.routerFactory, populated by rm.Init() below.
	rm.registerLocal(RouterLeastRequest, func() (types.Router, error) { return least.NewLeastRequestTopKRouterWithCache(c), nil })
	if err := rm.InitWithLifetimeContext(startup, lifetime); err != nil {

		return nil, err
	}

	sloQueue := queue.NewSLOQueueWithCache(rm.Select, modelName, c)
	router := &SLORouter{SLOQueue: sloQueue}
	if err := rm.SetFallbackContext(startup, router, RouterLeastRequest); err != nil {
		return nil, err
	}
	if err := startup.Err(); err != nil {
		return nil, err
	}
	return NewQueueRouterWithContext(lifetime, router, sloQueue, c)
}
