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

package discovery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
	v1alpha1 "github.com/vllm-project/aibrix/pkg/client/clientset/versioned"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// ValidateNamespace validates the raw configuration without I/O or
// normalization. An empty string must never select Kubernetes NamespaceAll.
func ValidateNamespace(namespace string) error {
	if namespace == "" {
		return fmt.Errorf("--installation-namespace is required in Kubernetes gateway mode")
	}
	if problems := validation.ValidateNamespaceName(namespace, false); len(problems) != 0 {
		return fmt.Errorf("invalid installation namespace %q: %s", namespace, strings.Join(problems, "; "))
	}
	return nil
}

// NewKubernetesProviderForNamespace constructs gateway discovery confined to one
// immutable namespace. Invalid namespace or nil config returns an error before
// any API request. Legacy NewKubernetesProvider remains for non-gateway callers.
func NewKubernetesProviderForNamespace(config *rest.Config, namespace string) (*KubernetesProvider, error) {
	if err := ValidateNamespace(namespace); err != nil {
		return nil, err
	}
	if config == nil {
		return nil, fmt.Errorf("kubernetes configuration is required for installation namespace %q", namespace)
	}
	p := NewKubernetesProvider(rest.CopyConfig(config))
	p.namespace = namespace
	return p, nil
}

// Namespace reports the immutable scope; empty means the legacy
// non-gateway provider, which is rejected by gateway initialization.
func (p *KubernetesProvider) Namespace() string { return p.namespace }

type namespaceWatch struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// Shutdown cancels and awaits all informer callbacks owned by confined
// discovery. It is safe to call repeatedly, including after failed startup.
func (p *KubernetesProvider) Shutdown() {
	p.confined.mu.Lock()
	cancel, done := p.confined.cancel, p.confined.done
	p.confined.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

type namespaceSource struct {
	object   runtime.Object
	list     func(context.Context, metav1.ListOptions) (runtime.Object, error)
	watch    func(context.Context, metav1.ListOptions) (watch.Interface, error)
	required bool
}

// WatchNamespaced verifies Namespace existence and actual required LIST/WATCH
// access, synchronizes informer and handler state, and reconciles adapters
// within startupCtx. Requests and callbacks continue under lifetimeCtx after
// success. A failure cancels and awaits discovery before returning a wrapped
// error. The caller must call Shutdown on lifetime termination.
func (p *KubernetesProvider) WatchNamespaced(startupCtx, lifetimeCtx context.Context, handler func(context.Context, WatchEvent) (bool, error)) (err error) {
	if err = ValidateNamespace(p.namespace); err != nil {
		return err
	}
	p.confined.mu.Lock()
	if p.confined.cancel != nil {
		p.confined.mu.Unlock()
		return fmt.Errorf("discovery for namespace %q has already been started", p.namespace)
	}
	runCtx, cancel := context.WithCancel(lifetimeCtx)
	done := make(chan struct{})
	p.confined.cancel, p.confined.done = cancel, done
	p.confined.mu.Unlock()
	var workers sync.WaitGroup
	failures := make(chan error, 1)
	var deliveryMu sync.Mutex
	initializing := true
	stopOnDeadline := context.AfterFunc(startupCtx, cancel)
	defer func() {
		stopOnDeadline()
		if err != nil {
			cancel()
			workers.Wait()
			close(done)
			err = preserveDiscoveryCancellation(err, startupCtx, lifetimeCtx)
			err = fmt.Errorf("initialize model discovery in namespace %q: %w", p.namespace, err)
		} else {
			go func() { workers.Wait(); close(done) }()
		}
	}()
	if err = startupCtx.Err(); err != nil {
		return err
	}
	probeCtx, cancelProbe := context.WithCancel(startupCtx)
	stopProbe := context.AfterFunc(runCtx, cancelProbe)
	defer func() { stopProbe(); cancelProbe() }()
	sources, core, custom, err := p.namespacedSources(probeCtx)
	if err != nil {
		return err
	}
	p.configureModelListHealth(core, custom)
	var requiredSync []cache.InformerSynced
	var adapters cache.SharedIndexInformer
	for _, source := range sources {
		source := source
		deliver := func(ev WatchEvent) {
			if _, applyErr := p.DispatchNamespaced(runCtx, ev, source.required, handler); applyErr != nil {
				klog.ErrorS(applyErr, "Apply namespaced discovery event", "namespace", p.namespace)
				if source.required {
					deliveryMu.Lock()
					if initializing {
						select {
						case failures <- applyErr:
						default:
						}
						cancel()
					}
					deliveryMu.Unlock()
				}
			}
		}
		if source.required {
			if _, err = source.list(probeCtx, metav1.ListOptions{Limit: 1}); err != nil {
				return fmt.Errorf("list %T: %w", source.object, err)
			}
			temporary, watchErr := source.watch(probeCtx, metav1.ListOptions{})
			if watchErr != nil {
				return fmt.Errorf("watch %T: %w", source.object, watchErr)
			}
			temporary.Stop()
		}
		informer := cache.NewSharedIndexInformer(&cache.ListWatch{
			ListFunc:  func(opts metav1.ListOptions) (runtime.Object, error) { return source.list(runCtx, opts) },
			WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) { return source.watch(runCtx, opts) },
		}, source.object, 0, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
		registration, registerErr := informer.AddEventHandler(p.eventHandlers(deliver))
		if registerErr != nil {
			return registerErr
		}
		if source.required {
			requiredSync = append(requiredSync, informer.HasSynced, registration.HasSynced)
			if err = informer.SetWatchErrorHandler(func(_ *cache.Reflector, watchErr error) {
				if p.modelListHealth != nil {
					p.modelListHealth.InvalidateContext(runCtx)
				}
				klog.ErrorS(watchErr, "Namespaced model discovery watch failed", "namespace", p.namespace)
			}); err != nil {
				return err
			}
		}
		if _, ok := source.object.(*modelv1alpha1.ModelAdapter); ok {
			adapters = informer
		}
		workers.Add(1)
		go func() { defer workers.Done(); informer.Run(runCtx.Done()) }()
	}
	syncCtx, cancelSync := context.WithCancel(startupCtx)
	stopOnDiscovery := context.AfterFunc(runCtx, cancelSync)
	defer func() { stopOnDiscovery(); cancelSync() }()
	if !cache.WaitForCacheSync(syncCtx.Done(), requiredSync...) {
		select {
		case failure := <-failures:
			return failure
		default:
		}
		return fmt.Errorf("synchronize discovery handlers: %w", syncCtx.Err())
	}
	if adapters != nil {
		for _, obj := range adapters.GetStore().List() {
			if _, err = p.DispatchNamespaced(probeCtx, WatchEvent{Type: EventAdd, Object: obj}, true, handler); err != nil {
				return err
			}
		}
	}
	deliveryMu.Lock()
	defer deliveryMu.Unlock()
	if err = startupCtx.Err(); err != nil {
		return err
	}
	if err = runCtx.Err(); err != nil {
		select {
		case failure := <-failures:
			return failure
		default:
			return err
		}
	}
	if err = lifetimeCtx.Err(); err != nil {
		return err
	}
	initializing = false
	return nil
}

// HTTP clients can return a signal-specific cause instead of context.Canceled.
// Keep the original stage error and the context result available to callers.
func preserveDiscoveryCancellation(err error, startup, lifetime context.Context) error {
	contextErr := startup.Err()
	if contextErr == nil {
		contextErr = lifetime.Err()
	}
	if contextErr != nil && !errors.Is(err, contextErr) {
		return fmt.Errorf("%w: %w", err, contextErr)
	}
	return err
}

// DispatchNamespaced is the informer/reconciliation application path. It
// unwraps tombstones and rejects foreign old or new objects before invoking
// handler or changing required-source health. Claims must use required=false.
// The returned bool reports application; rejection is false with no error.
func (p *KubernetesProvider) DispatchNamespaced(ctx context.Context, ev WatchEvent, required bool, handler func(context.Context, WatchEvent) (bool, error)) (bool, error) {
	if tombstone, ok := ev.Object.(cache.DeletedFinalStateUnknown); ok {
		ev.Object = tombstone.Obj
	}
	if tombstone, ok := ev.Object.(*cache.DeletedFinalStateUnknown); ok && tombstone != nil {
		ev.Object = tombstone.Obj
	}
	obj := namespacedMetadata(ev.Object)
	if obj == nil {
		return false, nil
	}
	if obj.GetNamespace() != p.namespace {
		klog.Warningf("Ignored foreign discovery event: %T %s/%s (installation %s)", ev.Object, obj.GetNamespace(), obj.GetName(), p.namespace)
		return false, nil
	}
	if ev.Type == EventUpdate {
		old := namespacedMetadata(ev.OldObject)
		if old == nil || old.GetNamespace() != p.namespace {
			if old != nil {
				klog.Warningf("Ignored foreign discovery update: %T %s/%s (installation %s)", ev.OldObject, old.GetNamespace(), old.GetName(), p.namespace)
			}
			return false, nil
		}
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if p.prepareEvent != nil {
		if err := p.prepareEvent(ctx, ev); err != nil {
			if required && p.modelListHealth != nil {
				p.modelListHealth.InvalidateContext(ctx)
			}
			return false, err
		}
	}
	if required && p.modelListHealth != nil {
		return p.modelListHealth.ApplyContextWithCompletion(ctx, ev, handler, p.completeEvent)
	}
	applied, err := handler(ctx, ev)
	if err == nil && applied && p.completeEvent != nil {
		err = p.completeEvent(ctx, ev)
	}
	if err == nil {
		err = ctx.Err()
	}
	return applied && err == nil, err
}

// WithEventPreparation prepares cancellable dependencies after the namespace
// guard and before the health barrier. It must not mutate cache membership.
// Configure it before WatchNamespaced starts.
func (p *KubernetesProvider) WithEventPreparation(prepare func(context.Context, WatchEvent) error) *KubernetesProvider {
	p.prepareEvent = prepare
	return p
}

func namespacedMetadata(obj interface{}) metav1.Object {
	switch typed := obj.(type) {
	case *v1.Pod:
		if typed != nil {
			return typed
		}
	case *modelv1alpha1.ModelAdapter:
		if typed != nil {
			return typed
		}
	case *modelv1alpha1.ModelClaim:
		if typed != nil {
			return typed
		}
	}
	return nil
}

func (p *KubernetesProvider) eventHandlers(delivery func(WatchEvent)) cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { delivery(WatchEvent{Type: EventAdd, Object: obj}) },
		UpdateFunc: func(old, obj interface{}) { delivery(WatchEvent{Type: EventUpdate, Object: obj, OldObject: old}) },
		DeleteFunc: func(obj interface{}) { delivery(WatchEvent{Type: EventDelete, Object: obj}) },
	}
}

func (p *KubernetesProvider) namespacedSources(startupCtx context.Context) ([]namespaceSource, kubernetes.Interface, v1alpha1.Interface, error) {
	core, err := kubernetes.NewForConfig(p.config)
	if err != nil {
		return nil, nil, nil, err
	}
	namespace, err := core.CoreV1().Namespaces().Get(startupCtx, p.namespace, metav1.GetOptions{})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get Namespace: %w", err)
	}
	if namespace.DeletionTimestamp != nil || namespace.Status.Phase == v1.NamespaceTerminating {
		return nil, nil, nil, fmt.Errorf("namespace is terminating")
	}
	pods := core.CoreV1().Pods(p.namespace)
	sources := []namespaceSource{{object: &v1.Pod{}, required: true,
		list: func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
			return pods.List(ctx, opts)
		}, watch: pods.Watch}}
	var custom v1alpha1.Interface
	if p.watchModelAdapters || p.watchModelClaims {
		custom, err = v1alpha1.NewForConfig(p.config)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	if p.watchModelAdapters {
		adapters := custom.ModelV1alpha1().ModelAdapters(p.namespace)
		sources = append(sources, namespaceSource{object: &modelv1alpha1.ModelAdapter{}, required: true,
			list: func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
				return adapters.List(ctx, opts)
			}, watch: adapters.Watch})
	}
	if p.watchModelClaims {
		claims := custom.ModelV1alpha1().ModelClaims(p.namespace)
		_, probeErr := claims.List(startupCtx, metav1.ListOptions{Limit: 1})
		if apierrors.IsForbidden(probeErr) || apierrors.IsNotFound(probeErr) {
			klog.InfoS("Not watching optional ModelClaims", "namespace", p.namespace, "err", probeErr)
		} else {
			if err = startupCtx.Err(); err != nil {
				return nil, nil, nil, err
			}
			sources = append(sources, namespaceSource{object: &modelv1alpha1.ModelClaim{},
				list: func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
					return claims.List(ctx, opts)
				}, watch: claims.Watch})
		}
	}

	return sources, core, custom, nil
}

// WithEventCompletion configures cancellable notifications outside membership
// and health locks. Required-source health records advance only after success.
func (p *KubernetesProvider) WithEventCompletion(complete func(context.Context, WatchEvent) error) *KubernetesProvider {
	p.completeEvent = complete
	return p
}

func (p *KubernetesProvider) acceptEvent(ev WatchEvent) bool {
	if p.namespace == "" {
		return true
	}
	obj := eventMetadata(ev.Object)
	if obj == nil || obj.GetNamespace() != p.namespace {
		if obj != nil {
			klog.Warningf("Ignored discovery object outside installation namespace: %s/%s", obj.GetNamespace(), obj.GetName())
		}
		return false
	}
	if ev.Type == EventUpdate {
		old := eventMetadata(ev.OldObject)
		if old == nil || old.GetNamespace() != p.namespace {
			if old != nil {
				klog.Warningf("Ignored discovery update from outside installation namespace: %s/%s", old.GetNamespace(), old.GetName())
			}
			return false
		}
	}
	return true
}

func eventMetadata(obj interface{}) metav1.Object {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	if tombstone, ok := obj.(*cache.DeletedFinalStateUnknown); ok && tombstone != nil {
		obj = tombstone.Obj
	}
	switch typed := obj.(type) {
	case *v1.Pod:
		if typed != nil {
			return typed
		}
	case *modelv1alpha1.ModelAdapter:
		if typed != nil {
			return typed
		}
	case *modelv1alpha1.ModelClaim:
		if typed != nil {
			return typed
		}
	}
	return nil
}

// WatchContext is the compatibility form for callers using a stop channel.
// New gateway handlers should use WatchNamespaced and honor their event context.
func (p *KubernetesProvider) WatchContext(startup context.Context, handler func(WatchEvent) bool, stopCh <-chan struct{}) error {
	lifetime, cancel := contextWithStop(context.Background(), stopCh)
	err := p.WatchNamespaced(startup, lifetime, func(ctx context.Context, ev WatchEvent) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return handler(ev), ctx.Err()
	})
	if err != nil {
		cancel()
	} else {
		go func() { <-p.confined.done; cancel() }()
	}
	return err
}

func contextWithStop(parent context.Context, stopCh <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
