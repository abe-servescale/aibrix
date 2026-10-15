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

package discovery

import (
	"context"
	"errors"
	"fmt"
	"time"

	crdinformers "github.com/vllm-project/aibrix/pkg/client/informers/externalversions"

	v1alpha1 "github.com/vllm-project/aibrix/pkg/client/clientset/versioned"
	v1alpha1scheme "github.com/vllm-project/aibrix/pkg/client/clientset/versioned/scheme"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// KubernetesProvider implements Provider using Kubernetes informers.
type KubernetesProvider struct {
	config        *rest.Config
	namespace     string
	confined      namespaceWatch
	prepareEvent  func(context.Context, WatchEvent) error
	completeEvent func(context.Context, WatchEvent) error
	// watchModelAdapters keeps the existing adapter discovery behavior by default.
	watchModelAdapters bool
	// watchModelClaims adds ModelClaim objects to what is watched.
	watchModelClaims bool
	modelListHealth  *ModelListHealth
}

// NewKubernetesProvider creates a new Kubernetes discovery provider.
func NewKubernetesProvider(config *rest.Config) *KubernetesProvider {
	return &KubernetesProvider{config: config, watchModelAdapters: true}
}

// WithModelAdapters controls whether the provider lists and watches ModelAdapters.
func (p *KubernetesProvider) WithModelAdapters(enabled bool) *KubernetesProvider {
	p.watchModelAdapters = enabled
	return p
}

// WithModelClaims makes the provider watch ModelClaim objects as well. The
// gateway needs them to answer for a model that is claimed but not placed yet.
func (p *KubernetesProvider) WithModelClaims() *KubernetesProvider {
	p.watchModelClaims = true
	return p
}

// WithModelListHealth enables on-demand verification of the model-list cache.
// A positive maxAge is required; callers select the bound they need.
func (p *KubernetesProvider) WithModelListHealth(maxAge time.Duration) *KubernetesProvider {
	p.modelListHealth = newModelListHealth(maxAge)
	return p
}

// ModelListHealth returns the verification state after WatchApplied starts.
func (p *KubernetesProvider) ModelListHealth() *ModelListHealth {
	return p.modelListHealth
}

// Type returns the provider type identifier.
func (p *KubernetesProvider) Type() string {
	return "kubernetes"
}

// Watch starts K8s informers with the handler wired directly into informer callbacks.
// Watch returns once the initial sync and reconcile are complete. After return,
// informer callbacks continue delivering ongoing changes asynchronously.
func (p *KubernetesProvider) Watch(handler EventHandler, stopCh <-chan struct{}) error {
	return p.watch(func(ev WatchEvent) bool {
		handler(ev)
		return true
	}, stopCh)
}

// WatchApplied is Watch with a handler that reports whether a cache event was
// applied. Use it when model-list verification is enabled.
func (p *KubernetesProvider) WatchApplied(handler func(WatchEvent) bool, stopCh <-chan struct{}) error {
	return p.watch(handler, stopCh)
}

func (p *KubernetesProvider) configureModelListHealth(k8sClientSet kubernetes.Interface, crdClientSet v1alpha1.Interface) {
	health := p.modelListHealth
	if health == nil {
		return
	}
	health.snapshot = func(ctx context.Context) (sourceVersions, error) {
		pods, err := listPodVersionsForNamespace(ctx, k8sClientSet, p.namespace)
		if err != nil {
			return sourceVersions{}, err
		}
		adapters := make(map[string]objectVersion)
		if p.watchModelAdapters {
			adapters, err = listAdapterVersionsForNamespace(ctx, crdClientSet, p.namespace)
			if err != nil {
				return sourceVersions{}, err
			}
		}
		return sourceVersions{pods: pods, adapters: adapters}, nil
	}
}

func (p *KubernetesProvider) watch(handler func(WatchEvent) bool, stopCh <-chan struct{}) error {
	if p.namespace != "" {
		return p.WatchContext(context.Background(), handler, stopCh)
	}
	return p.watchContext(context.Background(), handler, stopCh)
}

func (p *KubernetesProvider) watchContext(ctx context.Context, handler func(WatchEvent) bool, stopCh <-chan struct{}) error {
	ctx, cancel := contextWithStop(ctx, stopCh)
	defer cancel()
	if err := v1alpha1scheme.AddToScheme(scheme.Scheme); err != nil {
		return err
	}

	config := p.config
	k8sClientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	factory := informers.NewSharedInformerFactoryWithOptions(k8sClientSet, 0, informers.WithNamespace(p.namespace))

	podInformer := factory.Core().V1().Pods().Informer()
	var modelInformer cache.SharedIndexInformer
	var claimInformer cache.SharedIndexInformer
	var crdFactory crdinformers.SharedInformerFactory
	var crdClientSet v1alpha1.Interface
	if p.watchModelAdapters || p.watchModelClaims {
		crdClientSet, err = v1alpha1.NewForConfig(config)
		if err != nil {
			return err
		}
		crdFactory = crdinformers.NewSharedInformerFactoryWithOptions(crdClientSet, 0, crdinformers.WithNamespace(p.namespace))
		if p.watchModelAdapters {
			modelInformer = crdFactory.Model().V1alpha1().ModelAdapters().Informer()
		}
		if p.watchModelClaims && canListModelClaimsForNamespace(ctx, crdClientSet, p.namespace) {
			claimInformer = crdFactory.Model().V1alpha1().ModelClaims().Informer()
		}
	}
	p.configureModelListHealth(k8sClientSet, crdClientSet)

	// Wire handler directly into informer callbacks.
	// Events flow from the start — including during the initial list phase.
	var requiredHandlers []cache.ResourceEventHandlerRegistration
	registerHandlers := func(inf cache.SharedIndexInformer, required bool) error {
		registration, err := p.registerHandler(inf, required, handler)
		if err != nil {
			return err
		}
		if required {
			requiredHandlers = append(requiredHandlers, registration)
		}
		return nil
	}

	if err := registerHandlers(podInformer, true); err != nil {
		return err
	}
	if modelInformer != nil {
		if err := registerHandlers(modelInformer, true); err != nil {
			return err
		}
	}
	if claimInformer != nil {
		if err := registerHandlers(claimInformer, false); err != nil {
			return err
		}
	}

	// Start informers and wait for initial list+sync.
	// During this phase, AddFunc fires for each existing object.
	factory.Start(stopCh)
	if crdFactory != nil {
		crdFactory.Start(stopCh)
	}

	// ModelClaims are left out of the wait. They only tell a model that is
	// claimed but not placed yet from one that nobody serves. A gateway whose
	// role cannot list them yet should still start, and answer for such a
	// model as it did before.
	requiredSync := []cache.InformerSynced{podInformer.HasSynced}
	if modelInformer != nil {
		requiredSync = append(requiredSync, modelInformer.HasSynced)
	}
	for _, registration := range requiredHandlers {
		requiredSync = append(requiredSync, registration.HasSynced)
	}
	if !cache.WaitForCacheSync(ctx.Done(), requiredSync...) {
		if ctx.Err() != nil {
			return fmt.Errorf("sync model discovery in namespace %q: %w", p.namespace, ctx.Err())
		}
		return errors.New("model discovery stopped before synchronization")
	}

	// Post-sync reconcile: re-emit all ModelAdapters to fix ordering.
	// During initial sync, Pod and ModelAdapter informers list concurrently.
	// A ModelAdapter's AddFunc may fire before its pods' AddFunc, causing
	// the pod-model mapping to be missed. Re-emitting adapters after sync
	// ensures all mappings are established (addModelAdapter is idempotent).
	adapterCount := 0
	if modelInformer != nil {
		adapters := modelInformer.GetStore().List()
		adapterCount = len(adapters)
		for _, obj := range adapters {
			if err := ctx.Err(); err != nil {
				return err
			}
			p.deliver(WatchEvent{Type: EventAdd, Object: obj}, true, handler)
		}
	}

	klog.InfoS("Kubernetes discovery provider initialized",
		"pods", len(podInformer.GetStore().List()), "modelAdapters", adapterCount)

	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (p *KubernetesProvider) deliver(ev WatchEvent, required bool, handler func(WatchEvent) bool) {
	if !p.acceptEvent(ev) {
		return
	}
	if required && p.modelListHealth != nil {
		p.modelListHealth.Apply(ev, handler)
	} else {
		handler(ev)
	}
}

func (p *KubernetesProvider) registerHandler(inf cache.SharedIndexInformer, required bool, handler func(WatchEvent) bool) (cache.ResourceEventHandlerRegistration, error) {
	registration, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) { p.deliver(WatchEvent{Type: EventAdd, Object: obj}, required, handler) },
		UpdateFunc: func(oldObj, newObj interface{}) {
			p.deliver(WatchEvent{Type: EventUpdate, Object: newObj, OldObject: oldObj}, required, handler)
		},
		DeleteFunc: func(obj interface{}) {
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			if tombstone, ok := obj.(*cache.DeletedFinalStateUnknown); ok && tombstone != nil {
				obj = tombstone.Obj
			}
			p.deliver(WatchEvent{Type: EventDelete, Object: obj}, required, handler)
		},
	})
	if err != nil {
		return nil, err
	}
	if required && p.modelListHealth != nil {
		err = inf.SetWatchErrorHandler(func(_ *cache.Reflector, watchErr error) {
			p.modelListHealth.Invalidate()
			klog.ErrorS(watchErr, "Model-list discovery watch failed")
		})
	}
	return registration, err
}

// canListModelClaims lists ModelClaims once before they are watched. A role
// that may not list them, or a cluster without the ModelClaim CRD, is logged
// here once, where an informer would log it every minute for as long as the
// process runs. Any other error is left to the informer, which retries it.
func canListModelClaims(client v1alpha1.Interface) bool {
	return canListModelClaimsForNamespace(context.Background(), client, metav1.NamespaceAll)
}

func canListModelClaimsForNamespace(ctx context.Context, client v1alpha1.Interface, namespace string) bool {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := client.ModelV1alpha1().ModelClaims(namespace).List(ctx, metav1.ListOptions{Limit: 1})
	if apierrors.IsForbidden(err) || apierrors.IsNotFound(err) {
		klog.InfoS("Not watching ModelClaims: a model that is claimed but not placed yet "+
			"is answered as one that does not exist", "err", err)
		return false
	}
	return true
}

var _ Provider = (*KubernetesProvider)(nil)
