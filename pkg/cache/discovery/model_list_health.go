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
	"sync"
	"sync/atomic"
	"time"

	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	v1alpha1 "github.com/vllm-project/aibrix/pkg/client/clientset/versioned"
	"github.com/vllm-project/aibrix/pkg/utils"
)

const (
	modelListPageSize            = 500
	modelListVerificationTimeout = 10 * time.Second
	modelListVerificationRetry   = 5 * time.Second
)

// ErrModelListDiscoveryUnavailable means the model cache cannot currently be
// verified against the required Kubernetes discovery sources.
var ErrModelListDiscoveryUnavailable = errors.New("model discovery is unavailable")

type objectVersion struct {
	uid             string
	resourceVersion string
}

type sourceVersions struct {
	pods     map[string]objectVersion
	adapters map[string]objectVersion
}

// ModelListHealth tracks events after their cache handler completes and checks
// them against a current Kubernetes snapshot when the last proof expires.
// Its lock also makes the health decision and model-list read one snapshot.
type ModelListHealth struct {
	interrupted  atomic.Bool
	inflight     atomic.Int64
	mu           sync.RWMutex
	verifyMu     sync.Mutex
	applied      sourceVersions
	lastVerified time.Time
	retryAfter   time.Time
	errorEpoch   uint64
	maxAge       time.Duration
	snapshot     func(context.Context) (sourceVersions, error)
}

func newModelListHealth(maxAge time.Duration) *ModelListHealth {
	return &ModelListHealth{
		maxAge: maxAge,
		applied: sourceVersions{
			pods:     make(map[string]objectVersion),
			adapters: make(map[string]objectVersion),
		},
	}
}

func objectIdentity(obj metav1.Object) (string, objectVersion) {
	return obj.GetNamespace() + "/" + obj.GetName(), objectVersion{
		uid:             string(obj.GetUID()),
		resourceVersion: obj.GetResourceVersion(),
	}
}

// Apply holds the health read barrier while a required-source event changes
// the derived cache. A failed handler cannot advance the applied record.
func (h *ModelListHealth) Apply(ev WatchEvent, handler func(WatchEvent) bool) {
	_, _ = h.ApplyContext(context.Background(), ev, func(_ context.Context, ev WatchEvent) (bool, error) { return handler(ev), nil })
}

// ApplyContext preserves health-before-store lock order while allowing startup
// and lifetime cancellation to interrupt both barrier and cache-handler waits.
// Interrupted events never advance the UID/resource-version snapshot.
func (h *ModelListHealth) ApplyContext(ctx context.Context, ev WatchEvent, handler func(context.Context, WatchEvent) (bool, error)) (bool, error) {
	return h.ApplyContextWithCompletion(ctx, ev, handler, nil)
}

// ApplyContextWithCompletion changes membership under the health barrier,
// releases it for cancellable I/O, then records the event only after completion.
// Verification cannot publish a healthy proof while an event is in flight.
func (h *ModelListHealth) ApplyContextWithCompletion(ctx context.Context, ev WatchEvent, handler func(context.Context, WatchEvent) (bool, error), complete func(context.Context, WatchEvent) error) (bool, error) {
	if err := utils.LockContext(ctx, h.mu.TryLock, h.mu.Unlock); err != nil {
		h.interrupted.Store(true)
		return false, err
	}
	var versions map[string]objectVersion
	var obj metav1.Object
	switch typed := ev.Object.(type) {
	case *v1.Pod:
		if typed != nil {
			versions, obj = h.applied.pods, typed
		}
	case *modelv1alpha1.ModelAdapter:
		if typed != nil {
			versions, obj = h.applied.adapters, typed
		}
	}
	if obj == nil {
		h.invalidateLocked()
		h.mu.Unlock()
		return false, nil
	}
	h.inflight.Add(1)
	defer h.inflight.Add(-1)
	applied, err := handler(ctx, ev)
	if err == nil {
		err = ctx.Err()
	}
	if !applied || err != nil {
		h.invalidateLocked()
		h.mu.Unlock()
		return false, err
	}
	h.mu.Unlock()
	if complete != nil {
		err = complete(ctx, ev)
	}
	if err == nil {
		err = ctx.Err()
	}
	if lockErr := utils.LockContext(ctx, h.mu.TryLock, h.mu.Unlock); lockErr != nil {
		h.interrupted.Store(true)
		return false, lockErr
	}
	defer h.mu.Unlock()
	if err != nil {
		h.invalidateLocked()
		return false, err
	}
	key, version := objectIdentity(obj)
	if ev.Type == EventDelete {
		delete(versions, key)
	} else {
		versions[key] = version
	}
	return true, nil
}

// InvalidateContext prevents a reported watch failure from blocking shutdown.
func (h *ModelListHealth) InvalidateContext(ctx context.Context) {
	if err := utils.LockContext(ctx, h.mu.TryLock, h.mu.Unlock); err != nil {
		h.interrupted.Store(true)
		return
	}
	defer h.mu.Unlock()
	h.invalidateLocked()
}

// Invalidate causes the next request to verify immediately after a reported
// watch error. A verification already in flight cannot clear this error.
func (h *ModelListHealth) Invalidate() {
	h.mu.Lock()
	h.invalidateLocked()
	h.mu.Unlock()
}

func (h *ModelListHealth) invalidateLocked() {
	h.lastVerified = time.Time{}
	h.retryAfter = time.Time{}
	h.errorEpoch++
}

func equalVersions(a, b map[string]objectVersion) bool {
	if len(a) != len(b) {
		return false
	}
	for key, version := range a {
		other, ok := b[key]
		if !ok || other != version {
			return false
		}
	}
	return true
}

func (h *ModelListHealth) verificationState(now time.Time) (fresh, retryLater bool, epoch uint64) {
	fresh, retryLater, epoch, _ = h.verificationStateContext(context.Background(), now)
	return
}

func (h *ModelListHealth) verificationStateContext(ctx context.Context, now time.Time) (fresh, retryLater bool, epoch uint64, err error) {
	if err = utils.LockContext(ctx, h.mu.TryRLock, h.mu.RUnlock); err != nil {
		return
	}
	defer h.mu.RUnlock()
	return h.inflight.Load() == 0 && !h.lastVerified.IsZero() && now.Sub(h.lastVerified) < h.maxAge, now.Before(h.retryAfter), h.errorEpoch, nil
}

// EnsureVerified checks a complete current snapshot at most once per maxAge.
// Its timeout includes lock waits and I/O. Client cancellation does not delay
// verification for another client or change a previously valid proof.
func (h *ModelListHealth) EnsureVerified(ctx context.Context) error {
	checkCtx, cancel := context.WithTimeout(ctx, modelListVerificationTimeout)
	defer cancel()
	if err := checkCtx.Err(); err != nil {
		return err
	}
	if h.interrupted.Load() {
		return ErrModelListDiscoveryUnavailable
	}
	fresh, retryLater, _, err := h.verificationStateContext(checkCtx, time.Now())
	if err != nil {
		return err
	}
	if fresh {
		return nil
	}
	if retryLater {
		return ErrModelListDiscoveryUnavailable
	}
	if err := utils.LockContext(checkCtx, h.verifyMu.TryLock, h.verifyMu.Unlock); err != nil {
		return err
	}
	defer h.verifyMu.Unlock()
	fresh, retryLater, epoch, err := h.verificationStateContext(checkCtx, time.Now())
	if err != nil {
		return err
	}
	if fresh {
		return nil
	}
	if retryLater || h.snapshot == nil {
		return ErrModelListDiscoveryUnavailable
	}
	started := time.Now()
	current, snapshotErr := h.snapshot(checkCtx)
	if err := utils.LockContext(checkCtx, h.mu.TryLock, h.mu.Unlock); err != nil {
		return err
	}
	defer h.mu.Unlock()
	if snapshotErr == nil && h.inflight.Load() == 0 && epoch == h.errorEpoch && equalVersions(current.pods, h.applied.pods) && equalVersions(current.adapters, h.applied.adapters) {
		h.lastVerified = started
		h.retryAfter = time.Time{}
		return nil
	}
	h.lastVerified = time.Time{}
	h.retryAfter = time.Now().Add(modelListVerificationRetry)
	if snapshotErr != nil {
		return fmt.Errorf("verify model discovery: %w", snapshotErr)
	}
	return ErrModelListDiscoveryUnavailable
}

// ModelsIfHealthy retains the context-free interface for legacy callers.
func (h *ModelListHealth) ModelsIfHealthy(list func() []string) ([]string, bool) {
	models, healthy, _ := h.ModelsIfHealthyContext(context.Background(), func(context.Context) ([]string, error) { return list(), nil })
	return models, healthy
}

// ModelsIfHealthyContext takes a cancellable health snapshot and invokes a
// cancellable Store reader under the same barrier, in health-before-store order.
func (h *ModelListHealth) ModelsIfHealthyContext(ctx context.Context, list func(context.Context) ([]string, error)) ([]string, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if h.interrupted.Load() {
		return nil, false, nil
	}
	if err := utils.LockContext(ctx, h.mu.TryRLock, h.mu.RUnlock); err != nil {
		return nil, false, err
	}
	defer h.mu.RUnlock()
	if h.inflight.Load() != 0 || h.lastVerified.IsZero() || time.Since(h.lastVerified) >= h.maxAge {
		return nil, false, nil
	}
	models, err := list(ctx)
	if err == nil {
		err = ctx.Err()
	}
	return models, err == nil, err
}

func listPodVersions(ctx context.Context, client kubernetes.Interface) (map[string]objectVersion, error) {
	return listPodVersionsForNamespace(ctx, client, metav1.NamespaceAll)
}

func listPodVersionsForNamespace(ctx context.Context, client kubernetes.Interface, namespace string) (map[string]objectVersion, error) {
	versions := make(map[string]objectVersion)
	opts := metav1.ListOptions{Limit: modelListPageSize}
	for {
		page, err := client.CoreV1().Pods(namespace).List(ctx, opts)
		if err != nil {
			return nil, err
		}
		for i := range page.Items {
			key, version := objectIdentity(&page.Items[i])
			versions[key] = version
		}
		if page.Continue == "" {
			return versions, nil
		}
		if page.Continue == opts.Continue {
			return nil, errors.New("repeated Pod list continuation token")
		}
		opts.Continue = page.Continue
	}
}

func listAdapterVersions(ctx context.Context, client v1alpha1.Interface) (map[string]objectVersion, error) {
	return listAdapterVersionsForNamespace(ctx, client, metav1.NamespaceAll)
}

func listAdapterVersionsForNamespace(ctx context.Context, client v1alpha1.Interface, namespace string) (map[string]objectVersion, error) {
	versions := make(map[string]objectVersion)
	opts := metav1.ListOptions{Limit: modelListPageSize}
	for {
		page, err := client.ModelV1alpha1().ModelAdapters(namespace).List(ctx, opts)
		if err != nil {
			return nil, err
		}
		for i := range page.Items {
			key, version := objectIdentity(&page.Items[i])
			versions[key] = version
		}
		if page.Continue == "" {
			return versions, nil
		}
		if page.Continue == opts.Continue {
			return nil, errors.New("repeated ModelAdapter list continuation token")
		}
		opts.Continue = page.Continue
	}
}
