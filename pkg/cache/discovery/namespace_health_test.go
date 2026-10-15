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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
	v1alpha1 "github.com/vllm-project/aibrix/pkg/client/clientset/versioned"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

func TestNamespaceEventGuardLeavesHealthVersionsUnchanged(t *testing.T) {
	p, err := NewKubernetesProviderForNamespace(&rest.Config{Host: "http://127.0.0.1:1"}, "managed")
	require.NoError(t, err)
	p.WithModelListHealth(time.Minute)
	inside := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "same", Namespace: "managed", UID: "inside", ResourceVersion: "1"}}
	var calls atomic.Int32
	handler := func(context.Context, WatchEvent) (bool, error) { calls.Add(1); return true, nil }
	_, err = p.DispatchNamespaced(context.Background(), WatchEvent{Type: EventAdd, Object: inside}, true, handler)
	require.NoError(t, err)
	baseline := p.modelListHealth.applied.pods["managed/same"]
	foreign := inside.DeepCopy()
	foreign.Namespace = "foreign"
	foreign.UID = "foreign"
	foreign.ResourceVersion = "9"
	for _, event := range []WatchEvent{
		{Type: EventAdd, Object: foreign}, {Type: EventDelete, Object: foreign},
		{Type: EventDelete, Object: cache.DeletedFinalStateUnknown{Obj: foreign}},
		{Type: EventDelete, Object: &cache.DeletedFinalStateUnknown{Obj: foreign}},
		{Type: EventUpdate, Object: inside, OldObject: foreign},
		{Type: EventUpdate, Object: foreign, OldObject: inside},
		{Type: EventAdd, Object: &modelv1alpha1.ModelAdapter{ObjectMeta: foreign.ObjectMeta}},
		{Type: EventAdd, Object: &modelv1alpha1.ModelClaim{ObjectMeta: foreign.ObjectMeta}},
		{Type: EventAdd, Object: (*v1.Pod)(nil)},
	} {
		before := calls.Load()
		applied, err := p.DispatchNamespaced(context.Background(), event, true, handler)
		require.NoError(t, err)
		require.False(t, applied)
		require.Equal(t, before, calls.Load())
		require.Len(t, p.modelListHealth.applied.pods, 1)
		require.Equal(t, baseline, p.modelListHealth.applied.pods["managed/same"])
	}
	claim := &modelv1alpha1.ModelClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim", Namespace: "managed"}}
	_, err = p.DispatchNamespaced(context.Background(), WatchEvent{Type: EventAdd, Object: claim}, false, handler)
	require.NoError(t, err)
	require.Len(t, p.modelListHealth.applied.pods, 1)
	require.Empty(t, p.modelListHealth.applied.adapters)
}

func TestNamespaceHealthBarrierCancellationDoesNotAdvanceVersions(t *testing.T) {
	h := newModelListHealth(time.Minute)
	h.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := h.ApplyContext(ctx, WatchEvent{Type: EventAdd, Object: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "blocked", Namespace: "managed"}}}, func(context.Context, WatchEvent) (bool, error) {
			t.Error("interrupted handler was entered")
			return true, nil
		})
		result <- err
	}()
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("health lock wait ignored cancellation")
	}
	require.Empty(t, h.applied.pods)
	h.mu.Unlock()
	_, healthy := h.ModelsIfHealthy(func() []string { return []string{"blocked"} })
	require.False(t, healthy)
}

func TestNamespaceHealthPaginationUsesScopeOnEveryPage(t *testing.T) {
	for _, resource := range []string{"pods", "modeladapters"} {
		t.Run(resource, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Contains(t, r.URL.Path, "/namespaces/managed/")
				requests.Add(1)
				continued := r.URL.Query().Get("continue") != ""
				meta := metav1.ListMeta{ResourceVersion: "1"}
				if !continued {
					meta.Continue = "next-page"
				}
				w.Header().Set("Content-Type", "application/json")
				if resource == "pods" {
					_ = json.NewEncoder(w).Encode(&v1.PodList{TypeMeta: metav1.TypeMeta{Kind: "PodList", APIVersion: "v1"}, ListMeta: meta})
				} else {
					_ = json.NewEncoder(w).Encode(&modelv1alpha1.ModelAdapterList{TypeMeta: metav1.TypeMeta{Kind: "ModelAdapterList", APIVersion: "model.aibrix.ai/v1alpha1"}, ListMeta: meta})
				}
			}))
			defer server.Close()
			config := &rest.Config{Host: server.URL}
			if resource == "pods" {
				client, err := kubernetes.NewForConfig(config)
				require.NoError(t, err)
				_, err = listPodVersionsForNamespace(context.Background(), client, "managed")
				require.NoError(t, err)
			} else {
				client, err := v1alpha1.NewForConfig(config)
				require.NoError(t, err)
				_, err = listAdapterVersionsForNamespace(context.Background(), client, "managed")
				require.NoError(t, err)
			}
			require.EqualValues(t, 2, requests.Load())
		})
	}
}

func TestNamespaceDependencyPreparationRunsOutsideHealthBarrier(t *testing.T) {
	p, err := NewKubernetesProviderForNamespace(&rest.Config{Host: "https://localhost"}, "managed")
	require.NoError(t, err)
	p.WithModelListHealth(time.Minute)
	prepared := false
	p.WithEventPreparation(func(context.Context, WatchEvent) error {
		require.True(t, p.modelListHealth.mu.TryLock(), "dependency construction must not hold the health barrier")
		p.modelListHealth.mu.Unlock()
		prepared = true
		return nil
	})
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "managed", Name: "served", UID: "one", ResourceVersion: "1"}}
	applied, err := p.DispatchNamespaced(context.Background(), WatchEvent{Type: EventAdd, Object: pod}, true, func(context.Context, WatchEvent) (bool, error) { require.True(t, prepared); return true, nil })
	require.NoError(t, err)
	require.True(t, applied)
	require.Contains(t, p.modelListHealth.applied.pods, "managed/served")
}

func TestVerifiedReadsCancelWithHeldHealthOrVerificationLock(t *testing.T) {
	for _, kind := range []string{"health", "verification"} {
		t.Run(kind, func(t *testing.T) {
			h := newModelListHealth(time.Minute)
			if kind == "health" {
				h.mu.Lock()
				defer h.mu.Unlock()
			} else {
				h.verifyMu.Lock()
				defer h.verifyMu.Unlock()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- h.EnsureVerified(ctx) }()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case <-time.After(time.Second):
				t.Fatal("verification reader waited for held lock")
			}
		})
	}
}

func TestEventCompletionRunsOutsideHealthAndCancelsWithoutRecording(t *testing.T) {
	h := newModelListHealth(time.Minute)
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "managed", Name: "served", UID: "one", ResourceVersion: "1"}}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	applied, err := h.ApplyContextWithCompletion(ctx, WatchEvent{Type: EventAdd, Object: pod}, func(context.Context, WatchEvent) (bool, error) { return true, nil }, func(ctx context.Context, _ WatchEvent) error {
		require.True(t, h.mu.TryLock())
		h.mu.Unlock()
		<-ctx.Done()
		return ctx.Err()
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, applied)
	require.Empty(t, h.applied.pods)
}
