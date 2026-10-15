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
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

func TestNamespacedLiveWatchesRelistAfterResourceVersionExpires(t *testing.T) {
	for _, resource := range []string{"pods", "modeladapters", "modelclaims"} {
		t.Run(resource, func(t *testing.T) { testNamespacedRelist(t, resource) })
	}
}

func testNamespacedRelist(t *testing.T, resource string) {
	t.Helper()
	var mu sync.Mutex
	var requests []string
	liveWatches, relists, applications := 0, 0, 0
	expired := false
	interrupt := make(chan struct{})
	watchStarted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.URL.Path)
		mu.Unlock()
		if !managedDiscoveryPath(r.URL.Path) {
			http.NotFound(w, r)
			return
		}
		if path.Base(r.URL.Path) != resource {
			namespaceReply(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("watch") == "true" {
			// Access probes have no resourceVersion. Only interrupt the first
			// live informer stream after its initial LIST has completed.
			live := r.URL.Query().Get("resourceVersion") != ""
			first := false
			if live {
				mu.Lock()
				liveWatches++
				first = liveWatches == 1
				mu.Unlock()
			}
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			if first {
				close(watchStarted)
				select {
				case <-interrupt:
					mu.Lock()
					expired = true
					mu.Unlock()
					// Disconnect writes are harmless; the test also requires a
					// subsequent relist and live watch before it can pass.
					_ = json.NewEncoder(w).Encode(map[string]any{"type": "ERROR", "object": &metav1.Status{
						TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: "Failure",
						Reason: metav1.StatusReasonExpired, Code: http.StatusGone,
					}})
					w.(http.Flusher).Flush()
					return
				case <-r.Context().Done():
					return
				}
			}
			<-r.Context().Done()
			return
		}
		mu.Lock()
		version := "1"
		if expired {
			version = "2"
			relists++
		}
		mu.Unlock()
		meta := metav1.ObjectMeta{Namespace: "managed", Name: "served", UID: "served", ResourceVersion: version}
		list := metav1.ListMeta{ResourceVersion: version}
		// All objects are fixed JSON fixtures; only client disconnects can fail writing.
		switch resource {
		case "pods":
			_ = json.NewEncoder(w).Encode(&corev1.PodList{
				TypeMeta: metav1.TypeMeta{Kind: "PodList", APIVersion: "v1"}, ListMeta: list,
				Items: []corev1.Pod{{ObjectMeta: meta}},
			})
		case "modeladapters":
			_ = json.NewEncoder(w).Encode(&modelv1alpha1.ModelAdapterList{
				TypeMeta: metav1.TypeMeta{Kind: "ModelAdapterList", APIVersion: "model.aibrix.ai/v1alpha1"}, ListMeta: list,
				Items: []modelv1alpha1.ModelAdapter{{ObjectMeta: meta}},
			})
		case "modelclaims":
			_ = json.NewEncoder(w).Encode(&modelv1alpha1.ModelClaimList{
				TypeMeta: metav1.TypeMeta{Kind: "ModelClaimList", APIVersion: "model.aibrix.ai/v1alpha1"}, ListMeta: list,
				Items: []modelv1alpha1.ModelClaim{{ObjectMeta: meta}},
			})
		}
	}))
	provider, err := NewKubernetesProviderForNamespace(&rest.Config{Host: server.URL}, "managed")
	require.NoError(t, err)
	provider.WithModelClaims().WithModelListHealth(time.Minute)
	lifetime, stop := context.WithCancel(context.Background())
	t.Cleanup(func() { stop(); provider.Shutdown(); server.Close() })
	startup, stopStartup := context.WithTimeout(lifetime, 5*time.Second)
	defer stopStartup()
	require.NoError(t, provider.WatchNamespaced(startup, lifetime, func(_ context.Context, event WatchEvent) (bool, error) {
		metadata := event.Object.(metav1.Object)
		if metadata.GetResourceVersion() == "2" {
			mu.Lock()
			applications++
			mu.Unlock()
		}
		return true, nil
	}))
	stopStartup()
	select {
	case <-watchStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("live informer stream did not start")
	}
	close(interrupt)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return relists > 0 && liveWatches >= 2 && applications > 0
	}, 10*time.Second, 10*time.Millisecond, "expired live watch must relist, apply changed data, and reopen")
	mu.Lock()
	defer mu.Unlock()
	for _, request := range requests {
		require.True(t, managedDiscoveryPath(request), "%s", request)
	}
}
