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
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

func managedDiscoveryPath(requestPath string) bool {
	switch requestPath {
	case "/api/v1/namespaces/managed",
		"/api/v1/namespaces/managed/pods",
		"/apis/model.aibrix.ai/v1alpha1/namespaces/managed/modeladapters",
		"/apis/model.aibrix.ai/v1alpha1/namespaces/managed/modelclaims":
		return true
	default:
		return false
	}
}

func TestNamespacedFixtureRejectsForeignPrefixPaths(t *testing.T) {
	for _, requestPath := range []string{
		"/api/v1/namespaces/managed-foreign",
		"/api/v1/namespaces/managed-foreign/pods",
		"/apis/model.aibrix.ai/v1alpha1/namespaces/managed-foreign/modeladapters",
		"/apis/model.aibrix.ai/v1alpha1/namespaces/managed-foreign/modelclaims",
		"/api/v1/pods",
		"/api/v1/namespaces/managed/secrets",
	} {
		t.Run(requestPath, func(t *testing.T) {
			require.False(t, managedDiscoveryPath(requestPath))
			for _, query := range []string{"", "?watch=true&resourceVersion=2"} {
				response := httptest.NewRecorder()
				namespaceReply(response, httptest.NewRequest(http.MethodGet, requestPath+query, nil))
				require.Equal(t, http.StatusNotFound, response.Code)
			}
		})
	}
}

func namespaceReply(w http.ResponseWriter, r *http.Request) {
	namespaceListReply(w, r, metav1.ListMeta{ResourceVersion: "1"})
}

func namespaceListReply(w http.ResponseWriter, r *http.Request, meta metav1.ListMeta) {
	if !managedDiscoveryPath(r.URL.Path) {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Query().Get("watch") == "true" {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		return
	}
	switch {
	case r.URL.Path == "/api/v1/namespaces/managed":
		_ = json.NewEncoder(w).Encode(&v1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metav1.ObjectMeta{Name: "managed"}})
	case strings.HasSuffix(r.URL.Path, "/pods"):
		_ = json.NewEncoder(w).Encode(&v1.PodList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"}, ListMeta: meta})
	case strings.HasSuffix(r.URL.Path, "/modeladapters"):
		_ = json.NewEncoder(w).Encode(&modelv1alpha1.ModelAdapterList{TypeMeta: metav1.TypeMeta{APIVersion: "model.aibrix.ai/v1alpha1", Kind: "ModelAdapterList"}, ListMeta: meta})
	case strings.HasSuffix(r.URL.Path, "/modelclaims"):
		_ = json.NewEncoder(w).Encode(&modelv1alpha1.ModelClaimList{TypeMeta: metav1.TypeMeta{APIVersion: "model.aibrix.ai/v1alpha1", Kind: "ModelClaimList"}, ListMeta: meta})
	default:
		http.NotFound(w, r)
	}
}

func discoveryAccessError(w http.ResponseWriter, code int) {
	reason := metav1.StatusReasonForbidden
	if code == http.StatusNotFound {
		reason = metav1.StatusReasonNotFound
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	// A canceled client may disconnect before the fixture finishes writing.
	_ = json.NewEncoder(w).Encode(&metav1.Status{
		TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: "Failure", Reason: reason, Code: int32(code),
	})
}

func TestNamespaceConfigurationFailsBeforeIO(t *testing.T) {
	for _, value := range []string{"", " ", " managed", "managed ", "Upper", "*", "a/b", strings.Repeat("a", 64)} {
		t.Run(value, func(t *testing.T) {
			provider, err := NewKubernetesProviderForNamespace(&rest.Config{Host: "http://127.0.0.1:1"}, value)
			require.Error(t, err)
			require.Nil(t, provider)
		})
	}
	_, err := NewKubernetesProviderForNamespace(nil, "managed")
	require.Error(t, err)
	p, err := NewKubernetesProviderForNamespace(&rest.Config{Host: "http://127.0.0.1:1"}, "managed")
	require.NoError(t, err)
	require.Equal(t, "managed", p.Namespace())
	require.Error(t, p.Watch(func(WatchEvent) {}, make(chan struct{})), "legacy Watch must not escape confinement")
}

func TestNamespacedStartupRejectsUnavailableNamespaceBeforeDiscovery(t *testing.T) {
	for _, condition := range []string{"missing", "forbidden", "terminating-phase", "deletion-timestamp"} {
		t.Run(condition, func(t *testing.T) {
			var discoveryRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/namespaces/managed" {
					discoveryRequests.Add(1)
					http.Error(w, "unexpected discovery", http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				// Response write errors are irrelevant to the client-side startup result.
				if condition == "missing" || condition == "forbidden" {
					code := http.StatusForbidden
					if condition == "missing" {
						code = http.StatusNotFound
					}
					discoveryAccessError(w, code)
					return
				}
				namespace := &v1.Namespace{TypeMeta: metav1.TypeMeta{Kind: "Namespace", APIVersion: "v1"},
					ObjectMeta: metav1.ObjectMeta{Name: "managed"}}
				if condition == "terminating-phase" {
					namespace.Status.Phase = v1.NamespaceTerminating
				}
				if condition == "deletion-timestamp" {
					now := metav1.Now()
					namespace.DeletionTimestamp = &now
				}
				_ = json.NewEncoder(w).Encode(namespace)
			}))
			defer server.Close()
			provider, err := NewKubernetesProviderForNamespace(&rest.Config{Host: server.URL}, "managed")
			require.NoError(t, err)
			defer provider.Shutdown()
			ctx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			err = provider.WatchNamespaced(ctx, ctx, func(context.Context, WatchEvent) (bool, error) {
				t.Error("unavailable namespace must not dispatch an event")
				return true, nil
			})
			require.Error(t, err)
			require.ErrorContains(t, err, "managed")
			require.Zero(t, discoveryRequests.Load())
		})
	}
}

func TestNamespacedOptionalClaimsCanBeAbsent(t *testing.T) {
	for _, code := range []int{http.StatusForbidden, http.StatusNotFound} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var claimRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/modelclaims") {
					claimRequests.Add(1)
					discoveryAccessError(w, code)
					return
				}
				namespaceReply(w, r)
			}))
			defer server.Close()
			provider, err := NewKubernetesProviderForNamespace(&rest.Config{Host: server.URL}, "managed")
			require.NoError(t, err)
			provider.WithModelAdapters(false).WithModelClaims()
			lifetime, stop := context.WithCancel(context.Background())
			defer func() { stop(); provider.Shutdown() }()
			startup, stopStartup := context.WithTimeout(lifetime, 5*time.Second)
			defer stopStartup()
			require.NoError(t, provider.WatchNamespaced(startup, lifetime,
				func(context.Context, WatchEvent) (bool, error) { return true, nil }))
			require.EqualValues(t, 1, claimRequests.Load(), "only the optional availability probe is allowed")
		})
	}
}

func TestNamespacedRequestsIncludeHealthAndOptionalClaims(t *testing.T) {
	for _, api := range []string{"namespaced", "stop-channel"} {
		t.Run(api, func(t *testing.T) {
			var mu sync.Mutex
			var requests []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				requests = append(requests, r.URL.Path+"?"+r.URL.RawQuery)
				mu.Unlock()
				installationAPI(w, r)
			}))
			defer server.Close()
			p, err := NewKubernetesProviderForNamespace(&rest.Config{Host: server.URL}, "managed")
			require.NoError(t, err)
			p.WithModelClaims().WithModelListHealth(time.Minute)
			lifetime, cancel := context.WithCancel(context.Background())
			defer func() { cancel(); p.Shutdown() }()
			startup, stop := context.WithTimeout(lifetime, 5*time.Second)
			defer stop()
			if api == "namespaced" {
				err = p.WatchNamespaced(startup, lifetime, func(context.Context, WatchEvent) (bool, error) { return true, nil })
			} else {
				err = p.WatchContext(startup, func(WatchEvent) bool { return true }, lifetime.Done())
			}
			require.NoError(t, err)
			stop() // Successful startup cancellation must not stop live discovery.
			require.NoError(t, p.ModelListHealth().EnsureVerified(lifetime))
			mu.Lock()
			defer mu.Unlock()
			var lists, watches int
			continuations := make(map[string]bool)
			for _, request := range requests {
				requestPath, _, _ := strings.Cut(request, "?")
				require.True(t, managedDiscoveryPath(requestPath), "%s", request)
				if strings.Contains(request, "continue=page-two") {
					continuations[requestPath] = true
				}
				if strings.Contains(request, "watch=true") {
					watches++
				} else {
					lists++
				}
			}
			require.True(t, continuations["/api/v1/namespaces/managed/pods"])
			require.True(t, continuations["/apis/model.aibrix.ai/v1alpha1/namespaces/managed/modeladapters"])
			require.GreaterOrEqual(t, watches, 4, "required sources have independent watch probes and live watches")
			require.GreaterOrEqual(t, lists, 7, "include Namespace, probes, initial lists, and verification")
		})
	}
}

func TestNamespacedRequiredAccessFailures(t *testing.T) {
	for _, resource := range []string{"pods", "modeladapters"} {
		for _, verb := range []string{"list", "watch"} {
			for _, code := range []int{http.StatusForbidden, http.StatusNotFound} {
				t.Run(resource+"/"+verb+"/"+http.StatusText(code), func(t *testing.T) {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						isWatch := r.URL.Query().Get("watch") == "true"
						if strings.HasSuffix(r.URL.Path, "/"+resource) && isWatch == (verb == "watch") {
							discoveryAccessError(w, code)
							return
						}
						namespaceReply(w, r)
					}))
					defer server.Close()
					p, err := NewKubernetesProviderForNamespace(&rest.Config{Host: server.URL}, "managed")
					require.NoError(t, err)
					lifetime, cancel := context.WithCancel(context.Background())
					defer cancel()
					startup, stop := context.WithTimeout(lifetime, time.Second)
					defer stop()
					err = p.WatchNamespaced(startup, lifetime, func(context.Context, WatchEvent) (bool, error) { return true, nil })
					require.ErrorContains(t, err, verb)
					p.Shutdown()
				})
			}
		}
	}
}

func TestNamespacedStartupCancelsBlockedRequests(t *testing.T) {
	for _, stage := range []string{"namespace", "list", "watch"} {
		t.Run(stage, func(t *testing.T) {
			entered, stopped := make(chan struct{}), make(chan struct{})
			var once sync.Once
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				block := stage == "namespace" && r.URL.Path == "/api/v1/namespaces/managed" || stage == "list" && strings.HasSuffix(r.URL.Path, "/pods") && r.URL.Query().Get("watch") != "true" || stage == "watch" && r.URL.Query().Get("watch") == "true"
				if block {
					once.Do(func() { close(entered) })
					<-r.Context().Done()
					close(stopped)
					return
				}
				namespaceReply(w, r)
			}))
			defer server.Close()
			p, err := NewKubernetesProviderForNamespace(&rest.Config{Host: server.URL}, "managed")
			require.NoError(t, err)
			p.WithModelAdapters(false)
			lifetime, stopLifetime := context.WithCancel(context.Background())
			defer stopLifetime()
			startup, stopStartup := context.WithCancel(lifetime)
			result := make(chan error, 1)
			go func() {
				result <- p.WatchNamespaced(startup, lifetime, func(context.Context, WatchEvent) (bool, error) { return true, nil })
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("request never reached blocked stage")
			}
			stopStartup()
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("startup failed to cancel request")
			}
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("HTTP request was abandoned")
			}
			p.Shutdown()
		})
	}
}

func TestNamespacedAdapterReconciliationCancellation(t *testing.T) {
	adapter := modelv1alpha1.ModelAdapter{ObjectMeta: metav1.ObjectMeta{Namespace: "managed", Name: "adapter", UID: "adapter", ResourceVersion: "1"}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/modeladapters") && r.URL.Query().Get("watch") != "true" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(&modelv1alpha1.ModelAdapterList{TypeMeta: metav1.TypeMeta{Kind: "ModelAdapterList", APIVersion: "model.aibrix.ai/v1alpha1"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: []modelv1alpha1.ModelAdapter{adapter}})
			return
		}
		namespaceReply(w, r)
	}))
	defer server.Close()
	p, err := NewKubernetesProviderForNamespace(&rest.Config{Host: server.URL}, "managed")
	require.NoError(t, err)
	p.WithModelListHealth(time.Minute)
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	startup, stopStartup := context.WithTimeout(lifetime, 5*time.Second)
	defer stopStartup()
	entered := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- p.WatchNamespaced(startup, lifetime, func(ctx context.Context, ev WatchEvent) (bool, error) {
			if _, adapterEvent := ev.Object.(*modelv1alpha1.ModelAdapter); adapterEvent {
				if _, isReconciliation := ctx.Deadline(); isReconciliation {
					close(entered)
					<-ctx.Done()
					return false, ctx.Err()
				}
			}
			return true, nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("post-sync reconciliation was not reached")
	}
	stopStartup()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not cancel")
	}
	p.Shutdown()
	_, healthy := p.ModelListHealth().ModelsIfHealthy(func() []string { return []string{"adapter"} })
	require.False(t, healthy)
}

func TestNamespacedCancellationDuringInitialHandlerSync(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/pods") && r.URL.Query().Get("watch") != "true" {
			w.Header().Set("Content-Type", "application/json")
			// Only receipt and cancellation matter; a disconnected fixture client is harmless.
			_ = json.NewEncoder(w).Encode(&v1.PodList{
				TypeMeta: metav1.TypeMeta{Kind: "PodList", APIVersion: "v1"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"},
				Items: []v1.Pod{{ObjectMeta: metav1.ObjectMeta{Namespace: "managed", Name: "blocked", UID: "pod", ResourceVersion: "1"}}},
			})
			return
		}
		namespaceReply(w, r)
	}))
	defer server.Close()
	provider, err := NewKubernetesProviderForNamespace(&rest.Config{Host: server.URL}, "managed")
	require.NoError(t, err)
	provider.WithModelAdapters(false).WithModelListHealth(time.Minute)
	lifetime, stop := context.WithCancel(context.Background())
	defer func() { stop(); provider.Shutdown() }()
	startup, stopStartup := context.WithCancel(lifetime)
	defer stopStartup()
	entered, callbackStopped := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- provider.WatchNamespaced(startup, lifetime, func(ctx context.Context, _ WatchEvent) (bool, error) {
			close(entered)
			<-ctx.Done()
			close(callbackStopped)
			return false, ctx.Err()
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("initial handler was not reached")
	}
	stopStartup()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("handler synchronization abandoned blocked work")
	}
	select {
	case <-callbackStopped:
	default:
		t.Fatal("startup returned before its handler stopped")
	}
	require.Empty(t, provider.modelListHealth.applied.pods)
}

func TestNamespacedCancellationBetweenReconciledAdapters(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/modeladapters") && r.URL.Query().Get("watch") != "true" {
			w.Header().Set("Content-Type", "application/json")
			// Only receipt and cancellation matter; a disconnected fixture client is harmless.
			_ = json.NewEncoder(w).Encode(&modelv1alpha1.ModelAdapterList{
				TypeMeta: metav1.TypeMeta{Kind: "ModelAdapterList", APIVersion: "model.aibrix.ai/v1alpha1"},
				ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: []modelv1alpha1.ModelAdapter{
					{ObjectMeta: metav1.ObjectMeta{Namespace: "managed", Name: "first", UID: "first", ResourceVersion: "1"}},
					{ObjectMeta: metav1.ObjectMeta{Namespace: "managed", Name: "second", UID: "second", ResourceVersion: "1"}},
				},
			})
			return
		}
		namespaceReply(w, r)
	}))
	defer server.Close()
	provider, err := NewKubernetesProviderForNamespace(&rest.Config{Host: server.URL}, "managed")
	require.NoError(t, err)
	provider.WithModelListHealth(time.Minute)
	lifetime, stop := context.WithCancel(context.Background())
	defer func() { stop(); provider.Shutdown() }()
	startup, stopStartup := context.WithTimeout(lifetime, 5*time.Second)
	defer stopStartup()
	var reconciled int
	err = provider.WatchNamespaced(startup, lifetime, func(ctx context.Context, _ WatchEvent) (bool, error) {
		if _, reconciliation := ctx.Deadline(); reconciliation {
			reconciled++
			stopStartup()
		}
		return true, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, reconciled, "the next adapter must not run after cancellation")
	_, healthy := provider.ModelListHealth().ModelsIfHealthy(func() []string { return []string{"partial"} })
	require.False(t, healthy)
}

func TestNamespacedRuntimeHandlerFailureKeepsDiscoveryForRecovery(t *testing.T) {
	events := make(chan *v1.Pod)
	watching := make(chan struct{})
	var watchStarted sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("watch") == "true" && r.URL.Query().Get("resourceVersion") != "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			watchStarted.Do(func() { close(watching) })
			for {
				select {
				case <-r.Context().Done():
					return
				case pod := <-events:
					_ = json.NewEncoder(w).Encode(map[string]any{"type": "MODIFIED", "object": pod})
					w.(http.Flusher).Flush()
				}
			}
		}
		namespaceReply(w, r)
	}))
	defer server.Close()
	p, err := NewKubernetesProviderForNamespace(&rest.Config{Host: server.URL}, "managed")
	require.NoError(t, err)
	p.WithModelAdapters(false).WithModelListHealth(time.Minute)
	lifetime, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); p.Shutdown() }()
	startup, stop := context.WithTimeout(lifetime, 5*time.Second)
	defer stop()
	deliveries := make(chan int32, 2)
	var calls atomic.Int32
	require.NoError(t, p.WatchNamespaced(startup, lifetime, func(context.Context, WatchEvent) (bool, error) {
		call := calls.Add(1)
		deliveries <- call
		if call == 1 {
			return false, errors.New("controlled runtime handler failure")
		}
		return true, nil
	}))
	select {
	case <-watching:
	case <-time.After(time.Second):
		t.Fatal("live watch not established")
	}
	stop()
	pod := &v1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{Namespace: "managed", Name: "served", UID: "one", ResourceVersion: "2"}}
	select {
	case events <- pod:
	case <-time.After(time.Second):
		t.Fatal("live watch stopped")
	}
	select {
	case <-deliveries:
	case <-time.After(time.Second):
		t.Fatal("first event not delivered")
	}
	pod = pod.DeepCopy()
	pod.ResourceVersion = "3"
	select {
	case events <- pod:
	case <-time.After(time.Second):
		t.Fatal("failed callback stopped discovery")
	}
	select {
	case <-deliveries:
	case <-time.After(time.Second):
		t.Fatal("recovery event not delivered")
	}
	require.Eventually(t, func() bool {
		p.modelListHealth.mu.RLock()
		defer p.modelListHealth.mu.RUnlock()
		return p.modelListHealth.applied.pods["managed/served"].resourceVersion == "3"
	}, time.Second, 10*time.Millisecond)
}
