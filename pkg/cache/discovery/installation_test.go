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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

func installationAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Query().Get("watch") == "true" {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		return
	}
	if r.URL.Path == "/api/v1/namespaces/workloads" {
		_ = json.NewEncoder(w).Encode(&v1.Namespace{TypeMeta: metav1.TypeMeta{Kind: "Namespace", APIVersion: "v1"}, ObjectMeta: metav1.ObjectMeta{Name: "workloads"}})
		return
	}
	meta := metav1.ListMeta{ResourceVersion: "7"}
	if r.URL.Query().Get("limit") == "500" && r.URL.Query().Get("continue") == "" {
		meta.Continue = "page-two"
	}
	switch {
	case strings.HasSuffix(r.URL.Path, "/pods"):
		_ = json.NewEncoder(w).Encode(&v1.PodList{TypeMeta: metav1.TypeMeta{Kind: "PodList", APIVersion: "v1"}, ListMeta: meta})
	case strings.HasSuffix(r.URL.Path, "/modeladapters"):
		_ = json.NewEncoder(w).Encode(&modelv1alpha1.ModelAdapterList{TypeMeta: metav1.TypeMeta{Kind: "ModelAdapterList", APIVersion: "model.aibrix.ai/v1alpha1"}, ListMeta: meta})
	case strings.HasSuffix(r.URL.Path, "/modelclaims"):
		_ = json.NewEncoder(w).Encode(&modelv1alpha1.ModelClaimList{TypeMeta: metav1.TypeMeta{Kind: "ModelClaimList", APIVersion: "model.aibrix.ai/v1alpha1"}, ListMeta: meta})
	default:
		http.NotFound(w, r)
	}
}

func TestInstallationNameValidation(t *testing.T) {
	for _, value := range []string{"", " ", "Workloads", " workloads", "workloads ", "*", "a/b"} {
		_, err := NewKubernetesProviderForNamespace(&rest.Config{Host: "http://127.0.0.1:1"}, value)
		require.Error(t, err, value)
	}
	_, err := NewKubernetesProviderForNamespace(nil, "workloads")
	require.Error(t, err)
}

func TestInstallationRequestsAndVerificationAreScoped(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path+"?"+r.URL.RawQuery)
		mu.Unlock()
		installationAPI(w, r)
	}))
	defer api.Close()
	p, err := NewKubernetesProviderForNamespace(&rest.Config{Host: api.URL}, "workloads")
	require.NoError(t, err)
	p.WithModelClaims().WithModelListHealth(time.Minute)
	stop := make(chan struct{})
	defer close(stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, p.WatchContext(ctx, func(WatchEvent) bool { return true }, stop))
	cancel()
	require.NoError(t, p.ModelListHealth().EnsureVerified(context.Background()))
	mu.Lock()
	defer mu.Unlock()
	continuations := make(map[string]bool)
	watches := 0
	for _, path := range paths {
		require.Contains(t, path, "/namespaces/workloads", "no cluster-wide request is allowed")
		if strings.Contains(path, "continue=page-two") {
			continuations[strings.Split(path, "?")[0]] = true
		}
		if strings.Contains(path, "watch=true") {
			watches++
		}
	}
	require.True(t, continuations["/api/v1/namespaces/workloads/pods"])
	require.True(t, continuations["/apis/model.aibrix.ai/v1alpha1/namespaces/workloads/modeladapters"])
	require.GreaterOrEqual(t, watches, 4, "required sources must prove actual watch access")
}

func TestInstallationFailsOnDeniedRequiredVerb(t *testing.T) {
	for _, resource := range []string{"pods", "modeladapters"} {
		for _, verb := range []string{"list", "watch"} {
			t.Run(resource+"/"+verb, func(t *testing.T) {
				api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if strings.HasSuffix(r.URL.Path, "/"+resource) && (r.URL.Query().Get("watch") == "true") == (verb == "watch") {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusForbidden)
						_ = json.NewEncoder(w).Encode(&metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: "Failure", Reason: metav1.StatusReasonForbidden, Code: 403})
						return
					}
					installationAPI(w, r)
				}))
				defer api.Close()
				p, err := NewKubernetesProviderForNamespace(&rest.Config{Host: api.URL}, "workloads")
				require.NoError(t, err)
				stop := make(chan struct{})
				defer close(stop)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				require.ErrorContains(t, p.WatchContext(ctx, func(WatchEvent) bool { return true }, stop), verb)
			})
		}
	}
}

func TestInstallationMissingOrTerminatingNamespaceFails(t *testing.T) {
	for _, state := range []string{"missing", "terminating"} {
		t.Run(state, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/namespaces/workloads" {
					installationAPI(w, r)
					return
				}
				if state == "missing" {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(&v1.Namespace{TypeMeta: metav1.TypeMeta{Kind: "Namespace", APIVersion: "v1"}, ObjectMeta: metav1.ObjectMeta{Name: "workloads"}, Status: v1.NamespaceStatus{Phase: v1.NamespaceTerminating}})
			}))
			defer api.Close()
			p, err := NewKubernetesProviderForNamespace(&rest.Config{Host: api.URL}, "workloads")
			require.NoError(t, err)
			stop := make(chan struct{})
			defer close(stop)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			require.Error(t, p.WatchContext(ctx, func(WatchEvent) bool { return true }, stop))
		})
	}
}

func TestInstallationForeignEventGuard(t *testing.T) {
	p, err := NewKubernetesProviderForNamespace(&rest.Config{Host: "http://127.0.0.1:1"}, "workloads")
	require.NoError(t, err)
	inside := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "workloads", Name: "same"}}
	foreign := inside.DeepCopy()
	foreign.Namespace = "foreign"
	for _, event := range []WatchEvent{
		{Type: EventAdd, Object: foreign}, {Type: EventDelete, Object: foreign},
		{Type: EventDelete, Object: cache.DeletedFinalStateUnknown{Obj: foreign}},
		{Type: EventDelete, Object: &cache.DeletedFinalStateUnknown{Obj: foreign}},
		{Type: EventUpdate, Object: inside, OldObject: foreign},
		{Type: EventUpdate, Object: foreign, OldObject: inside},
	} {
		require.False(t, p.acceptEvent(event))
	}
	require.True(t, p.acceptEvent(WatchEvent{Type: EventAdd, Object: inside}))
	for _, obj := range []interface{}{
		&modelv1alpha1.ModelAdapter{ObjectMeta: foreign.ObjectMeta},
		&modelv1alpha1.ModelClaim{ObjectMeta: foreign.ObjectMeta},
		(*v1.Pod)(nil),
	} {
		require.False(t, p.acceptEvent(WatchEvent{Type: EventAdd, Object: obj}))
	}
}

func TestInstallationDeadlineCancelsInformerList(t *testing.T) {
	listStarted := make(chan struct{})
	listCanceled := make(chan struct{})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Access preflight succeeds; the subsequent informer LIST hangs.
		if strings.HasSuffix(r.URL.Path, "/pods") && r.URL.Query().Get("watch") != "true" && r.URL.Query().Get("limit") != "1" {
			close(listStarted)
			<-r.Context().Done()
			close(listCanceled)
			return
		}
		installationAPI(w, r)
	}))
	defer api.Close()
	p, err := NewKubernetesProviderForNamespace(&rest.Config{Host: api.URL}, "workloads")
	require.NoError(t, err)
	p.WithModelAdapters(false)
	stop := make(chan struct{})
	defer close(stop)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, p.WatchContext(ctx, func(WatchEvent) bool { return true }, stop), context.DeadlineExceeded)
	select {
	case <-listStarted:
	default:
		t.Fatal("test did not reach the informer LIST")
	}
	select {
	case <-listCanceled:
	case <-time.After(time.Second):
		t.Fatal("startup failure did not cancel the in-flight informer request")
	}
}

func TestInstallationStopCancelsInitialSynchronization(t *testing.T) {
	listStarted := make(chan struct{})
	listCanceled := make(chan struct{})
	var startedOnce, canceledOnce sync.Once
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/pods") && r.URL.Query().Get("watch") != "true" && r.URL.Query().Get("limit") != "1" {
			startedOnce.Do(func() { close(listStarted) })
			<-r.Context().Done()
			canceledOnce.Do(func() { close(listCanceled) })
			return
		}
		installationAPI(w, r)
	}))
	defer api.Close()
	p, err := NewKubernetesProviderForNamespace(&rest.Config{Host: api.URL}, "workloads")
	require.NoError(t, err)
	p.WithModelAdapters(false)
	stop := make(chan struct{})
	var stopOnce sync.Once
	stopDiscovery := func() { stopOnce.Do(func() { close(stop) }) }
	defer stopDiscovery()
	// There is no startup deadline or parent cancellation before the assertions.
	// The cancel function only prevents a leaked goroutine if this test fails.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- p.WatchContext(ctx, func(WatchEvent) bool { return true }, stop) }()
	select {
	case <-listStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("test did not reach the informer LIST")
	}
	stopDiscovery()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("closing stopCh did not interrupt initial synchronization")
	}
	select {
	case <-listCanceled:
	case <-time.After(time.Second):
		t.Fatal("closing stopCh did not cancel the in-flight informer LIST")
	}
}
