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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/cache/discovery"
	"github.com/vllm-project/aibrix/pkg/constants"
	"github.com/vllm-project/aibrix/pkg/types"
	"github.com/vllm-project/aibrix/pkg/utils"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

func lifecyclePod() *v1.Pod {
	return &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "served", Labels: map[string]string{constants.ModelLabelName: "org/model"}}}
}

func modelsDiscoveryPath(requestPath string) bool {
	switch requestPath {
	case "/api/v1/namespaces/models",
		"/api/v1/namespaces/models/pods",
		"/apis/model.aibrix.ai/v1alpha1/namespaces/models/modeladapters",
		"/apis/model.aibrix.ai/v1alpha1/namespaces/models/modelclaims":
		return true
	default:
		return false
	}
}

func TestNamespacedStoreFixtureRejectsForeignPrefixPaths(t *testing.T) {
	for _, requestPath := range []string{
		"/api/v1/namespaces/models-foreign",
		"/api/v1/namespaces/models-foreign/pods",
		"/apis/model.aibrix.ai/v1alpha1/namespaces/models-foreign/modeladapters",
		"/apis/model.aibrix.ai/v1alpha1/namespaces/models-foreign/modelclaims",
		"/api/v1/pods",
		"/api/v1/namespaces/models/secrets",
	} {
		t.Run(requestPath, func(t *testing.T) {
			require.False(t, modelsDiscoveryPath(requestPath))
		})
	}
}

func TestGatewayReconciliationCancelsHeldMembershipLocks(t *testing.T) {
	for _, resource := range []string{"pod", "claim"} {
		t.Run(resource, func(t *testing.T) {
			st := NewForTest()
			if resource == "pod" {
				st.mu.Lock()
				defer st.mu.Unlock()
			} else {
				st.modelClaims.mu.Lock()
				defer st.modelClaims.mu.Unlock()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				event := discovery.WatchEvent{Type: discovery.EventAdd, Object: lifecyclePod()}
				if resource == "claim" {
					event.Object = &modelv1alpha1.ModelClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "pending"}}
				}
				_, err := handleDiscoveryObjectContext(ctx, st, event)
				done <- err
			}()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case <-time.After(time.Second):
				t.Fatal("cancellation waited for the held lock to be released")
			}
			require.Equal(t, 0, st.metaPods.Len())
		})
	}
}

func TestGatewayRouterPreparationRunsOutsideMembershipLock(t *testing.T) {
	st := NewForTest()
	st.lifetimeCtx = context.Background()
	calls := 0
	st.contextRouterProvider = func(ctx, _ context.Context, candidate *Store, _ string) (types.QueueRouter, error) {
		require.True(t, candidate.mu.TryLock(), "router construction must not hold the store lock")
		candidate.mu.Unlock()
		require.Empty(t, candidate.ListModels())
		calls++
		return nil, ctx.Err()
	}
	require.NoError(t, st.addPodContext(context.Background(), lifecyclePod()))
	require.Equal(t, 1, calls)
	require.Equal(t, []string{"org/model"}, st.ListModels())
	require.NoError(t, st.addPodContext(context.Background(), lifecyclePod()))
	require.Equal(t, 1, calls)
	st.Close()
}

func TestGatewayInitializerDeadlineCancelsRequestWithoutPublishing(t *testing.T) {
	previous, published := store, gatewayPublished.Load()
	store = &Store{}
	gatewayPublished.Store(nil)
	t.Cleanup(func() { store = previous; gatewayPublished.Store(published) })
	entered, stopped := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(stopped)
	}))
	defer server.Close()
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	startup, stop := context.WithTimeout(lifetime, 5*time.Second)
	defer stop()
	done := make(chan error, 1)
	go func() {
		_, err := InitGatewayWithOptions(startup, lifetime, &rest.Config{Host: server.URL}, InitOptions{IsGateway: true, InstallationNamespace: "models", DisableModelClaimPodBindings: true})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(6 * time.Second):
		t.Fatal("Namespace request not observed")
	}
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorContains(t, err, "models")
	case <-time.After(6 * time.Second):
		t.Fatal("initializer abandoned blocked request")
	}
	select {
	case <-stopped:
	case <-time.After(6 * time.Second):
		t.Fatal("HTTP request was not canceled")
	}
	require.Nil(t, gatewayPublished.Load())
	_, err := Get()
	require.Error(t, err)
}

func TestGatewaySuccessfulStartupKeepsDiscoveryAndRejectsStoppedReuse(t *testing.T) {
	previous, published := store, gatewayPublished.Load()
	store = &Store{}
	gatewayPublished.Store(nil)
	t.Cleanup(func() { store = previous; gatewayPublished.Store(published) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !modelsDiscoveryPath(r.URL.Path) {
			t.Errorf("unexpected discovery path: %s", r.URL.Path)
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
		if r.URL.Path == "/api/v1/namespaces/models" {
			_ = json.NewEncoder(w).Encode(&v1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metav1.ObjectMeta{Name: "models"}})
		} else if strings.HasSuffix(r.URL.Path, "/pods") {
			_ = json.NewEncoder(w).Encode(&v1.PodList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"}})
		} else {
			_ = json.NewEncoder(w).Encode(&modelv1alpha1.ModelAdapterList{TypeMeta: metav1.TypeMeta{APIVersion: "model.aibrix.ai/v1alpha1", Kind: "ModelAdapterList"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"}})
		}
	}))
	defer server.Close()
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	startup, stop := context.WithTimeout(lifetime, 5*time.Second)
	defer stop()
	opts := InitOptions{IsGateway: true, InstallationNamespace: "models", DisableModelClaimPodBindings: true, ModelListMaxAge: time.Minute}
	st, err := InitGatewayWithOptions(startup, lifetime, &rest.Config{Host: server.URL}, opts)
	require.NoError(t, err)
	defer st.Close()
	stop()
	_, err = Get()
	require.NoError(t, err)
	require.NoError(t, st.modelListHealth.EnsureVerified(lifetime))
	nextStartup, nextStop := context.WithTimeout(lifetime, time.Second)
	defer nextStop()
	reused, err := InitGatewayWithOptions(nextStartup, lifetime, &rest.Config{Host: server.URL}, opts)
	require.NoError(t, err)
	require.Same(t, st, reused)
	st.Close()
	_, err = Get()
	require.Error(t, err)
	_, err = InitGatewayWithOptions(nextStartup, lifetime, &rest.Config{Host: server.URL}, opts)
	require.Error(t, err)
}

func TestModelSnapshotReadCancelsWithStoreLockHeld(t *testing.T) {
	st := NewForTest()
	st.mu.Lock()
	defer st.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := st.ListModelsVerified(ctx, true); done <- err }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("snapshot read waited for held Store lock")
	}
}

func TestGatewayTraceFlushUsesPublishedOwningStore(t *testing.T) {
	previous, published := store, gatewayPublished.Load()
	store = &Store{}
	gatewayPublished.Store(nil)
	t.Cleanup(func() { store = previous; gatewayPublished.Store(published) })
	client := newTestRunningRequestsClient(t)
	owner := NewForTest()
	owner.installationNamespace = "models"
	owner.lifetimeCtx = context.Background()
	owner.redisClient = client
	owner.enableTracing = true
	owner.requestTrace = &utils.SyncMap[string, *RequestTrace]{}
	gatewayPublished.Store(owner)
	active, err := Get()
	require.NoError(t, err)
	_ = active.AddRequestCount(nil, "trace-request", "org/model")
	require.Zero(t, store.numRequestsTraces, "legacy global store is not the gateway")
	now := time.Now()
	owner.flushTraceTick(context.Background(), now)
	round := now.Unix() - now.Unix()%int64(RequestTraceWriteInterval/time.Second)
	key := fmt.Sprintf("aibrix:%s_request_trace_%d", "org/model", round)
	value, err := client.Get(context.Background(), key).Result()
	require.NoError(t, err)
	require.NotEmpty(t, value)
}
