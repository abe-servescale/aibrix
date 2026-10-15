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

package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/cache/discovery"
	"github.com/vllm-project/aibrix/pkg/constants"
	routing "github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms"
	"github.com/vllm-project/aibrix/pkg/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

// Production cache initialization is a process singleton. Run it in a child
// process so other gateway tests cannot substitute an already populated cache.
func TestInstallationDiscoveryRoutingAndModelList(t *testing.T) {
	const child = "AIBRIX_TEST_INSTALLATION_DISCOVERY"
	if os.Getenv(child) != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestInstallationDiscoveryRoutingAndModelList$")
		command.Env = append(os.Environ(), child+"=1")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}
	checkInstallationDiscovery(t)
}

func checkInstallationDiscovery(t *testing.T) {
	t.Setenv("PROMETHEUS_BASIC_AUTH_SECRET_NAME", "")
	var pods []v1.Pod
	models := []string{"label-model", "/Models/Exact Name", "label-wins", "annotation-fallback"}
	for i, model := range models {
		pod := v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: modelPodName(i), Namespace: "workloads", ResourceVersion: "7",
				Labels: map[string]string{constants.ModelLabelName: model},
			},
			Status: v1.PodStatus{
				PodIP: "10.0.0.1", Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}},
			},
		}
		switch i {
		case 1:
			pod.Labels = nil
			pod.Annotations = map[string]string{constants.ModelLabelName: model}
		case 2:
			pod.Annotations = map[string]string{constants.ModelLabelName: "annotation-loses"}
		case 3:
			pod.Labels[constants.ModelLabelName] = ""
			pod.Annotations = map[string]string{constants.ModelLabelName: model}
		}
		pods = append(pods, pod)
		foreign := *pod.DeepCopy()
		foreign.Namespace, foreign.Status.PodIP = "foreign", "10.99.0.1"
		pods = append(pods, foreign)
	}
	foreignOnly := *pods[1].DeepCopy()
	foreignOnly.Name = "foreign-only"
	foreignOnly.Labels[constants.ModelLabelName] = "foreign-only"
	pods = append(pods, foreignOnly)
	adapters := []modelv1alpha1.ModelAdapter{
		{ObjectMeta: metav1.ObjectMeta{Name: "adapter", Namespace: "workloads"},
			Status: modelv1alpha1.ModelAdapterStatus{Instances: []string{modelPodName(0)}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "foreign-adapter", Namespace: "foreign"},
			Status: modelv1alpha1.ModelAdapterStatus{Instances: []string{modelPodName(0)}}},
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/namespaces/workloads") {
			t.Errorf("unscoped request: %s", r.URL.Path)
			http.Error(w, "unscoped request", http.StatusForbidden)
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
		case r.URL.Path == "/api/v1/namespaces/workloads":
			_ = json.NewEncoder(w).Encode(&v1.Namespace{
				TypeMeta:   metav1.TypeMeta{Kind: "Namespace", APIVersion: "v1"},
				ObjectMeta: metav1.ObjectMeta{Name: "workloads"},
			})
		case strings.HasSuffix(r.URL.Path, "/pods"):
			// Include foreign objects deliberately to exercise the defensive event
			// guard as well as checking that the request itself is namespaced.
			_ = json.NewEncoder(w).Encode(&v1.PodList{
				TypeMeta: metav1.TypeMeta{Kind: "PodList", APIVersion: "v1"},
				ListMeta: metav1.ListMeta{ResourceVersion: "7"}, Items: pods,
			})
		case strings.HasSuffix(r.URL.Path, "/modeladapters"):
			_ = json.NewEncoder(w).Encode(&modelv1alpha1.ModelAdapterList{
				TypeMeta: metav1.TypeMeta{Kind: "ModelAdapterList", APIVersion: "model.aibrix.ai/v1alpha1"},
				ListMeta: metav1.ListMeta{ResourceVersion: "7"}, Items: adapters,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop); api.CloseClientConnections(); api.Close() })
	config := &rest.Config{Host: api.URL}
	provider, err := discovery.NewKubernetesProviderForNamespace(config, "workloads")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := cache.InitWithContext(ctx, config, stop, cache.InitOptions{
		IsGateway: true, InstallationNamespace: "workloads", DiscoveryProvider: provider,
		DisableModelClaimPodBindings: true,
	})
	require.NoError(t, err)
	models = append(models, "adapter")
	require.ElementsMatch(t, models, store.ListModels())
	server := NewServerWithOptions(nil, nil, nil, ServerOptions{
		Cache: store, RouterManager: routing.NewRouterManagerWithCache(store),
	})
	for _, model := range models {
		candidates, err := store.ListPodsByModel(model)
		require.NoError(t, err, model)
		require.Equal(t, 1, candidates.Len(), model)
		require.Equal(t, "workloads", candidates.All()[0].Namespace)
		route := types.NewRoutingContext(context.Background(), routing.RouterRandom, model, "", "request", "")
		address, err := server.selectTargetPod(context.Background(), route, candidates, "")
		require.NoError(t, err, model)
		require.True(t, strings.HasPrefix(address, "10.0.0.1:"), address)
	}
	for _, mode := range []ModelListMode{ModelListKnown, ModelListReadyPods} {
		server := NewServerWithOptions(nil, nil, nil, ServerOptions{Cache: store, ModelListMode: mode})
		response := httptest.NewRecorder()
		server.handleListModels(response, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
		require.Equal(t, http.StatusOK, response.Code)
		var result struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		ids := make([]string, 0, len(result.Data))
		for _, model := range result.Data {
			ids = append(ids, model.ID)
		}
		require.ElementsMatch(t, models, ids)
	}
}

func modelPodName(i int) string { return fmt.Sprintf("pod-%d", i) }
