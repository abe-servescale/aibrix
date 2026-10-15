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
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vllm-project/aibrix/pkg/cache/discovery"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

func TestInstallationSingletonUsesActualDiscoveryScope(t *testing.T) {
	const child = "AIBRIX_TEST_SINGLETON_SCOPE"
	if mode := os.Getenv(child); mode != "" {
		checkInstallationSingletonScope(t, mode)
		return
	}
	// Each child exercises the production singleton from a fresh process.
	for _, mode := range []string{"legacy", "scoped"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestInstallationSingletonUsesActualDiscoveryScope$")
			command.Env = append(os.Environ(), child+"="+mode)
			output, err := command.CombinedOutput()
			require.NoError(t, err, "%s", output)
		})
	}
}

func checkInstallationSingletonScope(t *testing.T, mode string) {
	t.Setenv("PROMETHEUS_BASIC_AUTH_SECRET_NAME", "")
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("watch") == "true" {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		if r.URL.Path == "/api/v1/namespaces/workloads" {
			_ = json.NewEncoder(w).Encode(&v1.Namespace{
				TypeMeta:   metav1.TypeMeta{Kind: "Namespace", APIVersion: "v1"},
				ObjectMeta: metav1.ObjectMeta{Name: "workloads"},
			})
			return
		}
		if !strings.HasSuffix(r.URL.Path, "/pods") {
			http.NotFound(w, r)
			return
		}
		pods := []v1.Pod{*testPod("owned", "workloads", "owned-model", 1)}
		if r.URL.Path == "/api/v1/pods" {
			pods = append(pods, *testPod("foreign", "foreign", "foreign-model", 2))
		}
		_ = json.NewEncoder(w).Encode(&v1.PodList{
			TypeMeta: metav1.TypeMeta{Kind: "PodList", APIVersion: "v1"},
			ListMeta: metav1.ListMeta{ResourceVersion: "7"}, Items: pods,
		})
	}))
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop); api.CloseClientConnections(); api.Close() })
	config := &rest.Config{Host: api.URL}
	provider := discovery.NewKubernetesProvider(config).WithModelAdapters(false)
	firstNamespace := "workloads"
	if mode == "scoped" {
		var err error
		provider, err = discovery.NewKubernetesProviderForNamespace(config, "workloads")
		require.NoError(t, err)
		provider.WithModelAdapters(false)
		// A misleading caller option must not replace the provider's real scope.
		firstNamespace = "foreign"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := InitWithContext(ctx, config, stop, InitOptions{
		InstallationNamespace: firstNamespace, DiscoveryProvider: provider,
	})
	require.NoError(t, err)
	gatewayProvider, err := discovery.NewKubernetesProviderForNamespace(config, "workloads")
	require.NoError(t, err)
	gatewayProvider.WithModelAdapters(false)
	second, err := InitWithContext(ctx, config, stop, InitOptions{
		IsGateway: true, InstallationNamespace: "workloads", DiscoveryProvider: gatewayProvider,
	})
	if mode == "legacy" {
		require.Contains(t, first.ListModels(), "foreign-model", "the first cache really is cluster-wide")
		require.ErrorContains(t, err, "unconfined")
		require.Nil(t, second)
	} else {
		require.Equal(t, []string{"owned-model"}, first.ListModels())
		require.ErrorContains(t, err, "cannot reuse")
		require.Nil(t, second)
	}
}
