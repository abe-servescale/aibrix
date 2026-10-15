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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/cache/discovery"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

func TestPostSyncReconciliationHandlesActualSignals(t *testing.T) {
	for _, signal := range []os.Signal{syscall.SIGINT, syscall.SIGTERM} {
		t.Run(signal.String(), func(t *testing.T) {
			executable, err := os.Executable()
			require.NoError(t, err)
			ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			command := exec.CommandContext(ctx, executable, "-test.run=^TestPostSyncSignalHelper$")
			command.Env = append(os.Environ(), "AIBRIX_POST_SYNC_SIGNAL_HELPER=true")
			stdout, err := command.StdoutPipe()
			require.NoError(t, err)
			var errors bytes.Buffer
			command.Stderr = &errors
			require.NoError(t, command.Start())
			lines := make(chan string, 1)
			readDone := make(chan struct{})
			var output bytes.Buffer
			go func() {
				defer close(readDone)
				defer close(lines)
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					line := scanner.Text()
					output.WriteString(line + "\n")
					if line == "post-sync reconciliation blocked" {
						lines <- line
					}
				}
			}()
			select {
			case line := <-lines:
				require.Equal(t, "post-sync reconciliation blocked", line)
			case <-ctx.Done():
				t.Fatal("child did not reach post-sync reconciliation")
			}
			require.NoError(t, command.Process.Signal(signal))
			<-readDone
			require.NoError(t, command.Wait(), "%s\n%s", output.String(), errors.String())
			require.Contains(t, output.String(), "post-sync signal cleanup complete")
		})
	}
}

func TestPostSyncSignalHelper(t *testing.T) {
	if os.Getenv("AIBRIX_POST_SYNC_SIGNAL_HELPER") != "true" {
		return
	}
	lifetime, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	startup, stopStartup := context.WithTimeout(lifetime, 30*time.Second)
	defer stopStartup()
	var activeWatches atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("watch") == "true" {
			activeWatches.Add(1)
			defer activeWatches.Add(-1)
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		switch {
		// Fixture response errors after client cancellation do not change the assertion.
		case r.URL.Path == "/api/v1/namespaces/models":
			_ = json.NewEncoder(w).Encode(&corev1.Namespace{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metav1.ObjectMeta{Name: "models"},
			})
		case strings.HasSuffix(r.URL.Path, "/pods"):
			_ = json.NewEncoder(w).Encode(&corev1.PodList{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"},
			})
		default:
			_ = json.NewEncoder(w).Encode(&modelv1alpha1.ModelAdapterList{
				TypeMeta: metav1.TypeMeta{APIVersion: "model.aibrix.ai/v1alpha1", Kind: "ModelAdapterList"},
				ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: []modelv1alpha1.ModelAdapter{{
					ObjectMeta: metav1.ObjectMeta{Name: "adapter", Namespace: "models", UID: "adapter", ResourceVersion: "1"},
				}},
			})
		}
	}))
	defer api.Close()
	provider, err := discovery.NewKubernetesProviderForNamespace(&rest.Config{Host: api.URL}, "models")
	require.NoError(t, err)
	provider.WithModelListHealth(time.Minute)
	st := NewForTest()
	defer st.Close()
	locked := false
	err = provider.WatchNamespaced(startup, lifetime, func(ctx context.Context, event discovery.WatchEvent) (bool, error) {
		if _, adapter := event.Object.(*modelv1alpha1.ModelAdapter); adapter {
			if _, reconciliation := ctx.Deadline(); reconciliation {
				st.mu.Lock()
				locked = true
				_, err := os.Stdout.WriteString("post-sync reconciliation blocked\n")
				if err != nil {
					return false, err
				}
			}
		}
		return handleDiscoveryObjectContext(ctx, st, event)
	})
	if locked {
		defer st.mu.Unlock()
	}
	require.True(t, locked, "the signal must arrive after required informer/handler sync")
	require.ErrorIs(t, err, context.Canceled)
	provider.Shutdown()
	require.Eventually(t, func() bool { return activeWatches.Load() == 0 }, time.Second, time.Millisecond)
	require.False(t, st.mu.TryLock(), "cleanup must finish while the Store blocker is still held")
	_, healthy := provider.ModelListHealth().ModelsIfHealthy(st.ListModels)
	require.False(t, healthy)
	_, err = os.Stdout.WriteString("post-sync signal cleanup complete\n")
	require.NoError(t, err)
}
