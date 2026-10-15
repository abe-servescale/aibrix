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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

// installationAPI also paginates informer and health LISTs.
func installationAPI(w http.ResponseWriter, r *http.Request) {
	meta := metav1.ListMeta{ResourceVersion: "7"}
	if r.URL.Query().Get("limit") == "500" && r.URL.Query().Get("continue") == "" {
		meta.Continue = "page-two"
	}
	namespaceListReply(w, r, meta)
}

func TestInstallationCancellationDuringInformerList(t *testing.T) {
	for _, mode := range []string{"deadline", "stop-channel"} {
		t.Run(mode, func(t *testing.T) {
			entered, canceled := make(chan struct{}), make(chan struct{})
			var enteredOnce, canceledOnce sync.Once
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Access preflight succeeds; the subsequent informer LIST hangs.
				if strings.HasSuffix(r.URL.Path, "/pods") && r.URL.Query().Get("watch") != "true" && r.URL.Query().Get("limit") != "1" {
					enteredOnce.Do(func() { close(entered) })
					<-r.Context().Done()
					canceledOnce.Do(func() { close(canceled) })
					return
				}
				installationAPI(w, r)
			}))
			defer api.Close()
			p, err := NewKubernetesProviderForNamespace(&rest.Config{Host: api.URL}, "managed")
			require.NoError(t, err)
			p.WithModelAdapters(false)
			lifetime, stop := context.WithCancel(context.Background())
			defer func() { stop(); p.Shutdown() }()
			startup, cancel := context.WithCancel(context.Background())
			defer cancel()
			wantErr := context.Canceled
			if mode == "deadline" {
				var deadlineCancel context.CancelFunc
				startup, deadlineCancel = context.WithTimeout(startup, 500*time.Millisecond)
				defer deadlineCancel()
				wantErr = context.DeadlineExceeded
			}
			result := make(chan error, 1)
			go func() { result <- p.WatchContext(startup, func(WatchEvent) bool { return true }, lifetime.Done()) }()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("test did not reach the informer LIST")
			}
			if mode == "stop-channel" {
				stop() // No startup deadline or parent cancellation in this case.
			}
			select {
			case err := <-result:
				require.ErrorIs(t, err, wantErr)
			case <-time.After(2 * time.Second):
				t.Fatal("cancellation did not interrupt initial synchronization")
			}
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("in-flight informer LIST was not canceled")
			}
		})
	}
}
