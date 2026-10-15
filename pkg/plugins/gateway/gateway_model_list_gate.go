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
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vllm-project/aibrix/pkg/utils"

	"k8s.io/klog/v2"
)

// ModelListGate serves a 503 while the gateway cache is initially syncing,
// then delegates to the normal model-list handler. It is used only when
// discovery verification is explicitly enabled.
type ModelListGate struct {
	mu         sync.RWMutex
	server     *Server
	httpServer *http.Server
	lifetime   context.Context
	closed     bool
	ready      func(bool)
	published  atomic.Bool
}

// SetServer publishes the ready gateway handler after cache initialization.
func (g *ModelListGate) SetServer(server *Server) {
	g.mu.Lock()
	g.server = server
	g.mu.Unlock()
}

// StartHTTPServer opens the model-list and metrics listener before discovery.
func (g *ModelListGate) StartHTTPServer(addr string) error {
	if g.httpServer != nil {
		return nil
	}
	server, err := startModelListHTTPServer(addr, g.handleListModels)
	if err != nil {
		return err
	}
	g.httpServer = server
	return nil
}

func (g *ModelListGate) handleListModels(w http.ResponseWriter, r *http.Request) {
	g.mu.RLock()
	server := g.server
	g.mu.RUnlock()
	if server != nil && !g.unavailable() {
		server.handleListModels(w, r)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, `{"error":"method not allowed"}`)
		return
	}
	writeModelListUnavailable(w)
}

func writeModelListUnavailable(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = io.WriteString(w, generateErrorMessageWithHTTPCode(
		"model discovery is unavailable", http.StatusServiceUnavailable,
		ErrorCodeServiceUnavailable, ""))
}

// Shutdown stops the early listener after gateway shutdown.
func (g *ModelListGate) Shutdown() {
	if g == nil {
		return
	}
	g.Disable()
	if g.httpServer == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := g.httpServer.Shutdown(ctx); err != nil {
		klog.ErrorS(err, "Error stopping model-list HTTP server")
	}
}

// Publish installs the initialized handler and readiness together while holding
// the same state lock as Disable. Both contexts and the original deadline must
// still permit publication. The readiness callback must not perform I/O.
func (g *ModelListGate) Publish(startup, lifetime context.Context, server *Server, ready func(bool)) error {
	if err := utils.LockContext(startup, g.mu.TryLock, g.mu.Unlock); err != nil {
		return err
	}
	defer g.mu.Unlock()
	if err := publicationError(startup, lifetime); err != nil {
		return err
	}
	if g.closed {
		return fmt.Errorf("gateway publication is closed")
	}
	if server == nil || ready == nil {
		return fmt.Errorf("gateway publication requires a server and readiness setter")
	}
	g.lifetime, g.server, g.ready = lifetime, server, ready
	ready(true)
	if err := publicationError(startup, lifetime); err != nil {
		g.rollbackPublication()
		return err
	}
	g.published.Store(true)
	if err := publicationError(startup, lifetime); err != nil {
		g.rollbackPublication()
		return err
	}
	return nil
}

func publicationError(startup, lifetime context.Context) error {
	if err := startup.Err(); err != nil {
		return err
	}
	if deadline, ok := startup.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return lifetime.Err()
}

func (g *ModelListGate) rollbackPublication() {
	g.published.Store(false)
	g.server = nil
	g.closed = true
	g.ready(false)
}

// Disable closes the handler gate before discovery or server cleanup starts.
func (g *ModelListGate) Disable() {
	g.mu.Lock()
	g.published.Store(false)
	g.closed = true
	g.server = nil
	if g.ready != nil {
		g.ready(false)
	}
	g.mu.Unlock()
}

func (g *ModelListGate) unavailable() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.closed || (g.lifetime != nil && g.lifetime.Err() != nil)
}

// Published lets the startup timer avoid canceling a successfully published
// lifetime when its callback was already queued at the deadline.
func (g *ModelListGate) Published() bool { return g != nil && g.published.Load() }
