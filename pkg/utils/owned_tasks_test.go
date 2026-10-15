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

package utils

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOwnedDetachedHTTPStopsWithGateway(t *testing.T) {
	entered, canceled := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(entered)
		<-request.Context().Done()
		close(canceled)
	}))
	defer server.Close()
	owner := NewOwnedTasks(context.Background())
	defer owner.Close()
	clientCtx, cancelClient := context.WithCancel(context.Background())
	defer cancelClient()
	parent := context.WithoutCancel(WithTaskOwner(clientCtx, owner))
	result := make(chan error, 1)
	require.True(t, GoWithOwner(parent, func(ctx context.Context) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
		if err == nil {
			_, err = server.Client().Do(request)
		}
		result <- err
	}))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("HTTP operation did not start")
	}
	cancelClient()
	select {
	case <-canceled:
		t.Fatal("client cancellation stopped detached work")
	default:
	}
	done := make(chan struct{})
	go func() { owner.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("owner did not cancel and await HTTP")
	}
	require.ErrorIs(t, <-result, context.Canceled)
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("HTTP request remains active")
	}
	require.False(t, owner.Start(func() { t.Error("closed owner admitted work") }))
}
