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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type publicationDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c publicationDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }

func TestGatewayPublicationRejectsDeadlineWithoutTimerDelivery(t *testing.T) {
	gate := &ModelListGate{}
	ctx := publicationDeadlineContext{Context: context.Background(), deadline: time.Now().Add(-time.Second)}
	require.Nil(t, ctx.Err(), "controlled timer has not reported expiration")
	ready := false
	err := gate.Publish(ctx, context.Background(), &Server{}, func(value bool) { ready = value })
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, ready)
	require.False(t, gate.Published())
}

func TestGatewayPublicationRollsBackConcurrentStartupCancellation(t *testing.T) {
	gate := &ModelListGate{}
	startup, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := false
	err := gate.Publish(startup, context.Background(), &Server{}, func(value bool) {
		ready = value
		if value {
			cancel()
		}
	})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, ready)
	require.False(t, gate.Published())
	require.Nil(t, gate.server)
	recorder := httptest.NewRecorder()
	gate.handleListModels(recorder, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
}

func TestGatewayDisableClearsReadinessAndPreventsRepublishing(t *testing.T) {
	gate := &ModelListGate{}
	ready := false
	setter := func(value bool) { ready = value }
	require.NoError(t, gate.Publish(context.Background(), context.Background(), &Server{}, setter))
	require.True(t, ready)
	require.True(t, gate.Published())
	gate.Disable()
	require.False(t, ready)
	require.False(t, gate.Published())
	require.Error(t, gate.Publish(context.Background(), context.Background(), &Server{}, setter))
}
