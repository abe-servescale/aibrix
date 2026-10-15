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

package main

import (
	"context"

	"google.golang.org/grpc/health"
	healthPb "google.golang.org/grpc/health/grpc_health_v1"
)

// lifetimeHealthServer refuses readiness immediately after shutdown, including
// the interval before the signal-triggered cleanup goroutine updates health.
type lifetimeHealthServer struct {
	*health.Server
	lifetime context.Context
	ready    func() bool
}

func (s *lifetimeHealthServer) Check(
	ctx context.Context, req *healthPb.HealthCheckRequest,
) (*healthPb.HealthCheckResponse, error) {
	if s.lifetime.Err() != nil || (s.ready != nil && !s.ready()) {
		return &healthPb.HealthCheckResponse{Status: healthPb.HealthCheckResponse_NOT_SERVING}, nil
	}
	response, err := s.Server.Check(ctx, req)
	if s.lifetime.Err() != nil || (s.ready != nil && !s.ready()) {
		return &healthPb.HealthCheckResponse{Status: healthPb.HealthCheckResponse_NOT_SERVING}, nil
	}
	return response, err
}

// Watch applies the same gate to streamed health replies and ends the stream
// when the gateway lifetime ends.
func (s *lifetimeHealthServer) Watch(req *healthPb.HealthCheckRequest, stream healthPb.Health_WatchServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	stop := context.AfterFunc(s.lifetime, cancel)
	defer func() { stop(); cancel() }()
	return s.Server.Watch(req, &lifetimeHealthStream{Health_WatchServer: stream, ctx: ctx, owner: s})
}

type lifetimeHealthStream struct {
	healthPb.Health_WatchServer
	ctx   context.Context
	owner *lifetimeHealthServer
}

func (s *lifetimeHealthStream) Context() context.Context { return s.ctx }
func (s *lifetimeHealthStream) Send(response *healthPb.HealthCheckResponse) error {
	if s.owner.lifetime.Err() != nil || (s.owner.ready != nil && !s.owner.ready()) {
		response = &healthPb.HealthCheckResponse{Status: healthPb.HealthCheckResponse_NOT_SERVING}
	}
	return s.Health_WatchServer.Send(response)
}
