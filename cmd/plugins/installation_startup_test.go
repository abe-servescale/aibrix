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
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestInstallationStartupOptions(t *testing.T) {
	kube := kubeAPIOptions{qps: float64(rest.DefaultQPS), burst: rest.DefaultBurst}
	models := modelDiscoveryOptions{listMode: "known"}
	for _, namespace := range []string{"", " ", "Workloads", " workloads", "workloads ", "*", "one,two"} {
		models.namespace = namespace
		require.Error(t, validateStartupOptions(kube, models, false, ""), namespace)
	}
	models.namespace = "workloads"
	require.NoError(t, validateStartupOptions(kube, models, false, ""))
	models.namespace = ""
	require.NoError(t, validateStartupOptions(kube, models, true, "endpoints.yaml"))
}

// Run the real entry point to prove that a blocked API request cannot publish
// an empty successful list or leave the process starting forever.
func TestInstallationStartupDeadline(t *testing.T) {
	const child = "AIBRIX_TEST_STARTUP_DEADLINE"
	if os.Getenv(child) == "1" {
		os.Args = []string{os.Args[0],
			"--installation-namespace=workloads", "--kubeconfig=" + os.Getenv("AIBRIX_TEST_KUBECONFIG"),
			"--http-bind-address=" + os.Getenv("AIBRIX_TEST_HTTP_ADDRESS"),
			"--grpc-bind-address=127.0.0.1:0", "--pprof-bind-address=",
		}
		main()
		return
	}
	for _, scenario := range []struct {
		name   string
		signal os.Signal
	}{
		{name: "deadline"}, {name: "SIGINT", signal: syscall.SIGINT}, {name: "SIGTERM", signal: syscall.SIGTERM},
	} {
		t.Run(scenario.name, func(t *testing.T) { testInstallationStartupExit(t, scenario.signal) })
	}
}

func testInstallationStartupExit(t *testing.T, stopSignal os.Signal) {
	const child = "AIBRIX_TEST_STARTUP_DEADLINE"
	redis := miniredis.RunT(t)
	api := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer api.Close()
	kubeconfig := filepath.Join(t.TempDir(), "config")
	require.NoError(t, clientcmd.WriteToFile(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"test": {Server: api.URL}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"test": {}},
		Contexts:       map[string]*clientcmdapi.Context{"test": {Cluster: "test", AuthInfo: "test"}},
		CurrentContext: "test",
	}, kubeconfig))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestInstallationStartupDeadline$")
	command.Env = append(os.Environ(), child+"=1", "AIBRIX_TEST_KUBECONFIG="+kubeconfig,
		"AIBRIX_TEST_HTTP_ADDRESS="+address, "REDIS_HOST="+redis.Host(), "REDIS_PORT="+redis.Port(),
		"REDIS_PASSWORD=", "PROMETHEUS_BASIC_AUTH_SECRET_NAME=")
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	started := time.Now()
	require.NoError(t, command.Start())
	finished := make(chan error, 1)
	go func() { finished <- command.Wait() }()
	httpClient := &http.Client{Timeout: time.Second}
	require.Eventually(t, func() bool {
		response, err := httpClient.Get("http://" + address + "/v1/models")
		if err != nil {
			return false
		}
		defer func() { _ = response.Body.Close() }()
		body, err := io.ReadAll(response.Body)
		return err == nil && response.StatusCode == http.StatusServiceUnavailable &&
			bytes.Contains(body, []byte("service_unavailable"))
	}, 10*time.Second, 50*time.Millisecond)
	signalSent := time.Now()
	if stopSignal != nil {
		require.NoError(t, command.Process.Signal(stopSignal))
	}
	select {
	case err := <-finished:
		require.Error(t, err, "%s", output.String())
	case <-ctx.Done():
		t.Fatal("gateway did not exit after its startup deadline")
	}
	require.Less(t, time.Since(started), 40*time.Second)
	require.Contains(t, output.String(), "namespace")
	require.Contains(t, output.String(), "workloads")
	if stopSignal != nil {
		require.Less(t, time.Since(signalSent), 5*time.Second)
		require.True(t, bytes.Contains(output.Bytes(), []byte("context canceled")) ||
			bytes.Contains(output.Bytes(), []byte("signal received")), "%s", output.String())
		return
	}
	deadlineReported := bytes.Contains(output.Bytes(), []byte("30s")) ||
		bytes.Contains(output.Bytes(), []byte("deadline exceeded"))
	require.True(t, deadlineReported, "%s", output.String())
}
