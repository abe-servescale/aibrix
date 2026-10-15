/*
Copyright 2025 The Aibrix Team.

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
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof" // registers the profiling handlers that startPprofServer serves
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/filters"
	_ "go.uber.org/automaxprocs"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/cache/discovery"
	"github.com/vllm-project/aibrix/pkg/constants"
	"github.com/vllm-project/aibrix/pkg/plugins/gateway"
	routing "github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms"
	"github.com/vllm-project/aibrix/pkg/plugins/gateway/statesync"
	"github.com/vllm-project/aibrix/pkg/utils"
	"github.com/vllm-project/aibrix/pkg/utils/prefixcacheindexer"
	"google.golang.org/grpc/health"
	healthPb "google.golang.org/grpc/health/grpc_health_v1"
	"sigs.k8s.io/gateway-api/pkg/client/clientset/versioned"
)

const (
	defaultGRPCMaxMessageSizeBytes = 4 * 1024 * 1024
	envGRPCMaxMessageSizeBytes     = "AIBRIX_GRPC_MAX_MESSAGE_SIZE_BYTES"
	envDisableRateLimiting         = "AIBRIX_DISABLE_RATE_LIMITING"
	modelDiscoveryStartupTimeout   = 30 * time.Second
	envPriorityTierEnabled         = "AIBRIX_PRIORITY_TIER_ENABLED"
)

var (
	grpcAddr        string
	httpAddr        string
	metricsAddr     string // deprecated: use httpAddr
	standalone      bool
	endpointsConfig string
	pprofAddr       string
)

type kubeAPIOptions struct {
	qps   float64
	burst int
}

type modelDiscoveryOptions struct {
	namespace          string
	watchModelAdapters bool
	watchModelClaims   bool
	listMode           string
	maxAge             time.Duration
}

func (o *modelDiscoveryOptions) addFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.namespace, "installation-namespace", "",
		"Model workload namespace; required in Kubernetes mode.")
	fs.BoolVar(&o.watchModelAdapters, "watch-model-adapters", true,
		"List and watch ModelAdapter resources in Kubernetes discovery.")
	fs.BoolVar(&o.watchModelClaims, "watch-model-claims", true,
		"List and watch ModelClaim resources and use their Pod bindings in Kubernetes discovery.")
	fs.StringVar(&o.listMode, "model-list-mode", string(gateway.ModelListKnown),
		"Models returned by /v1/models: known or ready-pods.")
	fs.DurationVar(&o.maxAge, "model-list-discovery-max-age", 0,
		"Maximum age of a verified Kubernetes model-list snapshot; 0 disables verification.")
}

func (o modelDiscoveryOptions) validate() error {
	if o.listMode != string(gateway.ModelListKnown) && o.listMode != string(gateway.ModelListReadyPods) {
		return fmt.Errorf("invalid --model-list-mode %q: expected known or ready-pods", o.listMode)
	}
	if o.maxAge < 0 {
		return fmt.Errorf("--model-list-discovery-max-age must be nonnegative")
	}
	return nil
}

func (o modelDiscoveryOptions) validateNamespace(isStandalone bool) error {
	if isStandalone {
		return nil
	}
	return discovery.ValidateNamespace(o.namespace)
}

func (o modelDiscoveryOptions) kubernetesProvider(config *rest.Config) (*discovery.KubernetesProvider, error) {
	provider, err := discovery.NewKubernetesProviderForNamespace(config, o.namespace)
	if err != nil {
		return nil, err
	}
	provider.WithModelAdapters(o.watchModelAdapters)
	if o.watchModelClaims {
		provider = provider.WithModelClaims()
	}
	return provider, nil
}

func startEarlyModelListGate(enabled bool, addr string) (*gateway.ModelListGate, error) {
	if !enabled {
		return nil, nil
	}
	gate := &gateway.ModelListGate{}
	if err := gate.StartHTTPServer(addr); err != nil {
		return nil, err
	}
	return gate, nil
}

func startGatewayHTTPServer(gate *gateway.ModelListGate, server *gateway.Server, addr string) error {
	if gate != nil {
		gate.SetServer(server)
		return nil
	}
	return server.StartHTTPServer(addr)
}

func (o *kubeAPIOptions) addFlags(fs *flag.FlagSet) {
	fs.Float64Var(
		&o.qps,
		"kube-api-qps",
		float64(rest.DefaultQPS),
		"Maximum QPS for the core Kubernetes and Gateway API clients; must be greater than zero.",
	)
	fs.IntVar(
		&o.burst,
		"kube-api-burst",
		rest.DefaultBurst,
		"Maximum burst for the core Kubernetes and Gateway API clients; must be greater than zero.",
	)
}

func (o kubeAPIOptions) validate() error {
	// Validate the effective float32 value used by rest.Config as well, so that
	// conversion cannot turn a positive QPS into zero or infinity.
	qps := float32(o.qps)
	if !(qps > 0) || math.IsInf(float64(qps), 0) {
		return fmt.Errorf("--kube-api-qps must be finite and greater than zero as a float32, got %v", o.qps)
	}
	if o.burst <= 0 {
		return fmt.Errorf("--kube-api-burst must be greater than zero, got %d", o.burst)
	}
	return nil
}

func (o kubeAPIOptions) applyTo(config *rest.Config) {
	config.QPS = float32(o.qps)
	config.Burst = o.burst
}

func main() {
	if err := runGateway(); err != nil {
		klog.ErrorS(err, "Gateway stopped")
		klog.Flush()
		os.Exit(1)
	}
}

func runGateway() error {
	var kubeAPI kubeAPIOptions
	kubeAPI.addFlags(flag.CommandLine)
	var modelDiscovery modelDiscoveryOptions
	modelDiscovery.addFlags(flag.CommandLine)
	flag.StringVar(&grpcAddr, "grpc-bind-address", ":50052", "The address the gRPC server binds to.")
	flag.StringVar(&httpAddr, "http-bind-address", "", "The address the HTTP server binds to (metrics, /v1/models).")
	flag.StringVar(&metricsAddr, "metrics-bind-address", "", "[Deprecated] Use --http-bind-address instead.")
	flag.BoolVar(&standalone, "standalone", false, "Run in standalone mode without Kubernetes.")
	flag.StringVar(&pprofAddr, "pprof-bind-address", "localhost:6060",
		"The address the pprof debug server binds to. Empty disables it.")
	flag.StringVar(&endpointsConfig, "endpoints-config", "",
		"Path to endpoints config file (required in standalone mode).")
	klog.InitFlags(flag.CommandLine)
	defer klog.Flush()
	flag.Parse()
	if err := kubeAPI.validate(); err != nil {
		return err
	}
	if err := modelDiscovery.validateForMode(standalone); err != nil {
		return err
	}

	httpAddr = resolveHTTPAddress(httpAddr, metricsAddr)

	// Validate standalone mode flags
	if standalone && endpointsConfig == "" {
		return errors.New("--endpoints-config is required when running in standalone mode")
	}

	redisClient, err := prepareGatewayRedis(standalone)
	if err != nil {
		return err
	}
	defer closeGatewayRedis(redisClient)

	lifetimeCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	lifetimeCtx, cancelLifetime := context.WithCancel(lifetimeCtx)
	defer cancelLifetime()

	var config *rest.Config
	var k8sClient kubernetes.Interface
	var gatewayK8sClient versioned.Interface
	var discoveryProvider discovery.Provider

	if standalone {
		// Standalone mode: use file-based discovery
		klog.Info("Running in standalone mode")
		discoveryProvider = discovery.NewStaticProvider(endpointsConfig)
	} else {
		var err error
		config, k8sClient, gatewayK8sClient, err = loadGatewayKubernetes(flag.Lookup("kubeconfig").Value.String(), kubeAPI)
		if err != nil {
			return err
		}
		discoveryProvider, err = modelDiscovery.kubernetesProvider(config)
		if err != nil {
			return err
		}

	}
	startupCtx := lifetimeCtx
	cancelStartup := func() {}
	if !standalone {
		startupCtx, cancelStartup = context.WithTimeout(lifetimeCtx, modelDiscoveryStartupTimeout)
	}
	defer cancelStartup()
	modelListGate, err := startEarlyModelListGate(!standalone || modelDiscovery.maxAge > 0, httpAddr)
	if err != nil {
		return fmt.Errorf("start model-list HTTP server: %w", err)
	}
	defer modelListGate.Shutdown()
	stopOnStartup := context.AfterFunc(startupCtx, func() {
		if !modelListGate.Published() {
			cancelLifetime()
		}
	})
	defer stopOnStartup()

	// Initialize cache
	kvSyncEnabled := utils.LoadEnvBool(constants.EnvPrefixCacheKVEventSyncEnabled, false)
	remoteTokenizerEnabled := utils.LoadEnvBool(constants.EnvPrefixCacheUseRemoteTokenizer, false)

	cacheOptions := cache.InitOptions{
		InstallationNamespace:          modelDiscovery.namespace,
		ModelRouterProviderWithContext: routing.NewSLORouterWithContext,
		IsGateway:                      true,
		EnableKVSync:                   kvSyncEnabled && remoteTokenizerEnabled,
		RedisClient:                    redisClient,
		DiscoveryProvider:              discoveryProvider,
		DisableModelClaimPodBindings:   !standalone && !modelDiscovery.watchModelClaims,
		ModelListMaxAge:                modelDiscovery.maxAge,
	}
	gatewayCache, err := initializeGatewayCache(startupCtx, lifetimeCtx, config, cacheOptions, standalone)
	if err != nil {
		return fmt.Errorf("gateway startup in namespace %q: %w", modelDiscovery.namespace, err)
	}

	defer gatewayCache.Close()
	defer func() {
		cancelLifetime()
		if modelListGate != nil {
			modelListGate.Disable()
		}
	}()

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	defer func() { _ = lis.Close() }()
	serverOptions := gateway.ServerOptions{
		LifetimeContext:     lifetimeCtx,
		DisableRateLimiting: utils.LoadEnvBool(envDisableRateLimiting, false),
		PriorityTier:        utils.LoadEnvBool(envPriorityTierEnabled, false),
		ModelListMode:       gateway.ModelListMode(modelDiscovery.listMode),
		VerifyModelList:     modelDiscovery.maxAge > 0,
	}

	gatewayServer, err := gateway.NewServerWithContext(startupCtx, redisClient, k8sClient, gatewayK8sClient, serverOptions)

	if err != nil {
		return err
	}
	defer gatewayServer.Shutdown()

	stateSyncEnabled := utils.LoadEnvBool("AIBRIX_STATESYNC_ENABLED", false)
	var syncManager *statesync.RedisSync
	if stateSyncEnabled {
		klog.InfoS("statesync enabled; starting cross-replica state sync")
		table := prefixcacheindexer.GetSharedPrefixHashTable()
		table.EnableDeltaSync()
		syncManager = statesync.New(redisClient)
		syncManager.Register(prefixcacheindexer.NewPrefixHashTableSyncable(table))
		syncManager.StartWithContext(lifetimeCtx)
		defer syncManager.Stop()
	} else {
		klog.InfoS("statesync disabled; set AIBRIX_STATESYNC_ENABLED=true to enable cross-replica state sync")
	}

	telApp, opts, err := initializeGatewayTelemetry(startupCtx)
	if err != nil {
		return err
	}
	if telApp != nil {
		defer telApp.Shutdown()
	}

	grpcMaxMessageSize := utils.LoadEnvInt(envGRPCMaxMessageSizeBytes, defaultGRPCMaxMessageSizeBytes)
	opts = append(opts, grpc.MaxRecvMsgSize(grpcMaxMessageSize))

	// One panicking request must not take the process down with the requests in
	// flight on it, so recover in the stream interceptor and fail only that stream.
	opts = append(opts, grpc.StreamInterceptor(gateway.StreamPanicRecoveryInterceptor()))

	s := grpc.NewServer(opts...)

	extProcPb.RegisterExternalProcessorServer(s, gatewayServer)

	healthCheck := health.NewServer()
	defer healthCheck.Shutdown()
	healthCheck.SetServingStatus("gateway-plugin", healthPb.HealthCheckResponse_NOT_SERVING)
	healthService := &lifetimeHealthServer{Server: healthCheck, lifetime: lifetimeCtx}
	if !standalone {
		healthService.ready = modelListGate.Published
	}
	healthPb.RegisterHealthServer(s, healthService)
	err = publishGateway(startupCtx, lifetimeCtx, modelListGate, gatewayServer, healthCheck, httpAddr)
	if err != nil {
		return fmt.Errorf("publish gateway readiness: %w", err)
	}
	stopOnStartup()
	cancelStartup()
	if err := gatewayCache.StartGatewayBackground(cacheOptions); err != nil {
		return err
	}
	klog.Infof("Started HTTP server on %s", httpAddr)

	klog.Info("starting gRPC server on " + grpcAddr)

	profiling := startPprofServer(pprofAddr)
	if profiling != nil {
		defer func() { _ = profiling.Close() }()
	}

	klog.Infof("GOMAXPROCS is: %d", runtime.GOMAXPROCS(0))

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-lifetimeCtx.Done()
		if modelListGate != nil {
			modelListGate.Disable()
		}
		healthCheck.SetServingStatus("gateway-plugin", healthPb.HealthCheckResponse_NOT_SERVING)
		s.Stop()
	}()

	defer func() { cancelLifetime(); <-shutdownDone }()

	if err := s.Serve(lis); err != nil {
		return err
	}
	return nil
}

// startPprofServer serves the profiling endpoints (http.DefaultServeMux, where
// net/http/pprof registers them) on addr in the background and returns the
// listener, or nil when addr is empty or cannot be bound. A bind failure is
// logged, not fatal: profiling is a debugging aid, and a port that another
// process holds, such as a second gateway on the same host in local mode, must
// not stop the gateway from starting.
func startPprofServer(addr string) net.Listener {
	if addr == "" {
		return nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		klog.ErrorS(err, "pprof server disabled: cannot bind its address", "address", addr)
		return nil
	}
	klog.InfoS("pprof server listening", "address", ln.Addr().String())
	go func() {
		if err := http.Serve(ln, nil); err != nil && !errors.Is(err, net.ErrClosed) {
			klog.ErrorS(err, "pprof server stopped")
		}
	}()
	return ln
}

func (o modelDiscoveryOptions) validateForMode(isStandalone bool) error {
	if err := o.validate(); err != nil {
		return err
	}
	return o.validateNamespace(isStandalone)
}

func resolveHTTPAddress(addr, deprecated string) string {
	if addr != "" {
		return addr
	}
	if deprecated != "" {
		klog.Warning("--metrics-bind-address is deprecated, use --http-bind-address instead")
		return deprecated
	}
	return ":8080"
}

func loadGatewayKubernetes(
	path string, options kubeAPIOptions,
) (*rest.Config, kubernetes.Interface, versioned.Interface, error) {
	var config *rest.Config
	var err error
	if path == "" {
		config, err = rest.InClusterConfig()
	} else {
		config, err = clientcmd.BuildConfigFromFlags("", path)
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build kubeconfig: %w", err)
	}
	options.applyTo(config)
	core, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create Kubernetes client: %w", err)
	}
	gatewayClient, err := versioned.NewForConfig(config)
	return config, core, gatewayClient, err
}

func publishGateway(
	startup, lifetime context.Context, gate *gateway.ModelListGate, server *gateway.Server,
	healthCheck *health.Server, httpAddr string,
) (err error) {
	if gate != nil {
		err = gate.Publish(startup, lifetime, server, func(ready bool) {
			status := healthPb.HealthCheckResponse_NOT_SERVING
			if ready {
				status = healthPb.HealthCheckResponse_SERVING
			}
			healthCheck.SetServingStatus("gateway-plugin", status)
		})
	} else {
		if err = startup.Err(); err == nil {
			err = lifetime.Err()
		}
		if err == nil {
			err = startGatewayHTTPServer(nil, server, httpAddr)
		}
		if err == nil {
			healthCheck.SetServingStatus("gateway-plugin", healthPb.HealthCheckResponse_SERVING)
		}
	}
	return err
}

func prepareGatewayRedis(isStandalone bool) (*redis.Client, error) {
	client := utils.GetRedisClient()
	if client == nil {
		if !isStandalone {
			return nil, errors.New("redis is required in Kubernetes mode")
		}
		klog.Warning("Running without Redis: rate limiting and user auth disabled")
	}
	return client, nil
}

func closeGatewayRedis(client *redis.Client) {
	if client != nil {
		if err := client.Close(); err != nil {
			klog.Warningf("Error closing Redis client: %v", err)
		}
	}
}

func initializeGatewayCache(
	startup, lifetime context.Context, config *rest.Config, opts cache.InitOptions, isStandalone bool,
) (*cache.Store, error) {
	if isStandalone {
		opts.ModelRouterProviderWithContext = nil
		opts.ModelRouterProvider = routing.ModelRouterFactory
		return cache.InitWithOptions(config, lifetime.Done(), opts), nil
	}
	return cache.InitGatewayWithOptions(startup, lifetime, config, opts)
}

func initializeGatewayTelemetry(startupCtx context.Context) (*utils.Telemetry, []grpc.ServerOption, error) {
	var err error
	var opts []grpc.ServerOption

	otelEnabled := utils.OTELEnabled()
	var telApp *utils.Telemetry
	if otelEnabled {
		otlpProtocol := utils.LoadEnv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
		if otlpProtocol == "http/protobuf" {
			otlpProtocol = "http"
		}
		telApp, err = utils.InitOpenTelemetryWithContext(startupCtx, "aibrix-gateway-plugin", otlpProtocol)
		if err != nil || telApp == nil {
			return nil, nil, fmt.Errorf("initialize OpenTelemetry: %w", err)
		}
		opts = append(opts, grpc.StatsHandler(
			otelgrpc.NewServerHandler(
				otelgrpc.WithFilter(
					filters.Not(filters.HealthCheck()),
				),
			),
		))
	}

	return telApp, opts, nil
}

func kubernetesDiscoveryClients(kubeAPI kubeAPIOptions, models modelDiscoveryOptions) (
	*rest.Config, kubernetes.Interface, versioned.Interface, discovery.Provider, error,
) {
	kubeConfig := flag.Lookup("kubeconfig").Value.String()
	var config *rest.Config
	var err error
	if kubeConfig == "" {
		klog.Info("using in-cluster configuration")
		config, err = rest.InClusterConfig()
	} else {
		klog.Infof("using configuration from '%s'", kubeConfig)
		config, err = clientcmd.BuildConfigFromFlags("", kubeConfig)
	}
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("build kubeconfig: %w", err)
	}
	kubeAPI.applyTo(config)
	core, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	gatewayClient, err := versioned.NewForConfig(config)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	provider, err := models.kubernetesProvider(config)
	return config, core, gatewayClient, provider, err
}

func validateStartupOptions(
	kubeAPI kubeAPIOptions, models modelDiscoveryOptions, standalone bool, endpoints string,
) error {
	if err := kubeAPI.validate(); err != nil {
		return err
	}
	if err := models.validate(); err != nil {
		return err
	}
	if !standalone {
		return discovery.ValidateNamespace(models.namespace)
	}
	if endpoints == "" {
		return fmt.Errorf("--endpoints-config is required when running in standalone mode")
	}
	return nil
}
