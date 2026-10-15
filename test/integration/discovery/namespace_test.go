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

package discovery_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	model "github.com/vllm-project/aibrix/api/model/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/cache/discovery"
	custom "github.com/vllm-project/aibrix/pkg/client/clientset/versioned"
	"github.com/vllm-project/aibrix/pkg/constants"
	authorizationv1 "k8s.io/api/authorization/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// Exercise the rendered installation grants with real API-server RBAC, rather
// than relying only on fake-client request assertions.
func TestGatewayInstallationNamespaceRBAC(t *testing.T) {
	for _, packaging := range []string{"kustomize", "helm"} {
		t.Run(packaging, func(t *testing.T) { testGatewayInstallationNamespaceRBAC(t, packaging) })
	}
}

func testGatewayInstallationNamespaceRBAC(t *testing.T, packaging string) {
	root := filepath.Join("..", "..", "..")
	environment := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(root, "config", "crd", "model")},
		ErrorIfCRDPathMissing: true,
	}
	adminConfig, err := environment.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, environment.Stop()) })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := kubernetes.NewForConfig(adminConfig)
	require.NoError(t, err)
	for _, namespace := range []string{"aibrix-system", "models", "foreign"} {
		_, err := admin.CoreV1().Namespaces().Create(ctx, &v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
		_, err = admin.CoreV1().Pods(namespace).Create(ctx, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "same", Labels: map[string]string{constants.ModelLabelName: "same-model"}},
			Spec:       v1.PodSpec{Containers: []v1.Container{{Name: "server", Image: "example.invalid/model"}}},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
	}

	adminCustom, err := custom.NewForConfig(adminConfig)
	require.NoError(t, err)
	for _, namespace := range []string{"models", "aibrix-system", "foreign"} {
		_, err = adminCustom.ModelV1alpha1().ModelAdapters(namespace).Create(ctx, &model.ModelAdapter{
			ObjectMeta: metav1.ObjectMeta{Name: "same-adapter"},
			Spec:       model.ModelAdapterSpec{ArtifactURL: "s3://test/adapter", PodSelector: &metav1.LabelSelector{}},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	serviceAccount := applyGatewayGrants(t, ctx, root, adminConfig, packaging)
	identity, err := environment.AddUser(envtest.User{
		Name:   "system:serviceaccount:aibrix-system:" + serviceAccount,
		Groups: []string{"system:serviceaccounts", "system:serviceaccounts:aibrix-system"},
	}, adminConfig)
	require.NoError(t, err)
	core, err := kubernetes.NewForConfig(identity.Config())
	require.NoError(t, err)
	crds, err := custom.NewForConfig(identity.Config())
	require.NoError(t, err)
	// The authorization caches may briefly lag newly created bindings.
	require.Eventually(t, func() bool {
		_, err := core.CoreV1().Pods("models").List(ctx, metav1.ListOptions{})
		return err == nil
	}, 5*time.Second, 50*time.Millisecond)
	verifyLegacyGrantRemoval(t, ctx, admin, core, serviceAccount, packaging)
	assertDiscoveryPermissions(t, ctx, core, crds)
	provider, err := discovery.NewKubernetesProviderForNamespace(identity.Config(), "models")
	require.NoError(t, err)
	provider.WithModelClaims().WithModelListHealth(time.Minute)
	stop := make(chan struct{})
	defer close(stop)
	defer provider.Shutdown()
	var mu sync.Mutex
	var seen []string
	require.NoError(t, provider.WatchContext(ctx, func(event discovery.WatchEvent) bool {
		if pod, ok := event.Object.(*v1.Pod); ok {
			mu.Lock()
			seen = append(seen, pod.Namespace+"/"+pod.Name)
			mu.Unlock()
		}
		return true
	}, stop))
	require.NoError(t, provider.ModelListHealth().EnsureVerified(ctx))
	mu.Lock()
	require.Equal(t, []string{"models/same"}, seen)
	mu.Unlock()
	wrong, err := discovery.NewKubernetesProviderForNamespace(identity.Config(), "foreign")
	require.NoError(t, err)
	require.ErrorContains(t, wrong.WatchContext(ctx, func(discovery.WatchEvent) bool { return true }, stop), "forbidden")
}

func applyGatewayGrants(t *testing.T, ctx context.Context, root string, config *rest.Config, packaging string) string {
	t.Helper()

	var manifest []byte
	var err error
	if packaging == "helm" {
		manifest, err = exec.CommandContext(ctx, "helm", "template", "aibrix", filepath.Join(root, "dist", "chart"),
			"--namespace", "aibrix-system", "--set", "gatewayPlugin.installationNamespace=models").Output()
	} else {
		absolute, absErr := filepath.Abs(root)
		require.NoError(t, absErr)
		overlay := t.TempDir()
		absolute, err = filepath.Rel(overlay, absolute)
		require.NoError(t, err)
		content := fmt.Sprintf(`apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- %s/config/default
patches:
- target:
    kind: ConfigMap
    name: gateway-discovery-config
  patch: |-
    - op: replace
      path: /data/installationNamespace
      value: models
transformers:
- %s/config/gateway/discovery-namespace/transformer
`, absolute, absolute)
		require.NoError(t, os.WriteFile(filepath.Join(overlay, "kustomization.yaml"), []byte(content), 0600))
		manifest, err = exec.CommandContext(ctx, "kubectl", "kustomize", overlay).Output()
	}
	require.NoError(t, err)
	writer, err := client.New(config, client.Options{Scheme: scheme.Scheme})
	require.NoError(t, err)
	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(manifest), 4096)
	applied := 0
	serviceAccount := ""
	for {
		var object unstructured.Unstructured
		err := decoder.Decode(&object)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		component := object.GetLabels()["app.kubernetes.io/component"]
		if component != "aibrix-gateway-plugins" && component != "aibrix-gateway-plugin" {
			continue
		}
		grantKinds := []string{"Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding", "ServiceAccount"}
		if !containsKind(grantKinds, object.GetKind()) {
			continue
		}
		require.NoError(t, writer.Create(ctx, &object))
		if object.GetKind() == "ServiceAccount" {
			serviceAccount = object.GetName()
		}
		applied++
	}
	require.Equal(t, 9, applied, "apply every rendered gateway grant and its ServiceAccount")
	require.NotEmpty(t, serviceAccount)
	return serviceAccount
}

func assertDiscoveryPermissions(t *testing.T, ctx context.Context, core kubernetes.Interface, custom custom.Interface) {
	t.Helper()
	for _, namespace := range []string{"models", "aibrix-system", "foreign", metav1.NamespaceAll} {
		allowed := namespace == "models"
		_, err := core.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		assertPermission(t, err, allowed)
		pods, err := core.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{})
		assertPermission(t, err, allowed)
		if pods != nil {
			pods.Stop()
		}
		_, err = custom.ModelV1alpha1().ModelAdapters(namespace).List(ctx, metav1.ListOptions{})
		assertPermission(t, err, allowed)
		adapters, err := custom.ModelV1alpha1().ModelAdapters(namespace).Watch(ctx, metav1.ListOptions{})
		assertPermission(t, err, allowed)
		if adapters != nil {
			adapters.Stop()
		}
		_, err = custom.ModelV1alpha1().ModelClaims(namespace).List(ctx, metav1.ListOptions{})
		assertPermission(t, err, allowed)
		claims, err := custom.ModelV1alpha1().ModelClaims(namespace).Watch(ctx, metav1.ListOptions{})
		assertPermission(t, err, allowed)
		if claims != nil {
			claims.Stop()
		}
	}
	_, err := core.CoreV1().Namespaces().Get(ctx, "models", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = core.CoreV1().Namespaces().Get(ctx, "foreign", metav1.GetOptions{})
	require.True(t, apierrors.IsForbidden(err), "%v", err)
	_, err = core.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	require.True(t, apierrors.IsForbidden(err), "%v", err)
}

func assertPermission(t *testing.T, err error, allowed bool) {
	t.Helper()
	if allowed {
		require.NoError(t, err)
	} else {
		require.True(t, apierrors.IsForbidden(err), "%v", err)
	}
}

func containsKind(kinds []string, kind string) bool {
	for _, candidate := range kinds {
		if candidate == kind {
			return true
		}
	}
	return false
}

// A namespace Role cannot override an old cluster-wide grant. Exercise the
// documented migration against the authorizer before checking final discovery.
func verifyLegacyGrantRemoval(
	t *testing.T, ctx context.Context, admin, core kubernetes.Interface, serviceAccount, packaging string,
) {
	t.Helper()
	name := "aibrix-gateway-plugins-role"
	binding := "aibrix-gateway-plugins-rolebinding"
	if packaging == "helm" {
		name = "aibrix-gateway-plugins"
		binding = name
	}
	_, err := admin.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"list", "watch"},
		}},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = admin.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: binding},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Namespace: "aibrix-system", Name: serviceAccount}},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := core.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
		return err == nil
	}, 5*time.Second, 50*time.Millisecond, "old grant still permits cluster-wide discovery")
	require.NoError(t, admin.RbacV1().ClusterRoleBindings().Delete(ctx, binding, metav1.DeleteOptions{}))
	require.NoError(t, admin.RbacV1().ClusterRoles().Delete(ctx, name, metav1.DeleteOptions{}))
	require.Eventually(t, func() bool {
		_, err := core.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
		return apierrors.IsForbidden(err)
	}, 5*time.Second, 50*time.Millisecond, "removing old bindings must restore confinement")
	for _, namespace := range []string{"models", "aibrix-system", "foreign", ""} {
		for _, request := range []struct {
			group, resource, verb string
			allowed               bool
		}{
			{resource: "pods", verb: "patch", allowed: namespace == "models"},
			{group: "gateway.networking.k8s.io", resource: "httproutes", verb: "get", allowed: namespace == "aibrix-system"},
		} {
			access := &authorizationv1.SelfSubjectAccessReview{
				Spec: authorizationv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &authorizationv1.ResourceAttributes{
					Namespace: namespace, Group: request.group, Resource: request.resource, Verb: request.verb, Name: "same",
				}},
			}
			review, err := core.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, access, metav1.CreateOptions{})
			require.NoError(t, err)
			require.Equal(t, request.allowed, review.Status.Allowed, "%s %s in %q", request.verb, request.resource, namespace)
		}
	}
}
