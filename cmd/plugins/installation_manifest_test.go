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
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
)

func renderedObjects(t *testing.T, command ...string) []unstructured.Unstructured {
	t.Helper()
	if _, err := exec.LookPath(command[0]); err != nil {
		t.Skip(command[0] + " is required for manifest rendering")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	manifest, err := exec.CommandContext(ctx, command[0], command[1:]...).CombinedOutput()
	require.NoError(t, err, "%s", manifest)
	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(manifest), 4096)
	var objects []unstructured.Unstructured
	for {
		var obj unstructured.Unstructured
		err := decoder.Decode(&obj)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if obj.GetKind() != "" {
			objects = append(objects, obj)
		}
	}
	return objects
}

func assertGatewayNamespaceGrants(
	t *testing.T, objects []unstructured.Unstructured, control, workload, component string,
) {
	t.Helper()
	deployment, discovery, namespaceGET, routes, wake, bindings := false, false, false, false, false, 0
	for _, obj := range objects {
		if obj.GetKind() == "Deployment" {
			containers, _, err := unstructured.NestedSlice(obj.Object, "spec", "template", "spec", "containers")
			require.NoError(t, err)
			for _, item := range containers {
				container := item.(map[string]interface{})
				if container["name"] != "gateway-plugin" {
					continue
				}
				args, _, err := unstructured.NestedStringSlice(container, "args")
				require.NoError(t, err)
				require.Contains(t, args, "--installation-namespace="+workload)
				require.Equal(t, control, obj.GetNamespace())
				deployment = true
			}
		}
		if obj.GetLabels()["app.kubernetes.io/component"] != component {
			continue
		}
		if obj.GetKind() == "RoleBinding" || obj.GetKind() == "ClusterRoleBinding" {
			subjects, _, err := unstructured.NestedSlice(obj.Object, "subjects")
			require.NoError(t, err)
			for _, raw := range subjects {
				subject := raw.(map[string]interface{})
				require.Equal(t, control, subject["namespace"])
			}
			bindings++
		}
		if obj.GetKind() != "Role" && obj.GetKind() != "ClusterRole" {
			continue
		}
		rules, _, err := unstructured.NestedSlice(obj.Object, "rules")
		require.NoError(t, err)
		for _, raw := range rules {
			rule := raw.(map[string]interface{})
			resources, _, err := unstructured.NestedStringSlice(rule, "resources")
			require.NoError(t, err)
			verbs, _, err := unstructured.NestedStringSlice(rule, "verbs")
			require.NoError(t, err)
			for _, resource := range resources {
				switch resource {
				case "pods", "modeladapters", "modelclaims":
					require.Equal(t, "Role", obj.GetKind())
					require.Equal(t, workload, obj.GetNamespace())
					if len(verbs) == 1 && verbs[0] == "patch" {
						require.Equal(t, "pods", resource)
						wake = true
					} else {
						require.ElementsMatch(t, []string{"list", "watch"}, verbs)
						discovery = true
					}
				case "httproutes":
					require.Equal(t, "Role", obj.GetKind())
					require.Equal(t, control, obj.GetNamespace())
					require.Equal(t, []string{"get"}, verbs)
					routes = true
				case "namespaces":
					require.Equal(t, "ClusterRole", obj.GetKind())
					require.Equal(t, []string{"get"}, verbs)
					names, _, err := unstructured.NestedStringSlice(rule, "resourceNames")
					require.NoError(t, err)
					require.Equal(t, []string{workload}, names)
					namespaceGET = true
				default:
					t.Errorf("unexpected gateway resource grant: %s", resource)
				}
			}
		}
	}
	require.True(t, deployment, "gateway Deployment")
	require.True(t, discovery, "workload discovery Role")
	require.True(t, namespaceGET, "exact Namespace GET")
	require.True(t, routes, "control namespace HTTPRoute GET")
	require.True(t, wake, "workload Pod patch")
	require.Equal(t, 4, bindings, "each grant must have its own binding")
}

func TestInstallationNamespacePreservedByOverlays(t *testing.T) {
	for _, overlay := range []string{
		"default", "overlays/release", "overlays/dev/gateway-plugin",
		"overlays/vke-dev/gateway-plugin", "overlays/vke/default", "overlays/vke-ipv6",
	} {
		t.Run(overlay, func(t *testing.T) {
			objects := renderedObjects(t, "kubectl", "kustomize", filepath.Join("..", "..", "config", overlay))
			assertGatewayNamespaceGrants(t, objects, "aibrix-system", "default", "aibrix-gateway-plugins")
		})
	}
}

func TestCustomInstallationNamespaceHasOneSource(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	overlay := t.TempDir()
	root, err = filepath.Rel(overlay, root)
	require.NoError(t, err)
	configuration := fmt.Sprintf(`apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: control
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
`, root, root)
	require.NoError(t, os.WriteFile(filepath.Join(overlay, "kustomization.yaml"), []byte(configuration), 0600))
	objects := renderedObjects(t, "kubectl", "kustomize", overlay)
	assertGatewayNamespaceGrants(t, objects, "control", "models", "aibrix-gateway-plugins")
}

func TestHelmInstallationNamespace(t *testing.T) {
	for _, sidecar := range []bool{false, true} {
		name := "gateway"
		values := "gateway.enable=true,gateway.envoyAsSideCar=false"
		if sidecar {
			name = "sidecar"
			values = "gateway.enable=false,gateway.envoyAsSideCar=true"
		}
		t.Run(name, func(t *testing.T) {
			objects := renderedObjects(t, "helm", "template", "aibrix", filepath.Join("..", "..", "dist", "chart"),
				"--namespace", "control", "--set", values+",gatewayPlugin.installationNamespace=models")
			assertGatewayNamespaceGrants(t, objects, "control", "models", "aibrix-gateway-plugin")
		})
	}
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is required")
	}
	for _, value := range []string{"", "Bad", "one,two", " models", "models "} {
		command := exec.Command("helm", "template", "aibrix", filepath.Join("..", "..", "dist", "chart"),
			"--set-string", "gatewayPlugin.installationNamespace="+value)
		output, err := command.CombinedOutput()
		require.Error(t, err, "%s: %s", value, output)
	}
}
