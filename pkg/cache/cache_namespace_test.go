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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	modelv1alpha1 "github.com/vllm-project/aibrix/api/model/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/cache/discovery"
	"github.com/vllm-project/aibrix/pkg/constants"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	clientcache "k8s.io/client-go/tools/cache"
)

func TestGatewaySingletonCannotChangeOrOmitScope(t *testing.T) {
	original, published := store, gatewayPublished.Load()
	gatewayPublished.Store(nil)
	t.Cleanup(func() { store = original; gatewayPublished.Store(published) })
	for _, scope := range []string{"", "foreign"} {
		store = &Store{initialized: true, installationNamespace: scope}
		_, err := InitGatewayWithOptions(context.Background(), context.Background(), &rest.Config{Host: "https://unreachable.invalid"}, InitOptions{IsGateway: true, InstallationNamespace: "models"})
		require.ErrorContains(t, err, "cannot reuse")
	}
	store = &Store{initialized: true, installationNamespace: "models"}
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.lifetimeCtx = lifetime
	gatewayPublished.Store(store)
	got, err := InitGatewayWithOptions(context.Background(), context.Background(), &rest.Config{Host: "https://unreachable.invalid"}, InitOptions{IsGateway: true, InstallationNamespace: "models"})
	require.NoError(t, err)
	require.Same(t, store, got)
}

func TestNamespacedCacheForeignMembershipAndExactNames(t *testing.T) {
	c := NewForTest()
	c.installationNamespace = "models"
	own := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "same", Labels: map[string]string{constants.ModelLabelName: "label-model"}, Annotations: map[string]string{constants.ModelLabelName: "org/model"}}}
	require.True(t, handleDiscoveryObject(c, discovery.EventAdd, own, nil))
	require.Equal(t, []string{"label-model"}, c.ListModels())
	foreign := own.DeepCopy()
	foreign.Namespace = "foreign"
	foreign.Labels[constants.ModelLabelName] = "foreign-model"
	for _, ev := range []discovery.WatchEvent{{Type: discovery.EventAdd, Object: foreign}, {Type: discovery.EventUpdate, Object: own, OldObject: foreign}, {Type: discovery.EventUpdate, Object: foreign, OldObject: own}, {Type: discovery.EventDelete, Object: foreign}} {
		require.False(t, handleDiscoveryObject(c, ev.Type, ev.Object, ev.OldObject))
		require.Equal(t, []string{"label-model"}, c.ListModels())
	}
	adapter := &modelv1alpha1.ModelAdapter{ObjectMeta: metav1.ObjectMeta{Namespace: "foreign", Name: "foreign-adapter"}, Status: modelv1alpha1.ModelAdapterStatus{Instances: []string{"same"}}}
	require.False(t, handleDiscoveryObject(c, discovery.EventAdd, adapter, nil))
	changed := own.DeepCopy()
	changed.Labels[constants.ModelLabelName] = ""
	require.True(t, handleDiscoveryObject(c, discovery.EventUpdate, changed, own))
	require.Equal(t, []string{"org/model"}, c.ListModels())
	changedAgain := changed.DeepCopy()
	changedAgain.Annotations[constants.ModelLabelName] = ""
	require.True(t, handleDiscoveryObject(c, discovery.EventUpdate, changedAgain, changed))
	require.Empty(t, c.ListModels())
}

func TestNamespacedDispatchProtectsRealMembershipAndClaims(t *testing.T) {
	for _, kind := range []string{"pod", "adapter", "claim"} {
		t.Run(kind, func(t *testing.T) {
			st := NewForTest()
			defer st.Close()
			provider, err := discovery.NewKubernetesProviderForNamespace(&rest.Config{Host: "https://unused.invalid"}, "models")
			require.NoError(t, err)
			provider.WithModelListHealth(time.Minute)
			pod := lifecyclePod()
			pod.Name, pod.UID, pod.ResourceVersion = "same", "own-pod", "1"
			pod.Status.PodIP = "10.0.0.1"
			pod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}
			adapter := &modelv1alpha1.ModelAdapter{
				ObjectMeta: metav1.ObjectMeta{Name: "same-adapter", Namespace: "models", UID: "own-adapter", ResourceVersion: "1"},
				Status:     modelv1alpha1.ModelAdapterStatus{Instances: []string{"same"}},
			}
			claim := pendingModelClaim("models", "same-claim", "claim-model")
			claim.UID, claim.ResourceVersion = "own-claim", "1"
			entries := 0
			handler := func(ctx context.Context, ev discovery.WatchEvent) (bool, error) {
				entries++
				return handleDiscoveryObjectContext(ctx, st, ev)
			}
			for _, object := range []any{pod, adapter, claim} {
				_, required := object.(*modelv1alpha1.ModelClaim)
				applied, err := provider.DispatchNamespaced(context.Background(),
					discovery.WatchEvent{Type: discovery.EventAdd, Object: object}, !required, handler)
				require.NoError(t, err)
				require.True(t, applied)
			}
			// Seed a foreign Pod without model membership. An unguarded foreign
			// adapter could now add a second, different target to the own adapter.
			foreignPod := pod.DeepCopy()
			foreignPod.Namespace, foreignPod.UID, foreignPod.Status.PodIP = "foreign", "foreign-pod", "10.0.0.2"
			foreignPod.Labels[constants.ModelLabelName] = "foreign-model"
			st.mu.Lock()
			st.addPodLocked(foreignPod)
			st.mu.Unlock()
			var own, foreign any
			switch kind {
			case "pod":
				own, foreign = pod, foreignPod
			case "adapter":
				changed := adapter.DeepCopy()
				changed.Namespace, changed.UID = "foreign", "foreign-adapter"
				own, foreign = adapter, changed
			case "claim":
				changed := claim.DeepCopy()
				changed.Namespace, changed.UID = "foreign", "foreign-claim"
				changed.Status.Phase = modelv1alpha1.ModelClaimFailed
				own, foreign = claim, changed
			}
			beforeEntries := entries
			phase, reason, found := st.ModelClaimStatus("claim-model")
			require.True(t, found)
			for _, event := range []discovery.WatchEvent{
				{Type: discovery.EventAdd, Object: foreign},
				{Type: discovery.EventUpdate, OldObject: foreign, Object: foreign},
				{Type: discovery.EventUpdate, OldObject: foreign, Object: own},
				{Type: discovery.EventUpdate, OldObject: own, Object: foreign},
				{Type: discovery.EventDelete, Object: foreign},
				{Type: discovery.EventDelete, Object: clientcache.DeletedFinalStateUnknown{Key: "foreign/same", Obj: foreign}},
				{Type: discovery.EventDelete, Object: &clientcache.DeletedFinalStateUnknown{Key: "foreign/same", Obj: foreign}},
			} {
				applied, err := provider.DispatchNamespaced(context.Background(), event, kind != "claim", handler)
				require.NoError(t, err)
				require.False(t, applied)
				require.Equal(t, beforeEntries, entries, "even no-op foreign deletes must not enter the handler")
				require.ElementsMatch(t, []string{"org/model", "same-adapter"}, st.ListModels())
				for _, model := range st.ListModels() {
					pods, err := st.ListPodsByModel(model)
					require.NoError(t, err)
					require.Equal(t, 1, pods.Len())
					require.Equal(t, "10.0.0.1", pods.All()[0].Status.PodIP)
				}
				gotPhase, gotReason, gotFound := st.ModelClaimStatus("claim-model")
				require.Equal(t, phase, gotPhase)
				require.Equal(t, reason, gotReason)
				require.Equal(t, found, gotFound)
			}
			var updated any
			switch kind {
			case "pod":
				changed := pod.DeepCopy()
				changed.ResourceVersion, changed.Labels[constants.ModelLabelName] = "2", "own/changed"
				updated = changed
			case "adapter":
				changed := adapter.DeepCopy()
				changed.ResourceVersion, changed.Status.Instances = "2", nil
				updated = changed
			case "claim":
				changed := claim.DeepCopy()
				changed.ResourceVersion, changed.Status.Phase = "2", modelv1alpha1.ModelClaimFailed
				updated = changed
			}
			applied, err := provider.DispatchNamespaced(context.Background(), discovery.WatchEvent{
				Type: discovery.EventUpdate, OldObject: own, Object: updated,
			}, kind != "claim", handler)
			require.NoError(t, err)
			require.True(t, applied)
			switch kind {
			case "pod":
				require.True(t, st.HasModel("own/changed"))
			case "adapter":
				require.False(t, st.HasModel("same-adapter"))
			case "claim":
				phase, _, found := st.ModelClaimStatus("claim-model")
				require.True(t, found)
				require.Equal(t, string(modelv1alpha1.ModelClaimFailed), phase)
			}
			applied, err = provider.DispatchNamespaced(context.Background(), discovery.WatchEvent{
				Type: discovery.EventDelete, Object: clientcache.DeletedFinalStateUnknown{Key: "models/same", Obj: own},
			}, kind != "claim", handler)
			require.NoError(t, err)
			require.True(t, applied)
			require.Equal(t, beforeEntries+2, entries)
			if kind == "claim" {
				_, _, found := st.ModelClaimStatus("claim-model")
				require.False(t, found)
			} else {
				require.False(t, st.HasModel("same-adapter"))
			}
		})
	}
}

type cancelAfterAdapterMapping struct {
	context.Context
	cancel context.CancelFunc
	store  *Store
	model  string
}

func (c cancelAfterAdapterMapping) Err() error {
	if model, ok := c.store.metaModels.Load(c.model); ok && model.Pods.Len() > 0 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestCancellationWithinAdapterInstanceMapping(t *testing.T) {
	st := NewForTest()
	defer st.Close()
	for _, name := range []string{"first", "second"} {
		pod := lifecyclePod()
		pod.Name = name
		st.addPod(pod)
	}
	provider, err := discovery.NewKubernetesProviderForNamespace(&rest.Config{Host: "https://unused.invalid"}, "models")
	require.NoError(t, err)
	provider.WithModelListHealth(time.Minute)
	adapter := &modelv1alpha1.ModelAdapter{
		ObjectMeta: metav1.ObjectMeta{Name: "interrupted", Namespace: "models", UID: "adapter", ResourceVersion: "2"},
		Status:     modelv1alpha1.ModelAdapterStatus{Instances: []string{"first", "second"}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	controlled := cancelAfterAdapterMapping{Context: ctx, cancel: cancel, store: st, model: adapter.Name}
	applied, err := provider.DispatchNamespaced(controlled, discovery.WatchEvent{
		Type: discovery.EventAdd, Object: adapter,
	}, true, func(ctx context.Context, ev discovery.WatchEvent) (bool, error) {
		return handleDiscoveryObjectContext(ctx, st, ev)
	})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, applied)
	pods, err := st.ListPodsByModel(adapter.Name)
	require.NoError(t, err)
	require.Equal(t, 1, pods.Len(), "cancellation after the first real mapping must stop the second")
	_, healthy := provider.ModelListHealth().ModelsIfHealthy(st.ListModels)
	require.False(t, healthy)
}
