/*
Copyright 2026 The Kubernetes Authors.
Copyright 2026 The llm-d Authors.

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

package epp

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	v1 "sigs.k8s.io/gateway-api-inference-extension/api/v1"

	apixv1 "github.com/llm-d/llm-d-router/apix/v1"
	"github.com/llm-d/llm-d-router/pkg/common"
	"github.com/llm-d/llm-d-router/pkg/common/routing"
	"github.com/llm-d/llm-d-router/pkg/epp/controller"
	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/datastore"
	testutil "github.com/llm-d/llm-d-router/pkg/epp/util/testing"
)

// TestInferenceObjectivePoolWatchWiring verifies against a real API server
// that pool events requeue selector-bearing InferenceObjectives: a pool label
// change or pool creation must re-reconcile objectives whose poolSelector
// matches the pool, so datastore entries follow the pool's labels.
// The v1-only InferenceObjective CRD under testdata/crd serves llm-d.ai/v1;
// the shipped CRD serves v1alpha2 only.
func TestInferenceObjectivePoolWatchWiring(t *testing.T) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}",
		"sigs.k8s.io/gateway-api-inference-extension").Output()
	require.NoError(t, err, "failed to locate gateway-api-inference-extension module")
	gaieModulePath := strings.TrimSpace(string(out))

	// A dedicated environment: the objective CRD must serve v1 for the
	// PrimaryV1 reconciler watches, which the shipped v1alpha2-only CRD does not.
	env := &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join(gaieModulePath, "config", "crd", "bases"),
			filepath.Join(repoRootPath, "test", "integration", "epp", "testdata", "crd"),
		},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	require.NoError(t, err, "failed to start envtest environment")
	t.Cleanup(func() { _ = env.Stop() })

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(apixv1.Install(scheme))
	utilruntime.Must(v1.Install(scheme))

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	require.NoError(t, err, "failed to create manager")

	poolName := "pool-watch-pool"
	objectiveName := "selector-objective"
	namespace := "pool-watch-" + uuid.NewString()[:8]

	ds := datastore.NewDatastore(t.Context(), datalayer.NewTestRuntime(t, time.Second))
	reconciler := &controller.InferenceObjectiveReconciler{
		Datastore: ds,
		Reader:    mgr.GetClient(),
		PoolGKNN: common.GKNN{
			NamespacedName: types.NamespacedName{Name: poolName, Namespace: namespace},
			GroupKind:      schema.GroupKind{Group: routing.InferencePoolAPIGroup, Kind: "InferencePool"},
		},
		RunOnNonLeaders: true,
		PrimaryV1:       true,
	}
	require.NoError(t, reconciler.SetupWithManager(mgr), "failed to set up reconciler")

	ctx, cancel := context.WithCancel(t.Context())
	mgrErr := make(chan error, 1)
	go func() { mgrErr <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-mgrErr; err != nil && !strings.Contains(err.Error(), "context canceled") {
			t.Errorf("manager stopped unexpectedly: %v", err)
		}
	})

	directClient, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err, "failed to create direct client")
	require.NoError(t, directClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}))

	createPool := func(tier string) {
		pool := testutil.MakeInferencePool(poolName).
			Namespace(namespace).
			Selector(map[string]string{"app": poolName}).
			EndpointPickerRef("epp").
			TargetPorts(8000).
			ObjRef()
		pool.Spec.EndpointPickerRef.Port = &v1.Port{Number: v1.PortNumber(9002)}
		pool.Labels = map[string]string{"tiers": tier}
		require.NoError(t, directClient.Create(ctx, pool), "failed to create pool")
	}
	poolKey := types.NamespacedName{Name: poolName, Namespace: namespace}
	updatePoolLabels := func(tier string) {
		pool := &v1.InferencePool{}
		require.NoError(t, directClient.Get(ctx, poolKey, pool), "failed to get pool")
		pool.Labels = map[string]string{"tiers": tier}
		require.NoError(t, directClient.Update(ctx, pool), "failed to update pool labels")
	}
	objectiveRegistered := func() bool { return ds.ObjectiveGet(objectiveName) != nil }

	createPool("shared")
	objective := testutil.MakeV1InferenceObjective(objectiveName).
		Namespace(namespace).
		Priority(int32(1)).
		PoolSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"tiers": "shared"}}).
		ObjRef()
	require.NoError(t, directClient.Create(ctx, objective), "failed to create objective")

	// The objective's own create event registers it.
	require.Eventually(t, objectiveRegistered, 15*time.Second, 50*time.Millisecond,
		"objective was not registered from its create event")

	// A pool label change must requeue the objective; the selector no longer
	// matches, so the datastore entry is dropped.
	updatePoolLabels("dedicated")
	require.Eventually(t, func() bool { return !objectiveRegistered() }, 15*time.Second, 50*time.Millisecond,
		"pool label change did not requeue the selector objective")

	// Changing the labels back re-registers it.
	updatePoolLabels("shared")
	require.Eventually(t, objectiveRegistered, 15*time.Second, 50*time.Millisecond,
		"pool label change back did not requeue the selector objective")

	// Pool deletion does not requeue (the pool reconciler owns that path),
	// so the entry survives. Recreating the pool with non-matching labels
	// fires the create event: the requeue must drop the stale entry.
	require.NoError(t, directClient.Delete(ctx, &v1.InferencePool{
		ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: namespace},
	}))
	require.Eventually(t, func() bool {
		return errors.IsNotFound(directClient.Get(ctx, poolKey, &v1.InferencePool{}))
	}, 15*time.Second, 50*time.Millisecond, "pool was not deleted")

	createPool("dedicated")
	require.Eventually(t, func() bool { return !objectiveRegistered() }, 15*time.Second, 50*time.Millisecond,
		"pool create event did not requeue the selector objective")
}
