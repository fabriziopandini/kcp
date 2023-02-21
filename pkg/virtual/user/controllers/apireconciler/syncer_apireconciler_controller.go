/*
Copyright 2022 The KCP Authors.

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

package apireconciler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	kcpcache "github.com/kcp-dev/apimachinery/v2/pkg/cache"
	"github.com/kcp-dev/logicalcluster/v3"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	apisv1alpha1 "github.com/kcp-dev/kcp/pkg/apis/apis/v1alpha1"
	apisv1alpha1informers "github.com/kcp-dev/kcp/pkg/client/informers/externalversions/apis/v1alpha1"
	apisv1alpha1listers "github.com/kcp-dev/kcp/pkg/client/listers/apis/v1alpha1"
	"github.com/kcp-dev/kcp/pkg/logging"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/apidefinition"
	dynamiccontext "github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/context"
)

const (
	ControllerName = "kcp-virtual-user-api-reconciler-"
)

type CreateAPIDefinitionFunc func(apiBindingWorkspace logicalcluster.Name, apiBindingName string, apiResourceSchema *apisv1alpha1.APIResourceSchema, version string, identityHash string) (apidefinition.APIDefinition, error)
type AllowedAPIfilterFunc func(apiGroupResource schema.GroupResource) bool

func NewAPIReconciler(
	virtualWorkspaceName string,
	apiBindingInformer apisv1alpha1informers.APIBindingClusterInformer,
	apiResourceSchemaInformer apisv1alpha1informers.APIResourceSchemaClusterInformer,
	createAPIDefinition CreateAPIDefinitionFunc,
	allowedAPIfilter AllowedAPIfilterFunc,
) (*APIReconciler, error) {
	queue := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), ControllerName+virtualWorkspaceName)

	c := &APIReconciler{
		virtualWorkspaceName: virtualWorkspaceName,

		apiBindingLister:        apiBindingInformer.Lister(),
		apiResourceSchemaLister: apiResourceSchemaInformer.Lister(),

		queue: queue,

		createAPIDefinition: createAPIDefinition,
		allowedAPIfilter:    allowedAPIfilter,

		apiSets: map[dynamiccontext.APIDomainKey]apidefinition.APIDefinitionSet{},
	}

	logger := logging.WithReconciler(klog.Background(), ControllerName+virtualWorkspaceName)

	apiBindingInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) { c.enqueueApiBinding(obj, logger, "") },
		UpdateFunc: func(old, obj interface{}) {
			oldBinding := old.(*apisv1alpha1.APIBinding)
			newBinding := obj.(*apisv1alpha1.APIBinding)

			// only enqueue when syncedResource is changed.
			if !equality.Semantic.DeepEqual(oldBinding.Status.BoundResources, newBinding.Status.BoundResources) {
				c.enqueueApiBinding(obj, logger, "")
			}
		},
		DeleteFunc: func(obj interface{}) { c.enqueueApiBinding(obj, logger, "") },
	})

	return c, nil
}

// APIReconciler is a controller watching APIExports, APIResourceSchemas and SyncTargets, and updates the
// API definitions driving the virtual workspace.
type APIReconciler struct {
	virtualWorkspaceName string

	apiBindingLister        apisv1alpha1listers.APIBindingClusterLister
	apiResourceSchemaLister apisv1alpha1listers.APIResourceSchemaClusterLister

	queue workqueue.RateLimitingInterface

	createAPIDefinition CreateAPIDefinitionFunc
	allowedAPIfilter    AllowedAPIfilterFunc

	mutex   sync.RWMutex // protects the map, not the values!
	apiSets map[dynamiccontext.APIDomainKey]apidefinition.APIDefinitionSet
}

func (c *APIReconciler) enqueueApiBinding(obj interface{}, logger logr.Logger, logSuffix string) {
	key, err := kcpcache.DeletionHandlingMetaClusterNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	logging.WithQueueKey(logger, key).V(2).Info(fmt.Sprintf("queueing ApiBinding%s", logSuffix))
	c.queue.Add(key)
}

func (c *APIReconciler) startWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *APIReconciler) Start(ctx context.Context) {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	logger := logging.WithReconciler(klog.FromContext(ctx), ControllerName+c.virtualWorkspaceName)
	ctx = klog.NewContext(ctx, logger)
	logger.Info("Starting controller")
	defer logger.Info("Shutting down controller")

	go wait.Until(func() { c.startWorker(ctx) }, time.Second, ctx.Done())

	// stop all watches if the controller is stopped
	defer func() {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		for _, sets := range c.apiSets {
			for _, v := range sets {
				v.TearDown()
			}
		}
	}()

	<-ctx.Done()
}

func (c *APIReconciler) ShutDown() {
	c.queue.ShutDown()
}

func (c *APIReconciler) processNextWorkItem(ctx context.Context) bool {
	// Wait until there is a new item in the working queue
	k, quit := c.queue.Get()
	if quit {
		return false
	}
	key := k.(string)

	// No matter what, tell the queue we're done with this key, to unblock
	// other workers.
	defer c.queue.Done(key)

	if err := c.process(ctx, key); err != nil {
		runtime.HandleError(fmt.Errorf("%s: failed to sync %q, err: %w", ControllerName+c.virtualWorkspaceName, key, err))
		c.queue.AddRateLimited(key)
		return true
	}

	c.queue.Forget(key)
	return true
}

func (c *APIReconciler) process(ctx context.Context, key string) error {
	logger := logging.WithQueueKey(klog.FromContext(ctx), key)
	ctx = klog.NewContext(ctx, logger)

	clusterName, _, apiBindingName, err := kcpcache.SplitMetaClusterNamespaceKey(key)
	if err != nil {
		runtime.HandleError(err)
		return nil
	}

	// TODO: think again at this
	// The apiDomainKey is used as a key to store APIDefinitions for a specific view,
	// for now, we assume that our view is 1:1 with a workload cluster.
	// NOTE: current implementation does not work because sometimes we get
	// the path and sometime the key for a cluster, and as consequence we store APIDefinitions
	// under different keys.
	// We temporarily fixed this by reading the path from the apiBinding, but this does not works
	// on deletion, but this should be re-evaluated after all the changes.

	apiDomainKey := dynamiccontext.APIDomainKey(clusterName.String())

	apiBinding, err := c.apiBindingLister.Cluster(clusterName).Get(apiBindingName)
	if apierrors.IsNotFound(err) {
		c.removeAPIDefinitionSet(apiDomainKey)
		return nil
	}
	if err != nil {
		return err
	}

	apiDomainKey = dynamiccontext.APIDomainKey(logicalcluster.From(apiBinding))
	return c.reconcile(ctx, apiDomainKey, apiBinding)
}

func (c *APIReconciler) GetAPIDefinitionSet(_ context.Context, key dynamiccontext.APIDomainKey) (apidefinition.APIDefinitionSet, bool, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	apiSet, ok := c.apiSets[key]
	return apiSet, ok, nil
}

func (c *APIReconciler) removeAPIDefinitionSet(key dynamiccontext.APIDomainKey) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.apiSets, key)
}
