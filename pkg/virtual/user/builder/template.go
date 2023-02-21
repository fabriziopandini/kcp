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

package builder

import (
	"context"
	"errors"
	kcpdynamic "github.com/kcp-dev/client-go/dynamic"
	kcpkubernetesclientset "github.com/kcp-dev/client-go/kubernetes"
	dynamiccontext "github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/context"
	"github.com/kcp-dev/logicalcluster/v3"
	"strings"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	apisv1alpha1 "github.com/kcp-dev/kcp/pkg/apis/apis/v1alpha1"
	"github.com/kcp-dev/kcp/pkg/authorization/delegated"
	kcpinformers "github.com/kcp-dev/kcp/pkg/client/informers/externalversions"
	"github.com/kcp-dev/kcp/pkg/virtual/framework"
	virtualworkspacesdynamic "github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/apidefinition"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/apiserver"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/forwardingregistry"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/transforming"
	"github.com/kcp-dev/kcp/pkg/virtual/user/controllers/apireconciler"
)

type templateProvider struct {
	kubeClusterClient    kcpkubernetesclientset.ClusterInterface
	dynamicClusterClient kcpdynamic.ClusterInterface
	cachedKCPInformers   kcpinformers.SharedInformerFactory
	rootPathPrefix       string
}

type templateParameters struct {
	virtualWorkspaceName string

	restProviderBuilder   BuildRestProviderFunc
	allowedAPIFilter      apireconciler.AllowedAPIfilterFunc
	transformer           transforming.ResourceTransformer
	storageWrapperBuilder func(labels.Requirements) forwardingregistry.StorageWrapper
}

func (p *templateProvider) newTemplate(parameters templateParameters) *template {
	return &template{
		templateProvider:   *p,
		templateParameters: parameters,
		readyCh:            make(chan struct{}),
	}
}

type template struct {
	templateProvider
	templateParameters

	readyCh chan struct{}
}

func (t *template) resolveRootPath(urlPath string, requestContext context.Context) (accepted bool, prefixToStrip string, completedContext context.Context) {
	// This function is used by the handlers for the /services path; all virtualworkspace implementation are called, and they
	// can accept the request if it is targeted to them. Then the request url is stripper by the first part and the
	// remaining part gets handled by kcp "default" handles (it is the mechanism that forwards a request from the
	// virtual workspace to the real workspace).

	select {
	case <-t.readyCh:
	default:
		return
	}

	rootPathPrefix := t.rootPathPrefix + t.virtualWorkspaceName + "/"
	completedContext = requestContext
	if !strings.HasPrefix(urlPath, rootPathPrefix) {
		return
	}
	withoutRootPathPrefix := strings.TrimPrefix(urlPath, rootPathPrefix)

	// Incoming requests to this virtual workspace will look like:
	//  /services/user/root:org:ws/clusters/*/api/v1/configmaps
	//                └─────────────────────────────┐
	// Where the withoutRootPathPrefix starts here: ┘
	parts := strings.SplitN(withoutRootPathPrefix, "/", 2)
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return
	}
	path := logicalcluster.NewPath(parts[0])

	clusterName, ok := path.Name()
	if !ok {
		return
	}

	realPath := "/"
	if len(parts) > 1 {
		realPath += parts[1]
	}

	//  /services/user/root:org:ws/clusters/*/api/v1/configmaps
	//                  ┌─────────┘
	// We are now here: ┘
	// Now, we parse out the logical cluster.
	if !strings.HasPrefix(realPath, "/clusters/") {
		return // don't accept
	}

	withoutClustersPrefix := strings.TrimPrefix(realPath, "/clusters/")
	parts = strings.SplitN(withoutClustersPrefix, "/", 2)
	reqPath := logicalcluster.NewPath(parts[0])
	realPath = "/"
	if len(parts) > 1 {
		realPath += parts[1]
	}
	var cluster genericapirequest.Cluster
	if reqPath == logicalcluster.Wildcard {
		cluster.Wildcard = true
	} else {
		reqClusterName, ok := reqPath.Name()
		if !ok {
			return
		}
		cluster.Name = reqClusterName
	}

	// TODO: currently our virtual workspace is 1:1 with a workspace.
	//   investigate if/how we can make our view cross workspaces.
	if clusterName != cluster.Name {
		return
	}

	apiDomainKey := dynamiccontext.APIDomainKey(clusterName.String())

	// NOTE: those info can be used only in the lifecyle of this request (e.g authorize), not in the reconciler
	// TODO: investigate if we really need WithCluster (we are not using it in authorize).
	// TODO: investigate if the order matters (had some problems in on test)
	completedContext = genericapirequest.WithCluster(requestContext, cluster)
	completedContext = dynamiccontext.WithAPIDomainKey(completedContext, apiDomainKey)

	prefixToStrip = strings.TrimSuffix(urlPath, realPath)
	accepted = true
	return
}

func (t *template) ready() error {
	select {
	case <-t.readyCh:
		return nil
	default:
		return errors.New("syncer virtual workspace controllers are not started")
	}
}

func (t *template) authorize(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
	// This function is called to authorize each API request.
	// in this version we are relying on the underlying workspace for authorization, so this is a simple pass-trough
	apiDomainKey := dynamiccontext.APIDomainKeyFrom(ctx)

	authz, err := delegated.NewDelegatedAuthorizer(logicalcluster.Name(apiDomainKey), t.kubeClusterClient, delegated.Options{})
	if err != nil {
		return authorizer.DecisionNoOpinion, "Error", err
	}
	SARAttributes := a

	authorized, reason, err := authz.Authorize(ctx, SARAttributes)
	return authorized, reason, err
}

func (t *template) bootstrapManagement(mainConfig genericapiserver.CompletedConfig) (apidefinition.APIDefinitionSetGetter, error) {
	// This function start the apireconciler, which ultimate responsibility is to update the lit of api resourced being served.
	// NOTE: the reconciler is a apidefinition.APIDefinitionSetGetter !!!

	apiReconciler, err := apireconciler.NewAPIReconciler(
		t.virtualWorkspaceName,
		t.cachedKCPInformers.Apis().V1alpha1().APIBindings(),
		t.cachedKCPInformers.Apis().V1alpha1().APIResourceSchemas(),
		func(apiBindingWorkspace logicalcluster.Name, apiBindingName string, apiResourceSchema *apisv1alpha1.APIResourceSchema, version string, identityHash string) (apidefinition.APIDefinition, error) {
			// This function is called by the apireconciler when creating the apidefinition for a given apiResourceSchema.

			// TODO: investigate this better (storageWrapper/storageBuilder)
			requirements, _ := labels.Everything().Requirements()
			storageWrapper := t.storageWrapperBuilder(requirements)
			ctx, cancelFn := context.WithCancel(context.Background())
			storageBuilder := t.restProviderBuilder(ctx, t.dynamicClusterClient, identityHash, storageWrapper)
			def, err := apiserver.CreateServingInfoFor(mainConfig, apiResourceSchema, version, storageBuilder)
			if err != nil {
				cancelFn()
				return nil, err
			}
			return &apiDefinitionWithCancel{
				APIDefinition: def,
				cancelFn:      cancelFn,
			}, nil
		},
		t.allowedAPIFilter,
	)
	if err != nil {
		return nil, err
	}

	if err := mainConfig.AddPostStartHook(apireconciler.ControllerName+t.virtualWorkspaceName, func(hookContext genericapiserver.PostStartHookContext) error {
		defer close(t.readyCh)

		for name, informer := range map[string]cache.SharedIndexInformer{
			"apibindings":        t.cachedKCPInformers.Apis().V1alpha1().APIBindings().Informer(),
			"apiresourceschemas": t.cachedKCPInformers.Apis().V1alpha1().APIResourceSchemas().Informer(),
		} {
			if !cache.WaitForNamedCacheSync(name, hookContext.StopCh, informer.HasSynced) {
				klog.Background().Error(nil, "informer not synced")
				return nil
			}
		}

		go apiReconciler.Start(goContext(hookContext))
		return nil
	}); err != nil {
		return nil, err
	}

	return apiReconciler, nil
}

func (t *template) buildVirtualWorkspace() *virtualworkspacesdynamic.DynamicVirtualWorkspace {
	return &virtualworkspacesdynamic.DynamicVirtualWorkspace{
		RootPathResolver:          framework.RootPathResolverFunc(t.resolveRootPath),
		Authorizer:                authorizer.AuthorizerFunc(t.authorize),
		ReadyChecker:              framework.ReadyFunc(t.ready),
		BootstrapAPISetManagement: t.bootstrapManagement,
	}
}

// apiDefinitionWithCancel calls the cancelFn on tear-down.
type apiDefinitionWithCancel struct {
	apidefinition.APIDefinition
	cancelFn func()
}

func (d *apiDefinitionWithCancel) TearDown() {
	d.cancelFn()
	d.APIDefinition.TearDown()
}

func goContext(parent genericapiserver.PostStartHookContext) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func(done <-chan struct{}) {
		<-done
		cancel()
	}(parent.StopCh)
	return ctx
}
