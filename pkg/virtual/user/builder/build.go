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
	"strings"

	kcpdynamic "github.com/kcp-dev/client-go/dynamic"
	kcpkubernetesclientset "github.com/kcp-dev/client-go/kubernetes"

	kcpinformers "github.com/kcp-dev/kcp/pkg/client/informers/externalversions"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/forwardingregistry"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/rootapiserver"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	UserVirtualWorkspaceName string = "user"
)

// BuildVirtualWorkspace builds two virtual workspaces, SyncerVirtualWorkspace and UpsyncerVirtualWorkspace by instantiating a DynamicVirtualWorkspace which,
// combined with a ForwardingREST REST storage implementation, serves a SyncTargetAPI list maintained by the APIReconciler controller.
func BuildVirtualWorkspace(
	rootPathPrefix string,
	kubeClusterClient kcpkubernetesclientset.ClusterInterface,
	dynamicClusterClient kcpdynamic.ClusterInterface,
	cachedKCPInformers kcpinformers.SharedInformerFactory,
) []rootapiserver.NamedVirtualWorkspace {
	if !strings.HasSuffix(rootPathPrefix, "/") {
		rootPathPrefix += "/"
	}

	// the template provider builds a DynamicVirtualWorspace, which handle a variable list of APIs
	// which is changed dynamically di the apireconciler controller

	provider := templateProvider{
		kubeClusterClient:    kubeClusterClient,
		dynamicClusterClient: dynamicClusterClient,
		cachedKCPInformers:   cachedKCPInformers,
		rootPathPrefix:       rootPathPrefix,
	}

	return []rootapiserver.NamedVirtualWorkspace{
		{
			Name: UserVirtualWorkspaceName,
			VirtualWorkspace: provider.newTemplate(templateParameters{
				virtualWorkspaceName: UserVirtualWorkspaceName,
				restProviderBuilder:  NewUserRestProvider, // TODO: investigate this better
				allowedAPIFilter: func(apiGroupResource schema.GroupResource) bool {
					// This function is used by the apireconciler controller to drop apiGroupResource we do not want to be served by the virtual workspace.

					// Don't expose Endpoints or Pods via the Syncer VirtualWorkspace.
					if apiGroupResource.Group == "" && (apiGroupResource.Resource == "pods" || apiGroupResource.Resource == "endpoints") {
						return false
					}

					if strings.HasSuffix(apiGroupResource.Group, "kcp.io") {
						return false
					}

					return true
				},
				transformer:           nil,
				storageWrapperBuilder: forwardingregistry.WithStaticLabelSelector, // TODO: investigate this better
			}).buildVirtualWorkspace(),
		},
	}
}
