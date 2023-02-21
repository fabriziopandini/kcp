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
	syncerbuiltin "github.com/kcp-dev/kcp/pkg/virtual/user/schemas/builtin"
	"github.com/kcp-dev/logicalcluster/v3"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"

	apisv1alpha1 "github.com/kcp-dev/kcp/pkg/apis/apis/v1alpha1"
	"github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/apidefinition"
	dynamiccontext "github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/context"
)

func (c *APIReconciler) reconcile(ctx context.Context, apiDomainKey dynamiccontext.APIDomainKey, apiBinding *apisv1alpha1.APIBinding) error {
	// TODO: uncomment and fix the code that handles when an API should be stopped to serve or updated.
	//  main difference from the syncer, is that we want to serve APIs from all bindings vs serving API only for a specific SyncTarget.
	//  for now we fixed this by always appending to apiSets, but this is just a temporary workaround.

	c.mutex.RLock()
	newSet := c.apiSets[apiDomainKey]
	if newSet == nil {
		newSet = apidefinition.APIDefinitionSet{}
	}
	c.mutex.RUnlock()

	logger := klog.FromContext(ctx)

	// collect APIResourceSchemas by syncTarget.
	apiResourceSchemas, schemaIdentites, err := c.getAllAcceptedResourceSchemas(ctx, apiBinding)
	if err != nil {
		return err
	}

	if len(newSet) == 0 {
		// add built-in apiResourceSchema
		for _, apiResourceSchema := range syncerbuiltin.UserSchemas {
			shallow := *apiResourceSchema
			if shallow.Annotations == nil {
				shallow.Annotations = make(map[string]string)
			}
			shallow.Annotations[logicalcluster.AnnotationKey] = logicalcluster.From(apiBinding).String()
			apiResourceSchemas[schema.GroupResource{
				Group:    apiResourceSchema.Spec.Group,
				Resource: apiResourceSchema.Spec.Names.Plural,
			}] = &shallow
		}
	}

	// reconcile APIs for APIResourceSchemas
	newGVRs := []string{}
	// preservedGVR := []string{}
	for gr, apiResourceSchema := range apiResourceSchemas {
		if c.allowedAPIfilter != nil && !c.allowedAPIfilter(gr) {
			continue
		}

		for _, version := range apiResourceSchema.Spec.Versions {
			if !version.Served {
				continue
			}

			gvr := schema.GroupVersionResource{
				Group:    gr.Group,
				Version:  version.Name,
				Resource: gr.Resource,
			}

			// oldDef, found := oldSet[gvr]
			// if found {
			// 	oldDef := oldDef.(apiResourceSchemaApiDefinition)
			// 	if oldDef.UID != apiResourceSchema.UID {
			// 		logging.WithObject(logger, apiResourceSchema).V(4).Info("APIResourceSchema UID has changed:", "oldUID", oldDef.UID, "newUID", apiResourceSchema.UID)
			// 	}
			// 	if oldDef.IdentityHash != schemaIdentites[gr] {
			// 		logging.WithObject(logger, apiResourceSchema).V(4).Info("APIResourceSchema identity hash has changed", "oldIdentityHash", oldDef.IdentityHash, "newIdentityHash", schemaIdentites[gr])
			// 	}
			// 	if oldDef.UID == apiResourceSchema.UID && oldDef.IdentityHash == schemaIdentites[gr] {
			// 		// this is the same schema and identity as before. no need to update.
			// 		newSet[gvr] = oldDef
			// 		preservedGVR = append(preservedGVR, gvrString(gvr))
			// 		continue
			// 	}
			// }

			apiDefinition, err := c.createAPIDefinition(logicalcluster.From(apiBinding), apiBinding.Name, apiResourceSchema, version.Name, schemaIdentites[gr])
			if err != nil {
				logger.WithValues("gvr", gvr).Error(err, "failed to create API definition")
				continue
			}

			newSet[gvr] = apiResourceSchemaApiDefinition{
				APIDefinition: apiDefinition,
				UID:           apiResourceSchema.UID,
				IdentityHash:  schemaIdentites[gr],
			}
			newGVRs = append(newGVRs, gvrString(gvr))
		}
	}

	// // cleanup old definitions
	// removedGVRs := []string{}
	// for gvr, oldDef := range oldSet {
	// 	if _, found := newSet[gvr]; !found || oldDef != newSet[gvr] {
	// 		removedGVRs = append(removedGVRs, gvrString(gvr))
	// 		oldDef.TearDown()
	// 	}
	// }

	// logging.WithObject(logger, apiBinding).WithValues("APIDomainKey", apiDomainKey).V(2).Info("Updating APIs for APIBinding and APIDomainKey", "newGVRs", newGVRs, "preservedGVRs", preservedGVR, "removedGVRs", removedGVRs)

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.apiSets[apiDomainKey] = newSet

	return nil
}

type apiResourceSchemaApiDefinition struct {
	apidefinition.APIDefinition

	UID          types.UID
	IdentityHash string
}

func gvrString(gvr schema.GroupVersionResource) string {
	group := gvr.Group
	if group == "" {
		group = "core"
	}
	return fmt.Sprintf("%s.%s.%s", gvr.Resource, gvr.Version, group)
}

// getAllAcceptedResourceSchemas return all resourceSchemas from APIBindings, together with their identityHash.
func (c *APIReconciler) getAllAcceptedResourceSchemas(ctx context.Context, apiBinding *apisv1alpha1.APIBinding) (map[schema.GroupResource]*apisv1alpha1.APIResourceSchema, map[schema.GroupResource]string, error) {
	apiResourceSchemas := map[schema.GroupResource]*apisv1alpha1.APIResourceSchema{}

	// TODO: check if we need this, because we are not serving cross workspaces and thus it is not possible to have
	//   different versions of the same API.
	identityHashByGroupResource := map[schema.GroupResource]string{}

	logger := klog.FromContext(ctx)
	logger.V(4).Info("getting identity hashes for compatible APIs", "count", len(apiBinding.Status.BoundResources))

	var errs []error

	// get all identityHash for compatible APIs
	for _, boundResource := range apiBinding.Status.BoundResources {
		logger := logger.WithValues(
			"group", boundResource.Group,
			"resource", boundResource.Resource,
			"identity", boundResource.Schema.IdentityHash,
		)
		logger.V(4).Info("including bound resource")
		identityHashByGroupResource[schema.GroupResource{
			Group:    boundResource.Group,
			Resource: boundResource.Resource,
		}] = boundResource.Schema.IdentityHash

		apiResourceSchemaClusterName, _ := logicalcluster.NewPath(apiBinding.Spec.Reference.Export.Path).Name()
		apiResourceSchemaName := boundResource.Schema.Name
		apiResourceSchema, err := c.apiResourceSchemaLister.Cluster(apiResourceSchemaClusterName).Get(apiResourceSchemaName)
		if apierrors.IsNotFound(err) {
			logger.V(4).Info("APIResourceSchema not found")
			continue
		}
		if err != nil {
			logger.V(4).Error(err, "error getting APIResourceSchema")
			errs = append(errs, err)
			continue
		}

		gr := schema.GroupResource{
			Group:    apiResourceSchema.Spec.Group,
			Resource: apiResourceSchema.Spec.Names.Plural,
		}

		logger = logger.WithValues("group", gr.Group, "resource", gr.Resource)

		// if identityHash does not exist, it is not a compatible API.
		if _, ok := identityHashByGroupResource[gr]; ok {
			logger.V(4).Info("identity found, including resource")
			apiResourceSchemas[gr] = apiResourceSchema
		} else {
			logger.V(4).Info("identity not found, excluding resource")
		}
	}

	return apiResourceSchemas, identityHashByGroupResource, errors.NewAggregate(errs)
}
