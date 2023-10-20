package hypershift

import (
	"context"
	"fmt"

	"github.com/golang/glog"
	"github.com/openshift-kni/eco-goinfra/pkg/clients"
	"github.com/openshift-kni/eco-goinfra/pkg/msg"
	hypershiftV1Beta1 "github.com/openshift/hypershift/api/v1beta1"
	coreV1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	goclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// Builder provides struct for hostedcluster object containing connection to the cluster and the hostedcluster definitions.
type HostedClusterBuilder struct {
	// NodePool definition. Used to create the nodepool object.
	Definition *hypershiftV1Beta1.NodePool
	// Created nodepool object
	Object *hypershiftV1Beta1.NodePool
	// Used in functions that define or mutate deployment definition. errorMsg is processed before the deployment
	// object is created.
	errorMsg  string
	apiClient *clients.Settings
}

// NewNodePoolBuilder creates a new instance of
// NodePoolBuilder with platform type set to agent.
func NewNodePoolBuilder(
	apiClient *clients.Settings,
	name string,
	nsname string,
	clusterName string,
	agentNamespace string,
	release string,
	replicas int32) *NodePoolBuilder {
	glog.V(100).Infof(
		`Initializing new nodepool object with the following params: name: %s, namespace: %s,
		  clusterName: %s, agentNamespace: %s, release: %s, replicas: %s`,
		name, nsname, clusterName, agentNamespace, release, replicas)

	builder := NodePoolBuilder{
		apiClient: apiClient,
		Definition: &hypershiftV1Beta1.NodePool{
			ObjectMeta: metaV1.ObjectMeta{
				Name:      name,
				Namespace: nsname,
			},
			Spec: hypershiftV1Beta1.NodePoolSpec{
				ClusterName: clusterName,
				Release:  release,
				Replicas: replicas,
				Platform: hypershiftV1Beta1.NodePoolPlatform{
					Type: hypershiftV1Beta1.PlatformType.AgentPlatform,
					},
				},
			},
		}

	if name == "" {
		glog.V(100).Infof("The name of the nodepool is empty")

		builder.errorMsg = "nodepool 'name' cannot be empty"
	}

	if nsname == "" {
		glog.V(100).Infof("The namespace of the nodepool is empty")

		builder.errorMsg = "nodepool 'namespace' cannot be empty"
	}

	if clusterName == "" {
		glog.V(100).Infof("The clusterName of the nodepool is empty")

		builder.errorMsg = "nodepool 'clusterName' cannot be empty"
	}

	if release == "" {
		glog.V(100).Infof("The release of the nodepool is empty")

		builder.errorMsg = "nodepool 'release' cannot be empty"
	}

	return &builder
}

func (builder *NodePoolBuilder) WithReplicas(replicas *int32) *NodePoolBuilder {
	if valid, _ := builder.validate(); !valid {
		return builder
	}

	glog.V(100).Infof(
		"Scaling nodepool %s to %s replicas",
		builder.Definition.Name, replicas)

	builder.Definition.Spec.Replicas = replicas
	return builder
}

// PullNodePool pulls existing nodepool from cluster.
func PullNodePool(apiClient *clients.Settings, name, nsname string) (*NodePoolBuilder, error) {
	glog.V(100).Infof("Pulling existing nodepool name %s under namespace %s from cluster", name, nsname)

	builder := NodePoolBuilder{
		apiClient: apiClient,
		Definition: &hypershiftV1Beta1.NodePool{
			ObjectMeta: metaV1.ObjectMeta{
				Name:      name,
				Namespace: nsname,
			},
		},
	}

	if name == "" {
		glog.V(100).Infof("The name of the nodepool is empty")

		builder.errorMsg = "nodepool 'name' cannot be empty"
	}

	if nsname == "" {
		glog.V(100).Infof("The namespace of the nodepool is empty")

		builder.errorMsg = "nodepool 'namespace' cannot be empty"
	}

	if !builder.Exists() {
		return nil, fmt.Errorf("nodepool object %s doesn't exist in namespace %s", name, nsname)
	}

	builder.Definition = builder.Object

	return &builder, nil
}


// Get fetches the defined clusterdeployment from the cluster.
func (builder *NodePoolBuilder) Get() (*hypershiftV1Beta1.NodePool, error) {
	if valid, err := builder.validate(); !valid {
		return nil, err
	}

	glog.V(100).Infof("Getting nodepool %s in namespace %s",
		builder.Definition.Name, builder.Definition.Namespace)

	nodePool := &hypershiftV1Beta1.NodePool{}
	err := builder.apiClient.Get(context.TODO(), goclient.ObjectKey{
		Name:      builder.Definition.Name,
		Namespace: builder.Definition.Namespace,
	}, nodePool)

	if err != nil {
		return nil, err
	}

	return nodePool, err
}