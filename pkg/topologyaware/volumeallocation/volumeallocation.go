package volumeallocation

import (
	"context"
	"errors"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/controller-runtime/pkg/client"

	allocationrequestv1 "github.com/MohamedGouaouri/ms-app-controller/api/v1"
	pluginconfig "sigs.k8s.io/scheduler-plugins/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
)

// NodeNumber is
type TopologyAwareVolumeAllocation struct {
	client.Client
	logger              klog.Logger
	handle              framework.Handle
	EdgeNetworkTopology string
}

var (
	_ framework.PreBindPlugin = &TopologyAwareVolumeAllocation{}
)

const (
	Name = "TopologyAwareVolumeAllocation"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

}

// Name returns the name of the plugin. It is used in logs, etc.
func (ta *TopologyAwareVolumeAllocation) Name() string {
	return Name
}

// getArgs : returns the arguments for the TopologicalSort plugin.
func getArgs(obj runtime.Object) (*pluginconfig.TopologyAwareVolumeAllocationArgs, error) {
	TopologyAwareVolumeAllocationArgs, ok := obj.(*pluginconfig.TopologyAwareVolumeAllocationArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type TopologyAwareVolumeAllocationArgs, got %T", obj)
	}

	return TopologyAwareVolumeAllocationArgs, nil
}

func (ta *TopologyAwareVolumeAllocation) EventsToRegister() []framework.ClusterEvent {
	return []framework.ClusterEvent{
		{Resource: framework.Node, ActionType: framework.Add},
	}
}

var ErrNotExpectedPreScoreState = errors.New("unexpected pre score state")

func (ta *TopologyAwareVolumeAllocation) PreBind(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) *framework.Status {
	ta.logger.Info("Invoking PreBind plugin")

	// Extract annotations from Pod
	msName := p.Annotations["topology-aware-scheduling.cs.phd.uqtr/microservice"]
	volSizeStr := p.Annotations["topology-aware-scheduling.cs.phd.uqtr/volume_size"]

	// Create the VolumeAllocation object
	allocation := &allocationrequestv1.VolumeAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.Name + "-pvc",
			Namespace: p.Namespace,
		},
		Spec: allocationrequestv1.VolumeAllocationSpec{
			StorageClassName:      "", // Optionally set a default
			Microservice:          msName,
			MicroservicePlacement: nodeName,
			VolumeSize:            volSizeStr,
			EdgeNetworkTopology:   ta.EdgeNetworkTopology,
		},
	}

	// Create the resource in the cluster
	if err := ta.Create(ctx, allocation); err != nil {
		ta.logger.Error(err, "failed to create VolumeAllocation object")
		return framework.NewStatus(framework.Error, err.Error())
	}

	ta.logger.Info("Successfully created VolumeAllocation object", "name", allocation.Name)
	return framework.NewStatus(framework.Success)
}

// New initializes a new plugin and returns it.
func New(ctx context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	logger := klog.FromContext(ctx).WithValues("plugin", Name)
	logger.V(4).Info("Creating new instance of the TopologicalSort plugin")

	args, err := getArgs(obj)
	if err != nil {
		return nil, err
	}

	c, _, err := util.NewClientWithCachedReader(ctx, handle.KubeConfig(), scheme)
	if err != nil {
		return nil, err
	}
	return &TopologyAwareVolumeAllocation{
		Client:              c,
		handle:              handle,
		logger:              logger,
		EdgeNetworkTopology: args.EdgeNetworkTopology,
	}, nil
}
