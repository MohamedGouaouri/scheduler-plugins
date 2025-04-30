package volumeallocation

import (
	"context"
	"errors"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/controller-runtime/pkg/client"

	topologycrdv1 "github.com/MohamedGouaouri/ms-app-controller/api/v1"
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
	utilruntime.Must(topologycrdv1.AddToScheme(scheme))

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

	msName := p.Annotations["topology-aware-scheduling.cs.phd.uqtr/microservice"]
	volSizeStr := p.Annotations["topology-aware-scheduling.cs.phd.uqtr/volume_size"]
	claimName := p.Annotations["topology-aware-scheduling.cs.phd.uqtr/claim_name"]

	// // Create the VolumeAllocation object
	allocationRequest := topologycrdv1.VolumeAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      claimName,
			Namespace: p.Namespace,
		},
		Spec: topologycrdv1.VolumeAllocationSpec{
			StorageClassName:      "", // Optionally set a default
			Microservice:          msName,
			MicroservicePlacement: nodeName,
			VolumeSize:            volSizeStr,
			EdgeNetworkTopology:   ta.EdgeNetworkTopology,
		},
	}

	// // Create the resource in the cluster
	// if err := ta.Create(ctx, allocation); err != nil {
	// 	ta.logger.Error(err, "failed to create VolumeAllocation object")
	// 	return framework.NewStatus(framework.Error, err.Error())
	// }

	// Create PVC
	annotations := make(map[string]string)
	edgeTopologyName := types.NamespacedName{
		Name:      ta.EdgeNetworkTopology,
		Namespace: p.Namespace,
	}
	volumeAllocation := ta.DistributedVolumeAllocation(ctx, allocationRequest, edgeTopologyName, 3) // TODO, i need to change kmax to be configurable
	annotation := ""
	for edge, allocation := range volumeAllocation {
		annotation += fmt.Sprintf("%s:%d,", edge, allocation)
	}
	annotations["volumeallocation"] = annotation
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        allocationRequest.Name,
			Namespace:   allocationRequest.Namespace,
			Annotations: annotations,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(allocationRequest.Spec.VolumeSize),
				},
			},
			StorageClassName: &allocationRequest.Spec.StorageClassName,
		},
	}

	// Create the PVC if it doesn't exist
	var existingPVC corev1.PersistentVolumeClaim
	err := ta.Get(ctx, client.ObjectKey{Name: pvc.Name, Namespace: pvc.Namespace}, &existingPVC)
	if err != nil && client.IgnoreNotFound(err) == nil {
		ta.logger.Info("Creating PVC", "pvc", pvc.Name)
		if err := ta.Create(ctx, pvc); err != nil {
			ta.logger.Error(err, "failed to create PVC")
			return framework.NewStatus(framework.Error)
		}
	} else if err != nil {
		ta.logger.Error(err, "error checking for existing PVC")
		return framework.NewStatus(framework.Error)
	} else {
		ta.logger.Info("PVC already exists", "pvc", pvc.Name)
	}
	ta.logger.Info("Successfully created VolumeAllocation object", "name", allocationRequest.Name)
	return framework.NewStatus(framework.Success)
}

func (ta *TopologyAwareVolumeAllocation) DistributedVolumeAllocation(ctx context.Context, allocationRequest topologycrdv1.VolumeAllocation, edgeTopologyName types.NamespacedName, kMax int) map[string]int {
	var edgeNetworkTopology topologycrdv1.EdgeNetworkTopology

	if err := ta.Get(ctx, edgeTopologyName, &edgeNetworkTopology); err != nil {
		ta.logger.Error(err, "unable to fetch EdgeNetworkTopology")
		return nil
	}

	volumeQuantity, err := resource.ParseQuantity(allocationRequest.Spec.VolumeSize)
	if err != nil {
		ta.logger.Error(err, "unable to parse volume size")
		return nil
	}
	blocks := int(volumeQuantity.Value() / (1024 * 1024)) // Convert bytes to MB

	result := make(map[string]int)

	eStar := allocationRequest.Spec.MicroservicePlacement
	remaining := blocks

	for k := 0; k <= kMax; k++ {
		neighbors := KHopNeighbors(edgeNetworkTopology.Spec.Edges, eStar, k)

		// as for now, we sort neighbors alphabetically for testing purposes and can sort by affinity later
		sort.Strings(neighbors)

		for _, e := range neighbors {
			available := ta.GetAvailableDisk(e)
			if remaining > 0 && available > 0 {
				alloc := min(remaining, available)
				result[e] += alloc
				ta.UpdateDiskAvailability(e, available-alloc)
				remaining -= alloc
			}
		}

		if remaining == 0 {
			break
		}
	}

	// if remaining > 0 {
	// result["cloud"] += remaining // fallback to cloud
	// }

	return result
}

// Find K-hop neighbors of an edge node
func KHopNeighbors(edges []topologycrdv1.EdgeNode, start string, k int) []string {
	// BFS to find k-hop neighbors
	visited := make(map[string]bool)
	current := []string{start}
	visited[start] = true

	for depth := 0; depth < k; depth++ {
		next := []string{}
		for _, node := range current {
			for _, e := range edges {
				if e.Name == node {
					for _, link := range e.Links {
						if !visited[link.EdgeNodeRef] {
							visited[link.EdgeNodeRef] = true
							next = append(next, link.EdgeNodeRef)
						}
					}
				}
			}
		}
		current = next
	}

	return current
}

func (ta *TopologyAwareVolumeAllocation) GetAvailableDisk(node string) int {
	// TODO: Hook into actual storage monitoring or state cache
	return 10240 // Placeholder: 10Gi in MB
}

func (ta *TopologyAwareVolumeAllocation) UpdateDiskAvailability(node string, newAvailable int) {
	// TODO: Update the disk availability in the actual state or cache
}

// New initializes a new plugin and returns it.
func New(ctx context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	logger := klog.FromContext(ctx).WithValues("plugin", Name)
	logger.V(4).Info("Creating new instance of the TopologicalSort plugin")

	args, err := getArgs(obj)
	if err != nil {
		return nil, err
	}
	fmt.Println("Args", args, args == nil)
	c, _, err := util.NewClientWithCachedReader(ctx, handle.KubeConfig(), scheme)
	if err != nil {
		return nil, err
	}
	fmt.Println("K8s client", c, c == nil)
	return &TopologyAwareVolumeAllocation{
		Client: c,
		handle: handle,
		logger: logger,
		// EdgeNetworkTopology: args.EdgeNetworkTopology,
		EdgeNetworkTopology: "test-topology",
	}, nil
}
