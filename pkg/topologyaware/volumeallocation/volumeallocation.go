package volumeallocation

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"

	corev1 "k8s.io/api/core/v1"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
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
	PodRediness         sync.Map
}

var (
	_ framework.PreBindPlugin  = &TopologyAwareVolumeAllocation{}
	_ framework.PreScorePlugin = &TopologyAwareVolumeAllocation{}
)

const (
	Name                                  = "TopologyAwareVolumeAllocation"
	TopologyAwareVolumeAllocationStateKey = "TopologyAwareVolumeAllocationStateKey"
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

// state computed at PreScore and used at prebind.
type TopologyAwareVolumeAllocationState struct {
	availableDiskCache map[string]int
}

// Clone implements the mandatory Clone interface. We don't really copy the data since
// there is no need for that.
func (s *TopologyAwareVolumeAllocationState) Clone() framework.StateData {
	return s
}

// PreScore implements framework.PreScorePlugin.
func (ta *TopologyAwareVolumeAllocation) PreScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*framework.NodeInfo) *framework.Status {
	topologyAwareVolumeAllocationState := &TopologyAwareVolumeAllocationState{
		availableDiskCache: make(map[string]int),
	}
	// Wait for pod to finish sync
	key := pod.Namespace + "/" + pod.Name
	if _, ready := ta.PodRediness.Load(key); !ready {
		klog.Infof("Pod %s is not ready for prescoring yet", key)
		return framework.NewStatus(framework.Pending)
	}
	for _, node := range nodes {
		available_disk := node.Node().Annotations["topology-aware-scheduling.cs.phd.uqtr/available_disk"]
		ta.logger.Info(fmt.Sprintf("Available disk %s", available_disk))
		availableDisk, err := strconv.Atoi(available_disk)
		if err != nil {
			return framework.NewStatus(framework.Error)
		}
		topologyAwareVolumeAllocationState.availableDiskCache[node.GetName()] = availableDisk
	}
	state.Write(TopologyAwareVolumeAllocationStateKey, topologyAwareVolumeAllocationState)
	return nil
}

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
	volumeAllocation := ta.DistributedVolumeAllocation(ctx, state, allocationRequest, edgeTopologyName, 3) // TODO, i need to change kmax to be configurable
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

func (ta *TopologyAwareVolumeAllocation) DistributedVolumeAllocation(ctx context.Context, state *framework.CycleState, allocationRequest topologycrdv1.VolumeAllocation, edgeTopologyName types.NamespacedName, kMax int) map[string]int {
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
	// volumeQuantity.Value() returns size in bytes
	blocks := int(volumeQuantity.Value() / (1024 * 1024)) // Convert bytes to MB

	result := make(map[string]int)

	eStar := allocationRequest.Spec.MicroservicePlacement
	remaining := blocks

	for k := 0; k <= kMax; k++ {
		neighbors := KHopNeighbors(edgeNetworkTopology.Spec.Edges, eStar, k)

		// as for now, we sort neighbors alphabetically for testing purposes and can sort by affinity later
		sort.Strings(neighbors)

		for _, e := range neighbors {
			available := ta.GetAvailableDisk(state, e)
			if remaining > 0 && available > 0 {
				alloc := min(remaining, available)
				result[e] += alloc
				ta.UpdateDiskAvailability(state, e, available-alloc)
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

func (ta *TopologyAwareVolumeAllocation) GetAvailableDisk(state *framework.CycleState, node string) int {
	// TODO: Hook into actual storage monitoring or state cache
	stateData, err := state.Read(TopologyAwareVolumeAllocationStateKey)
	if err != nil {
		return 0
	}
	topologyAwareVolumeAllocationState, ok := stateData.(*TopologyAwareVolumeAllocationState)
	if !ok {
		return 0
	}
	return topologyAwareVolumeAllocationState.availableDiskCache[node]
}

func (ta *TopologyAwareVolumeAllocation) UpdateDiskAvailability(state *framework.CycleState, node string, newAvailable int) {
	// TODO: Update the disk availability in the actual state or cache
	// TODO: Make sure that this is no race condition
	stateData, err := state.Read(TopologyAwareVolumeAllocationStateKey)
	if err != nil {
		return
	}
	topologyAwareVolumeAllocationState, ok := stateData.(*TopologyAwareVolumeAllocationState)
	if !ok {
		return
	}
	topologyAwareVolumeAllocationState.availableDiskCache[node] = newAvailable
	// Update node annotation
	nodeObj, err := ta.handle.ClientSet().CoreV1().Nodes().Get(context.TODO(), node, metav1.GetOptions{})
	if err != nil {
		return
	}
	if nodeObj.Annotations == nil {
		nodeObj.Annotations = make(map[string]string)
	}

	nodeObj.Annotations["topology-aware-scheduling.cs.phd.uqtr/available_disk"] = strconv.Itoa(newAvailable)
	_, err = ta.handle.ClientSet().CoreV1().Nodes().Update(context.TODO(), nodeObj, metav1.UpdateOptions{})
	if err != nil {
		return
	}
	state.Write(TopologyAwareVolumeAllocationStateKey, topologyAwareVolumeAllocationState)
}

func (ta *TopologyAwareVolumeAllocation) handlePod(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return
	}
	key := pod.Namespace + "/" + pod.Name
	if _, ok := pod.Annotations["topology-aware-scheduling.cs.phd.uqtr/microservice"]; ok {
		ta.PodRediness.Store(key, true)
	} else {
		ta.PodRediness.Delete(key)
	}
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
	var ta *TopologyAwareVolumeAllocation = &TopologyAwareVolumeAllocation{
		Client: c,
		handle: handle,
		logger: logger,
		// EdgeNetworkTopology: args.EdgeNetworkTopology,
		EdgeNetworkTopology: "test-topology",
	}

	sharedInformerFactory := informers.NewSharedInformerFactory(handle.ClientSet(), 0)
	podInformer := sharedInformerFactory.Core().V1().Pods().Informer()
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ta.handlePod,
		UpdateFunc: func(oldObj, newObj interface{}) { ta.handlePod(newObj) },
		DeleteFunc: func(obj interface{}) {
			if pod, ok := obj.(*v1.Pod); ok {
				ta.PodRediness.Delete(pod.Namespace + "/" + pod.Name)
			}
		},
	})
	go podInformer.Run(ctx.Done())
	return ta, nil
}
