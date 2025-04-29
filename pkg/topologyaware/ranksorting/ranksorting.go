package ranksorting

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
)

// NodeNumber is
type RankBasedSorting struct {
	client.Client
	logger klog.Logger
	handle framework.Handle
}

var (
	_ framework.QueueSortPlugin = &RankBasedSorting{}
)

const (
	Name = "RankBasedSorting"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

}

// Name returns the name of the plugin. It is used in logs, etc.
func (ta *RankBasedSorting) Name() string {
	return Name
}

// // getArgs : returns the arguments for the TopologicalSort plugin.
// func getArgs(obj runtime.Object) (*pluginconfig.RankBasedSortingArgs, error) {
// 	RankBasedSortingArgs, ok := obj.(*pluginconfig.RankBasedSortingArgs)
// 	if !ok {
// 		return nil, fmt.Errorf("want args to be of type RankBasedSortingArgs, got %T", obj)
// 	}

//		return RankBasedSortingArgs, nil
//	}
func (ta *RankBasedSorting) Less(pInfo1 *framework.QueuedPodInfo, pInfo2 *framework.QueuedPodInfo) bool {
	logger := ta.logger.WithValues("ExtensionPoint", "Less")
	// TODO: Change this example code
	// Ranks are calculated by the Application controller
	// We need to fetch the ranks by microservice name
	// Suppose that are annotated with name of microservice
	fmt.Printf("Comparing between pod %s and %s\n", pInfo1.Pod.Name, pInfo2.Pod.Name)
	logger.Info(fmt.Sprintf("Comparing between pod %s and %s", pInfo1.Pod.Name, pInfo2.Pod.Name))

	ms1 := pInfo1.Pod.Annotations["topology-aware-scheduling.cs.phd.uqtr/microservice"]
	ms2 := pInfo2.Pod.Annotations["topology-aware-scheduling.cs.phd.uqtr/microservice"]

	if ms1 == "" || ms2 == "" {
		// logger.Error(errors.New("Pods are not part of any microservice"), "Pods are not part of any microservice")
		fmt.Printf("%s", "Pods are not part of any microservice")
		return true
	}
	// TODO: Check if ms1 and ms2 are part of the same application, otherwise, sort the pods using priority sort algorithm

	fmt.Println("Pods belong to the same MicroserviceApplication CR")

	// Get ranks
	ms1Rank := pInfo1.Pod.Annotations["topology-aware-scheduling.cs.phd.uqtr/rank"]
	ms2Rank := pInfo2.Pod.Annotations["topology-aware-scheduling.cs.phd.uqtr/rank"]
	if ms1Rank == "" || ms2Rank == "" {
		// logger.Error(errors.New("Pods dont have a rank"), "Pods dont have a rank")
		fmt.Printf("%s", "Pods dont have a rank")

		return true
	}

	ms1Ranki, err := strconv.Atoi(ms1Rank)
	if err != nil {
		// logger.Error(err, "Could convert ranks")
		fmt.Printf("%s", "Could convert ranks")

		return true
	}
	ms2Ranki, err := strconv.Atoi(ms2Rank)
	if err != nil {
		// logger.Error(err, "Could convert ranks")
		fmt.Printf("%s", "Could convert ranks")

		return true
	}

	fmt.Printf("Ranks: %d %d %v", ms1Ranki, ms2Ranki, ms1Ranki <= ms2Ranki)
	return ms1Ranki > ms2Ranki
}

func (ta *RankBasedSorting) EventsToRegister() []framework.ClusterEvent {
	return []framework.ClusterEvent{
		{Resource: framework.Node, ActionType: framework.Add},
	}
}

var ErrNotExpectedPreScoreState = errors.New("unexpected pre score state")

// ScoreExtensions of the Score plugin.
func (ta *RankBasedSorting) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

// New initializes a new plugin and returns it.
func New(ctx context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	logger := klog.FromContext(ctx).WithValues("plugin", Name)
	logger.V(4).Info("Creating new instance of the TopologicalSort plugin")

	// args, err := getArgs(obj)
	// if err != nil {
	// 	return nil, err
	// }

	c, _, err := util.NewClientWithCachedReader(ctx, handle.KubeConfig(), scheme)
	if err != nil {
		return nil, err
	}
	return &RankBasedSorting{
		Client: c,
		handle: handle,
		logger: logger,
	}, nil
}
