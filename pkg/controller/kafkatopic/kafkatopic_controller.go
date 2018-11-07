package kafkatopic

import (
	"context"
	"log"

	kafkav1alpha1 "github.com/lrolaz/kafka-topic-operator/pkg/apis/kafka/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	
	sarama "github.com/Shopify/sarama"
)

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new KafkaTopic Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileKafkaTopic{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("kafkatopic-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource KafkaTopic
	err = c.Watch(&source.Kind{Type: &kafkav1alpha1.KafkaTopic{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcileKafkaTopic{}

// ReconcileKafkaTopic reconciles a KafkaTopic object
type ReconcileKafkaTopic struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
	kafka sarama.ClusterAdmin
}

// Reconcile reads that state of the cluster for a KafkaTopic object and makes changes based on the state read
// and what is in the KafkaTopic.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileKafkaTopic) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	log.Printf("Reconciling KafkaTopic %s/%s\n", request.Namespace, request.Name)

	// Fetch the KafkaTopic instance
	instance := &kafkav1alpha1.KafkaTopic{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// Check if this Topic already exists
	resource := sarama.ConfigResource{Name: "r1", Type: TopicResource, ConfigNames: []string{instance.Spec.TopicName}}
	entries, err := r.kafka.DescribeConfig(resource)
	if err != nil {
		return reconcile.Result{}, err
	}

	if len(entries) <= 0 {
		log.Printf("Creating a new Topic %s/%s\n", request.Namespace, request.Name)
		err = r.kafka.CreateTopic(instance.Spec.TopicName, 
			&sarama.TopicDetail{
				NumPartitions: instance.Spec.Partitions, 
				ReplicationFactor: instance.Spec.Replicas,
				ConfigEntries: instance.Spec.Config,
			}, false)
		if err != nil {
			return reconcile.Result{}, err
		}

		// Topic created successfully - don't requeue
		return reconcile.Result{}, nil
	} else {
		log.Printf("Updating new Topic %s/%s\n", request.Namespace, request.Name)
		// Todo	
		return reconcile.Result{}, nil
	}

}
