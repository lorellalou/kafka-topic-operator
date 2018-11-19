package kafkauser

import (
	"context"
	"log"
	"time"

	kafkav1alpha1 "github.com/lrolaz/kafka-topic-operator/pkg/apis/kafka/v1alpha1"
	kafka "github.com/lrolaz/kafka-topic-operator/pkg/kafka"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	
	sarama "github.com/Shopify/sarama"
	
	cmv1alpha1 "github.com/jetstack/cert-manager/pkg/apis/certmanager/v1alpha1"
	cmclientset "github.com/jetstack/cert-manager/pkg/client/clientset/versioned"	
)

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new KafkaUser Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	kafka, err := kafka.New()
	if err != nil {
		return err
	}
	log.Printf("Kafka Broker connected\n")
	cmClientSet, err := cmclientset.NewForConfig(mgr.GetConfig())
    if err != nil {
		return err
    }	
	return add(mgr, newReconciler(mgr, kafka, cmClientSet))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, kafka *kafka.KafkaUtil, cmClientSet *cmclientset.Clientset) reconcile.Reconciler {
	return &ReconcileKafkaUser{
		client: mgr.GetClient(), 
		scheme: mgr.GetScheme(),
		kafka: kafka,
		cmClient: cmClientSet,
	}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("kafkauser-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource KafkaUser
	err = c.Watch(&source.Kind{Type: &kafkav1alpha1.KafkaUser{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}


	return nil
}

var _ reconcile.Reconciler = &ReconcileKafkaUser{}

// ReconcileKafkaUser reconciles a KafkaUser object
type ReconcileKafkaUser struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
	kafka *kafka.KafkaUtil	
	cmClient *cmclientset.Clientset	
}

// Reconcile reads that state of the cluster for a KafkaUser object and makes changes based on the state read
// and what is in the KafkaUser.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileKafkaUser) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	log.Printf("Reconciling KafkaUser %s/%s\n", request.Namespace, request.Name)

	// Fetch the KafkaUser instance
	instance := &kafkav1alpha1.KafkaUser{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{Requeue: true, RequeueAfter: time.Duration(30)}, err
	}

	// Check if this Certificate already exists
	r.cmClient.CertmanagerV1alpha1().Certificates(request.Namespace).Get(request.Name)
	entries, err := r.kafka.KafkaAdmin.DescribeConfig(resource)
	if err != nil {
		return reconcile.Result{}, err
	}

	// Set KafkaUser instance as the owner and controller
	if err := controllerutil.SetControllerReference(instance, pod, r.scheme); err != nil {
		return reconcile.Result{}, err
	}


	// Pod already exists - don't requeue
	return reconcile.Result{}, nil
}

// newPodForCR returns a busybox pod with the same name/namespace as the cr
func newPodForCR(cr *kafkav1alpha1.KafkaUser) *corev1.Pod {
	labels := map[string]string{
		"app": cr.Name,
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-pod",
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    "busybox",
					Image:   "busybox",
					Command: []string{"sleep", "3600"},
				},
			},
		},
	}
}
