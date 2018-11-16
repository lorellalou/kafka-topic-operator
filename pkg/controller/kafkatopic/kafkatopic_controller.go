package kafkatopic

import (
	"context"
	"log"
	"strings"
	"os"
	"crypto/tls"
	"crypto/x509"	
	"io/ioutil"
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
)

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new KafkaTopic Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	kafka, err := kafka.New()
	if err != nil {
		return err
	}
	log.Printf("Kafka Broker connected !\n")
	return add(mgr, newReconciler(mgr, kafka))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, kafka kafka.KafkaUtil) reconcile.Reconciler {

	return &ReconcileKafkaTopic{
		client: mgr.GetClient(), 
		scheme: mgr.GetScheme(),
		kafka: kafka,
	}
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
	kafka kafka.KafkaUtil
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
			log.Printf("KafkaTopic %s/%s deleted but the topic is kept on Brokers\n", request.Namespace, request.Name)	
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// Check if this Topic already exists
	resource := sarama.ConfigResource{Name: instance.Spec.TopicName, Type: sarama.TopicResource, ConfigNames: []string{}}
	entries, err := r.kafka.DescribeConfig(resource)
	if err != nil {
		return reconcile.Result{}, err
	}

	config := make(map[string]*string)
	// loop over config
	for key, value := range instance.Spec.Config {
		v := value
		config[key] = &v
	}	

	if len(entries) <= 0 {
		log.Printf("Creating a new Topic %s/%s\n", request.Namespace, instance.Spec.TopicName)	
		err = r.kafka.CreateTopic(instance.Spec.TopicName, 
			&sarama.TopicDetail{
				NumPartitions: instance.Spec.Partitions, 
				ReplicationFactor: instance.Spec.Replicas,
				ConfigEntries: config,
			}, false)
		if err != nil {
			log.Printf("%s\n", err)
			return reconcile.Result{Requeue: true, RequeueAfter: time.Duration(30)}, err
		}

		log.Printf("Topic %s/%s created successfully", request.Namespace, instance.Spec.TopicName)
		return reconcile.Result{}, nil
	} else {
		log.Printf("Updating Topic %s/%s\n", request.Namespace, instance.Spec.TopicName)
		err = r.kafka.AlterConfig(sarama.TopicResource, instance.Spec.TopicName, config, false)
		if err != nil {
			log.Printf("%s\n", err)
			return reconcile.Result{Requeue: true, RequeueAfter: time.Duration(30)}, err
		}
		log.Printf("Topic %s/%s updated successfully", request.Namespace, instance.Spec.TopicName)
		return reconcile.Result{}, nil
	}
}

