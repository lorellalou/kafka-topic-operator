package kafkauser

import (
	"context"
	"log"
	"fmt"
	"time"
	"bytes"
	"bufio"
	"encoding/pem"
	"math/rand"
	"crypto/x509"	

	kafkav1alpha1 "github.com/lrolaz/kafka-topic-operator/pkg/apis/kafka/v1alpha1"
	kafka "github.com/lrolaz/kafka-topic-operator/pkg/kafka"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"

		
	//sarama "github.com/Shopify/sarama"
	
	cmv1alpha1 "github.com/jetstack/cert-manager/pkg/apis/certmanager/v1alpha1"
	
	"github.com/pavel-v-chernykh/keystore-go"
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
	//cmClientSet, err := cmclientset.NewForConfig(mgr.GetConfig())
    //if err != nil {
	//	return err
    //}	
	return add(mgr, newReconciler(mgr, kafka))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, kafka *kafka.KafkaUtil) reconcile.Reconciler {
	return &ReconcileKafkaUser{
		client: mgr.GetClient(), 
		scheme: mgr.GetScheme(),
		kafka: kafka,
		//cmClient: cmClientSet,
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
	//cmClient *cmclientset.Clientset	
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
		log.Printf("%s\n", err)
		return reconcile.Result{Requeue: true, RequeueAfter: time.Duration(30)}, err
	}

	// build certificate
	certificate := &cmv1alpha1.Certificate{
		ObjectMeta: metav1.ObjectMeta{	
			Namespace: instance.Namespace,
			Name:      instance.Spec.Authentication.TLS.SecretName,
		},
		Spec: cmv1alpha1.CertificateSpec{
			CommonName: instance.Name,
			SecretName: instance.Spec.Authentication.TLS.SecretName,
			KeyAlgorithm: cmv1alpha1.RSAKeyAlgorithm,
			KeySize: 2048,			
			IssuerRef: cmv1alpha1.ObjectReference{
				Name: instance.Spec.Authentication.TLS.IssuerName,
				Kind: instance.Spec.Authentication.TLS.IssuerKind,
			},
		},
	}	

	controllerutil.SetControllerReference(instance, certificate, r.scheme)

	// Check if this Certificate already exists
	existingCert := &cmv1alpha1.Certificate{}
	err = r.client.Get(context.TODO(), 
		types.NamespacedName{
			Name: instance.Spec.Authentication.TLS.SecretName, 
			Namespace: instance.Namespace,
		}, existingCert)
	if err == nil {
		r.client.Delete(context.TODO(), existingCert)
		existingSecret := &corev1.Secret{}
		err = r.client.Get(context.TODO(), 
			types.NamespacedName{
				Name: instance.Spec.Authentication.TLS.SecretName, 
				Namespace: instance.Namespace,
			}, existingSecret)
		if err != nil {
			r.client.Delete(context.TODO(), existingSecret)
		}
	}
	if err == nil || k8sErrors.IsNotFound(err) {
		log.Printf("Creating a new User Certificate %s/%s\n", request.Namespace, instance.Spec.Authentication.TLS.SecretName)	
		err = r.client.Create(context.TODO(), certificate)
		if err != nil {
			log.Printf("%s\n", err)
			return reconcile.Result{Requeue: true, RequeueAfter: time.Duration(30)}, err
		}		
	} else {
		log.Printf("%s\n", err)
		return reconcile.Result{Requeue: true, RequeueAfter: time.Duration(30)}, err
	}
	
	log.Printf("Successfully created Certificate %s", instance.Spec.Authentication.TLS.SecretName)

	log.Println("waiting for secret...")
	secret := &corev1.Secret{}
	for {
		err = r.client.Get(context.TODO(), 
			types.NamespacedName{
				Name: instance.Spec.Authentication.TLS.SecretName, 
				Namespace: instance.Namespace,
			}, secret)
		if err != nil {
			log.Printf("unable to retrieve certificate secret (%s): %s", instance.Spec.Authentication.TLS.SecretName, err)
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}
	log.Printf("Successfully rerieved certificate secret %s", instance.Spec.Authentication.TLS.SecretName)
	
	keyStore, trustStore, err := r.createJavaKeystore(certificate)
	if err != nil {
		log.Printf("unable to create the java keystore (%s): %s", certificate.Name, err)
		return reconcile.Result{Requeue: true, RequeueAfter: time.Duration(30)}, err
	}
	log.Printf("Successfully created the java keystore (%s)", certificate.Name)
	// generate random password
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	password := make([]byte, 10)
    for i := range password {
        password[i] = letterBytes[rand.Int63() % int64(len(letterBytes))]
    }

	// Write Keystore
	var buffer bytes.Buffer
	var writer *bufio.Writer
    writer = bufio.NewWriter(&buffer)
	err = keystore.Encode(writer, *keyStore, password)
	if err != nil {
		log.Printf("unable to create or update the java keystore (%s): %s", "keystore.jks", err)
		return reconcile.Result{Requeue: true, RequeueAfter: time.Duration(30)}, err
	}
	writer.Flush()
	secret.Data["keystore.jks"] = buffer.Bytes()
	secret.Data["keystore.password"] = password
	buffer.Reset()	
	
	// Write Truststore
    writer = bufio.NewWriter(&buffer)
	err = keystore.Encode(writer, *trustStore, password)
	if err != nil {
		log.Printf("unable to create or update the java keystore (%s): %s", "truststore.jks", err)
		return reconcile.Result{Requeue: true, RequeueAfter: time.Duration(30)}, err
	}
	writer.Flush()
	secret.Data["truststore.jks"] = buffer.Bytes()
	secret.Data["truststore.password"] = password
	buffer.Reset()	
	
	// save Secret
	err = r.client.Update(context.TODO(), secret)
	if err != nil {
		log.Printf("Unable to update the certificate secret: %s", err)
		return reconcile.Result{Requeue: true, RequeueAfter: time.Duration(30)}, err
	}	
	//  don't requeue
	return reconcile.Result{}, nil
}

func (r *ReconcileKafkaUser) createJavaKeystore(crt *cmv1alpha1.Certificate) (*keystore.KeyStore, *keystore.KeyStore, error) {
	
	secret := &corev1.Secret{}
	err := r.client.Get(context.TODO(), 
		types.NamespacedName{
			Name: crt.Spec.SecretName, 
			Namespace: crt.Namespace,
		}, secret)
	if err != nil {
		return nil, nil, err
	}		
	
	trustedCertificates := make([]keystore.TrustedCertificateEntry,0)
	for _, trustedCert := range r.kafka.CACertificates {
		trustedCertificates = append(trustedCertificates, 
			keystore.TrustedCertificateEntry{
				Entry: keystore.Entry{
					CreationDate: time.Now(),
				},
				Certificate: keystore.Certificate{
					Type: "X509",
					Content: trustedCert.Raw,	
				},
	        },
		)
	}	
		
		
	pcks1KeyBlock, _ := pem.Decode(secret.Data[corev1.TLSPrivateKeyKey])
	pkcs1Key, err := x509.ParsePKCS1PrivateKey(pcks1KeyBlock.Bytes)
	if err != nil {
		log.Printf("error parsing rsa private key: %s", err)
		return nil, nil, err
	}
	pkcs8Key, err := x509.MarshalPKCS8PrivateKey(pkcs1Key)
	if err != nil {
		log.Printf("error converting private key to PKCS8: %s", err)
		return nil, nil, err
	}		
		
	var certificates []keystore.Certificate
	var buf []byte = secret.Data[corev1.TLSCertKey]
	var block *pem.Block
	// loop over pem encoded data
	for len(buf) > 0 {
		block, buf = pem.Decode(buf)
		if block == nil {
			log.Printf("invalid PEM data")
		}
		certificates = append(certificates, 
			keystore.Certificate{
				Type: "X509",
				Content: block.Bytes,	
			})
	}		
		
	keyStore := keystore.KeyStore{
		crt.Spec.SecretName: &keystore.PrivateKeyEntry{
			Entry: keystore.Entry{
				CreationDate: time.Now(),
			},
			PrivKey: pkcs8Key,
			CertChain: certificates,
		},
	}

	for i, trustedCertif := range trustedCertificates {
		keyStore[fmt.Sprintf("trusted-%d", i)] = &trustedCertif
	}

	trustStore := keystore.KeyStore{}

	for i, trustedCertif := range trustedCertificates {
		trustStore[fmt.Sprintf("trusted-%d", i)] = &trustedCertif
	}

	for i, cert := range certificates {
        if i == 0 {
            continue
        }
        trustStore[fmt.Sprintf("intermediate-%d", i)] = &keystore.TrustedCertificateEntry{
			Entry: keystore.Entry{
				CreationDate: time.Now(),
			},
			Certificate: cert,
        }
    }

	return &keyStore, &trustStore, nil
}

