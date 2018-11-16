package kafka

import (
	"fmt"
	"log"
	"log"
	"strings"
	"os"
	"crypto/tls"
	"crypto/x509"	
	"io/ioutil"

	"github.com/Shopify/sarama"

)

type KafkaUtil struct {
	KafkaAdmin sarama.ClusterAdmin
}

func New() (*KafkaUtil, error) {
	config := sarama.NewConfig()
	config.ClientID = "kafkatopic-controller"
	config.Version = sarama.V2_0_0_0
	brokers := strings.Split(os.Getenv("KAFKA_BOOTSTRAP_SERVERS"), ",")
	log.Printf("Kafka Broker %s\n", strings.Join(brokers, ","))
	certFile := os.Getenv("OPERATOR_TLS_CERT_FILE")
	keyFile := os.Getenv("OPERATOR_TLS_KEY_FILE")
	caFile := os.Getenv("OPERATOR_TLS_CA_FILE")
	tlsConfig, err := createTlsConfiguration(certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	} else {
		log.Printf("No TLS config\n")
	}
	
	kafka, err := sarama.NewClusterAdmin(brokers, config)	
	if err != nil {
		return nil, err
	}	
	
	k := &KafkaUtil{
		KafkaAdmin: kafka,
	}
	
	return k, nil
}

func createTlsConfiguration(certFile string, keyFile string, caFile string) (t *tls.Config, err error) {
	if certFile != "" && keyFile != "" && caFile != "" {
		log.Printf("Loading TLS Key Pair %s %s\n", certFile, keyFile)
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}

		log.Printf("Loading TLS CA %s\n", caFile)
		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		intermediateCert, err := ioutil.ReadFile(certFile)
		if err != nil {
			return nil, err
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		caCertPool.AppendCertsFromPEM(intermediateCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			CipherSuites:       []uint16{tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384},
			InsecureSkipVerify:	true,
			ClientAuth:			tls.RequireAndVerifyClientCert,
			
		}
	}
	// will be nil by default if nothing is provided
	return t, nil
}

func (k *KafkaUtil) ListTopics() ([]string, error) {
	fmt.Println("Listing KafkaTopics")
	topics, err := k.KafkaClient.Topics()
	if err != nil {
		return nil, err
	}

	for _, t := range topics {
		fmt.Println("Current topic:", t)
	}
	return topics, nil
}

func (k *KafkaUtil) GetPartitions(topic string) ([]int32, error) {
	partitions, err := k.KafkaClient.Partitions(topic)
	if err != nil {
		return nil, err
	}
	return partitions, nil
}

func (k *KafkaUtil) PrintFullStats() error {
	topics, err := k.ListTopics()
	if err != nil {
		return err
	}
	for _, topic := range topics {
		partitions, err := k.GetPartitions(topic)
		if err != nil {
			return err
		}
		fmt.Println("Topic: %s, Partitions %s", topic, partitions)
	}

	return nil
}

func (k *KafkaUtil) GetTopicsOnBroker(cluster spec.Kafkacluster, brokerId int32) ([]string, error) {
	methodLogger := log.WithFields(log.Fields{
		"method":      "GetTopicsOnBroker",
		"clusterName": cluster.ObjectMeta.Name,
	})
	topicConfiguration, err := k.GetTopicConfiguration(cluster)
	if err != nil {
		return nil, err
	}
	topicOnBroker := make([]string, 0)

	for _, topic := range topicConfiguration {
	partitionLoop:
		for _, partition := range topic.Partitions {
			for _, replica := range partition.Replicas {
				if replica == brokerId {
					topicOnBroker = append(topicOnBroker, topic.Topic)
					break partitionLoop
				}
			}
		}
	}
	methodLogger.WithFields(log.Fields{
		"topics": topicOnBroker,
	}).Debug("Topics on Broker")
	return topicOnBroker, nil
}

func (k *KafkaUtil) GetTopicConfiguration(cluster spec.Kafkacluster) ([]spec.KafkaTopic, error) {
	methodLogger := log.WithFields(log.Fields{
		"method":      "GetTopicConfiguration",
		"clusterName": cluster.ObjectMeta.Name,
	})
	topics, err := k.KafkaClient.Topics()
	if err != nil {
		methodLogger.Error("Error Listing Topics")
		return nil, err
	}
	configuration := make([]spec.KafkaTopic, len(topics))
	for i, topic := range topics {

		partitions, err := k.KafkaClient.Partitions(topic)
		if err != nil {
			methodLogger.Error("Error Listing Partitions")
			return nil, err
		}
		t := spec.KafkaTopic{
			Topic:             topic,
			PartitionFactor:   int32(len(partitions)),
			ReplicationFactor: 3,
			Partitions:        make([]spec.KafkaPartition, len(partitions)),
		}
		for j, partition := range partitions {
			replicas, err := k.KafkaClient.Replicas(topic, partition)
			if err != nil {
				methodLogger.Error("Error listing partitions")
				return nil, err
			}
			t.Partitions[j] = spec.KafkaPartition{
				Partition: int32(j),
				Replicas:  replicas,
			}
		}
		configuration[i] = t
	}
	return configuration, nil
}

func (k *KafkaUtil) RemoveTopicFromBrokers(cluster spec.Kafkacluster, brokerToDelete int32, topic string) error {
	methodLogger := log.WithFields(log.Fields{
		"method":        "RemoveTopicFromBrokers",
		"clusterName":   cluster.ObjectMeta.Name,
		"brokerToDelte": brokerToDelete,
		"topic":         topic,
	})

	brokersToDelete := []int32{brokerToDelete}
	err := k.KazooClient.RemoveTopicFromBrokers(topic, brokersToDelete)
	if err != nil {
		methodLogger.Warn("Error removing topic from Broker", err)
		return err
	}
	return nil
}

func (k *KafkaUtil) RemoveTopicsFromBrokers(cluster spec.Kafkacluster, brokerToDelete int32) error {
	methodLogger := log.WithFields(log.Fields{
		"method":        "RemoveTopicsFromBrokers",
		"clusterName":   cluster.ObjectMeta.Name,
		"brokerToDelte": brokerToDelete,
	})
	topics, err := k.KafkaClient.Topics()
	if err != nil {
		methodLogger.Error("Error Listing Topics")
		return err
	}

	//TODO it should be possible to Delete multiple Brokers
	for _, topic := range topics {
		//TODO what do in cases where ReplicationFactor > remaining broker count
		k.RemoveTopicFromBrokers(cluster, brokerToDelete, topic)
	}

	return nil
}

func (k *KafkaUtil) AllTopicsInSync() (bool, error) {
	topics, err := k.KazooClient.Topics()
	if err != nil {
		return false, err
	}
	for _, topic := range topics {
		partitions, err := topic.Partitions()
		if err != nil {
			return false, err
		}
		for _, partition := range partitions {
			underReplicated, err := partition.UnderReplicated()
			if err != nil {
				return false, err
			}
			if underReplicated {
				return false, nil
			}
		}
	}
	return true, nil
}

func (k *KafkaUtil) CreateTopic(topicSpec spec.KafkaTopicSpec) error {
	fmt.Println("Creating Kafka Topics: ", topicSpec)
	broker, _ := k.KafkaClient.Coordinator("operatorConsumerGroup")
	request := sarama.MetadataRequest{Topics: []string{topicSpec.Name}}
	metadataPartial, err := broker.GetMetadata(&request)
	if err != nil {
		return err
	}

	replicas := []int32{0}
	isr := []int32{0}

	metadataResponse := &sarama.MetadataResponse{}
	metadataResponse.AddBroker(broker.Addr(), broker.ID())

	metadataPartial.AddTopic(topicSpec.Name, sarama.ErrNoError)
	//TODO dynamic partitions
	metadataPartial.AddTopicPartition(topicSpec.Name, 0, broker.ID(), replicas, isr, sarama.ErrNoError)
	metadataPartial.AddTopicPartition(topicSpec.Name, 1, broker.ID(), replicas, isr, sarama.ErrNoError)

	return nil
}