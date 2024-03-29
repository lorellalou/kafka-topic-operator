package kafka

import (
	"log"
	"strings"
	"os"
	"crypto/tls"
	"crypto/x509"	
	"io/ioutil"
	"encoding/pem"	

	"github.com/Shopify/sarama"

)

type KafkaUtil struct {
	KafkaAdmin sarama.ClusterAdmin
	CACertificates []*x509.Certificate
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
	tlsConfig, cacert, err := createTlsConfiguration(certFile, keyFile, caFile)
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
		CACertificates: cacert,
	}
	
	return k, nil
}

func createTlsConfiguration(certFile string, keyFile string, caFile string) (t *tls.Config, cacert []*x509.Certificate, err error) {
	if certFile != "" && keyFile != "" && caFile != "" {
		log.Printf("Loading TLS Key Pair %s %s\n", certFile, keyFile)
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, nil, err
		}

		log.Printf("Loading TLS CA %s\n", caFile)

		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, nil, err
		}
		for len(caCert) > 0 {
			var block *pem.Block
			block, caCert = pem.Decode(caCert)
			if block == nil {
				break
			}
			if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
				continue
			}
	
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				continue
			}
	
			cacert = append(cacert, cert)
			log.Printf("Add CA Cert %s (%s)\n", cert.Subject.CommonName, cert.SerialNumber)
		}
		intermediateCert, err := ioutil.ReadFile(certFile)
		if err != nil {
			return nil, nil, err
		}
		for len(intermediateCert) > 0 {
			var block *pem.Block
			block, intermediateCert = pem.Decode(intermediateCert)
			if block == nil {
				break
			}
			if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
				continue
			}
	
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				continue
			}
	
			cacert = append(cacert, cert)
			log.Printf("Add CA Cert %s (%s)\n", cert.Subject.CommonName, cert.SerialNumber)
		}

		caCert, err = ioutil.ReadFile(caFile)
		if err != nil {
			return nil, nil, err
		}
		intermediateCert, err = ioutil.ReadFile(certFile)
		if err != nil {
			return nil, nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		caCertPool.AppendCertsFromPEM(intermediateCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			CipherSuites:       []uint16{tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384},
//			InsecureSkipVerify:	true,
			ClientAuth:			tls.RequireAndVerifyClientCert,
			
		}
	}
	// will be nil by default if nothing is provided
	return t, cacert, nil
}
