package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// KafkaUserSpec defines the desired state of KafkaUser
type KafkaUserSpec struct {
	Authentication   KafkaUserAuthentication   `json:"authentication"`
//	Authorization    KafkaUserAuthorization    `json:"authorization"`
}

type KafkaUserAuthentication struct {
	TLS     		*KafkaUserAuthenticationTLS		`json:"tls"`
}

type KafkaUserAuthenticationTLS struct {
	SecretName	   string			  `json:"secretName"`
	IssuerName     string             `json:"issuerName"`
	IssuerKind     string             `json:"issuerKind"`
}

type KafkaUserAuthorization struct {


}

// KafkaUserStatus defines the observed state of KafkaUser
type KafkaUserStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "operator-sdk generate k8s" to regenerate code after modifying this file
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// KafkaUser is the Schema for the kafkausers API
// +k8s:openapi-gen=true
type KafkaUser struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaUserSpec   `json:"spec,omitempty"`
	Status KafkaUserStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// KafkaUserList contains a list of KafkaUser
type KafkaUserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaUser `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KafkaUser{}, &KafkaUserList{})
}
