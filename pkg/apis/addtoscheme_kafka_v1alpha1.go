package apis

import (
	"github.com/lrolaz/kafka-topic-operator/pkg/apis/kafka/v1alpha1"
	cmv1alpha1 "github.com/jetstack/cert-manager/pkg/apis/certmanager/v1alpha1"	
)

func init() {
	// Register the types with the Scheme so the components can map objects to GroupVersionKinds and back
	AddToSchemes = append(AddToSchemes, v1alpha1.SchemeBuilder.AddToScheme)
	AddToSchemes = append(AddToSchemes, cmv1alpha1.SchemeBuilder.AddToScheme)
}
