package controller

import (
	"github.com/lrolaz/kafka-topic-operator/pkg/controller/kafkauser"
)

func init() {
	// AddToManagerFuncs is a list of functions to create controllers and add them to a manager.
	AddToManagerFuncs = append(AddToManagerFuncs, kafkauser.Add)
}
