package context

import (
	"errors"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/saagie/fluent-bit-mongo/pkg/log"
)

type Value struct {
	Logger log.Logger
	Config interface{}
}

func Get(ctxPointer unsafe.Pointer) (*Value, error) {
	value := output.FLBPluginGetContext(ctxPointer)
	if value == nil {
		return &Value{}, errors.New("no value found")
	}

	return value.(*Value), nil
}

func Set(ctxPointer unsafe.Pointer, value *Value) {
	output.FLBPluginSetContext(ctxPointer, value)
}
