package entry

import (
	"context"
	"errors"
	"fmt"

	"github.com/fluent/fluent-bit-go/output"
)

type Processor interface {
	ProcessRecord(context.Context, map[interface{}]interface{}) error
}

var ErrNoRecord = errors.New("failed to decode entry")

type ErrRetry struct {
	Cause error
}

func (err *ErrRetry) Error() string {
	return fmt.Sprintf("retry: %s", err.Cause)
}

func (err *ErrRetry) Unwrap() error {
	return err.Cause
}

func (err *ErrRetry) Is(err2 error) bool {
	_, ok := err2.(*ErrRetry)

	return ok
}

func GetRecord(dec *output.FLBDecoder) (map[interface{}]interface{}, error) {
	ret, _, record := output.GetRecord(dec)
	switch ret {
	default:
		return record, nil
	case -1:
		return nil, ErrNoRecord
	case -2:
		return nil, errors.New("unepxected entry type")
	}
}
