package entry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fluent/fluent-bit-go/output"
)

type Processor interface {
	ProcessRecord(context.Context, time.Time, map[interface{}]interface{}) error
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

func GetRecord(dec *output.FLBDecoder) (time.Time, map[interface{}]interface{}, error) {
	ret, ts, record := output.GetRecord(dec)

	switch ret {
	default:
		return ts.(output.FLBTime).Time, record, nil
	case -1:
		return time.Time{}, nil, ErrNoRecord
	case -2:
		return time.Time{}, nil, errors.New("unexpected entry type")
	}
}
