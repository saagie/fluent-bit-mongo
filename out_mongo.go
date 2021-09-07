package main

import (
	"C"
	"context"
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/saagie/fluent-bit-mongo/pkg/config"
	flbcontext "github.com/saagie/fluent-bit-mongo/pkg/context"
	"github.com/saagie/fluent-bit-mongo/pkg/entry"
	"github.com/saagie/fluent-bit-mongo/pkg/entry/mongo"
	"github.com/saagie/fluent-bit-mongo/pkg/log"
	"golang.org/x/sync/errgroup"
	mgo "gopkg.in/mgo.v2"
)

const PluginID = "mongo"

//export FLBPluginRegister
func FLBPluginRegister(ctxPointer unsafe.Pointer) int {
	logger, err := log.New(log.OutputPlugin, PluginID)
	if err != nil {
		fmt.Printf("error initializing logger: %s\n", err)

		return output.FLB_ERROR
	}

	logger.Info("Registering plugin", nil)

	result := output.FLBPluginRegister(ctxPointer, PluginID, "Go mongo go")

	switch result {
	case output.FLB_OK:
		flbcontext.Set(ctxPointer, &flbcontext.Value{
			Logger: logger,
		})
	default:
		// nothing to do
	}

	return result
}

//export FLBPluginInit
// (fluentbit will call this)
// ctx (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(ctxPointer unsafe.Pointer) int {
	value, err := flbcontext.Get(ctxPointer)
	if err != nil {
		logger, err := log.New(log.OutputPlugin, PluginID)
		if err != nil {
			fmt.Printf("error initializing logger: %s\n", err)

			return output.FLB_ERROR
		}

		logger.Info("New logger initialized", nil)

		value.Logger = logger
	}

	value.Logger.Info("Initializing plugin", nil)

	value.Config = config.GetConfig(ctxPointer)

	flbcontext.Set(ctxPointer, value)

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	panic(errors.New("not supported call"))
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctxPointer, data unsafe.Pointer, length C.int, tag *C.char) (result int) {
	value, err := flbcontext.Get(ctxPointer)
	if err != nil {
		fmt.Printf("error getting value: %s\n", err)

		return output.FLB_ERROR
	}

	logger := value.Logger
	ctx := log.WithLogger(context.TODO(), logger)

	// Open mongo session
	config := value.Config.(*mgo.DialInfo)

	logger.Info("Connecting to mongodb", map[string]interface{}{
		"hosts":         config.Addrs,
		"user":          config.Username,
		"source":        config.Source,
		"database":      config.Database,
		"with_password": config.Password != "",
	})

	session, err := mgo.DialWithInfo(config)
	if err != nil {
		logger.Error("Failed to connect to mongodb", map[string]interface{}{
			"error": err,
		})

		return output.FLB_RETRY
	}

	defer session.Close()

	dec := output.NewDecoder(data, int(length)) // Create Fluent Bit decoder
	processor := mongo.New(session)

	if err := ProcessAll(ctx, dec, processor); err != nil {
		logger.Error("Failed to process logs", map[string]interface{}{
			"error": err,
		})

		if errors.Is(err, &entry.ErrRetry{}) {
			return output.FLB_RETRY
		}

		return output.FLB_ERROR
	}

	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later.
	return output.FLB_OK
}

func ProcessAll(ctx context.Context, dec *output.FLBDecoder, processor entry.Processor) error {
	g, ctx := errgroup.WithContext(ctx)

	// For log purpose
	startTime := time.Now()
	total := 0
	logger, err := log.GetLogger(ctx)
	if err != nil {
		return fmt.Errorf("get logger: %w", err)
	}

	// Iterate Records
	for {
		// Extract Record
		ts, record, err := entry.GetRecord(dec)
		if err != nil {
			if errors.Is(err, entry.ErrNoRecord) {
				logger.Debug("Records flushed", map[string]interface{}{
					"count":    total,
					"duration": time.Since(startTime),
				})

				break
			}

			return fmt.Errorf("get record: %w", err)
		}

		total++

		//g.Go(func() error {

		if err := processor.ProcessRecord(ctx, ts, record); err != nil {
			return fmt.Errorf("process record: %w", err)
		}

		//return nil
		//})
	}

	return g.Wait()
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}
