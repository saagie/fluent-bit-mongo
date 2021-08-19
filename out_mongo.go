package main

import (
	"C"
	"fmt"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/saagie/fluent-bit-mongo/pkg/config"
	"github.com/saagie/fluent-bit-mongo/pkg/document"
	"github.com/saagie/fluent-bit-mongo/pkg/log"
	mgo "gopkg.in/mgo.v2"
)

const PluginID = "mongo"

var logger log.Logger

func init() {
	l, err := log.New(log.OutputPlugin, PluginID)
	if err != nil {
		panic(fmt.Errorf("new logger: %w", err))
	}

	logger = l

	logger.Debug("Logger initialized", nil)
}

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	logger.Debug("Registering plugin", nil)

	return output.FLBPluginRegister(ctx, PluginID, "Go mongo go")
}

//export FLBPluginInit
// (fluentbit will call this)
// ctx (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(ctx unsafe.Pointer) int {
	logger.Info("Initializing plugin", nil)

	output.FLBPluginSetContext(ctx, config.GetConfig(ctx))

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	return output.FLB_ERROR
}

const MongoDefaultDB = ""

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	var ret int
	var record map[interface{}]interface{}

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))
	config := output.FLBPluginGetContext(ctx).(*mgo.DialInfo)

	session, err := mgo.DialWithInfo(config)
	if err != nil {
		logger.Error("Failed to connect to mongo", map[string]interface{}{
			"hosts":         config.Addrs,
			"user":          config.Username,
			"source":        config.Source,
			"database":      config.Database,
			"with_password": config.Password != "",
			"error":         err,
		})

		return output.FLB_RETRY
	}

	defer session.Close()

	// Iterate Records
	for {
		// Extract Record
		ret, _, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}

		logDoc, err := document.RecordToDocument(record)
		if err != nil {
			logger.Error("Failed to convert record to document", map[string]interface{}{
				"error": err,
			})

			return output.FLB_ERROR
		}

		collection := session.DB(MongoDefaultDB).C(logDoc.CollectionName())

		logger.Debug("Flushing to mongo", map[string]interface{}{
			"document.id": logDoc.Id,
		})

		if err := logDoc.SaveTo(collection); err != nil {
			logger.Error("Failed to save document", map[string]interface{}{
				"document":   logDoc,
				"collection": collection.FullName,
				"error":      err,
			})

			return output.FLB_RETRY
		}
	}

	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later.
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}
