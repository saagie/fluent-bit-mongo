package main

import (
	"C"
	"fmt"
	"strings"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/saagie/fluent-bit-mongo/pkg/config"
	"github.com/saagie/fluent-bit-mongo/pkg/document"
	"gopkg.in/mgo.v2"
)

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "mongo", "Go mongo go")
}

//export FLBPluginInit
// (fluentbit will call this)
// ctx (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(ctx unsafe.Pointer) int {
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
		panic(err)
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
			fmt.Printf("FLB_ERROR: %s\n", err.Error())
			return output.FLB_ERROR
		}

		collectionName := strings.Replace(fmt.Sprintf("%s_%s_%s", logDoc.Customer, logDoc.PlatformId, logDoc.ProjectId), "-", "_", -1)
		collection := session.DB(MongoDefaultDB).C(collectionName)

		_, err = collection.UpsertId(logDoc.Id, logDoc)
		if err != nil {
			fmt.Printf("FLB_RETRY: %s\n", err.Error())
			return output.FLB_RETRY
		}

		err = collection.EnsureIndexKey("job_execution_id", "time")
		if err != nil {
			fmt.Printf("FLB_RETRY: %s\n", err.Error())
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

func main() {
}
