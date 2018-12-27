package main

import (
	"C"
	"fmt"
	"github.com/fluent/fluent-bit-go/output"
	"gopkg.in/mgo.v2"
	"os"
	"strings"
	"unsafe"
)

type configType struct {
	host             []string
	authDatabase     string
	username         string
	password         string
	database         string
	collectionFormat string
}

var config = configType{}

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "mongo", "Go mongo go")
}

//export FLBPluginInit
// (fluentbit will call this)
// ctx (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(ctx unsafe.Pointer) int {
	// Example to retrieve an optional configuration parameter
	config.database = output.FLBPluginConfigKey(ctx, "database")

	config.host = []string{output.FLBPluginConfigKey(ctx, "host_port")}
	config.authDatabase = output.FLBPluginConfigKey(ctx, "auth_database")
	config.username = output.FLBPluginConfigKey(ctx, "username")
	config.password = os.Getenv("MONGOPASSWORD")
	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	var ret int
	var record map[interface{}]interface{}

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))
	session, err := mgo.DialWithInfo(&mgo.DialInfo{
		Addrs:    config.host,
		Username: config.username,
		Password: config.password,
		Source:   config.authDatabase,
	})
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

		logDoc, err := recordToDocument(record)
		if err != nil {
			fmt.Printf("FLB_ERROR: %s\n", err.Error())
			return output.FLB_ERROR
		}

		collectionName := strings.Replace(fmt.Sprintf("%s_%s_%s", logDoc.Customer, logDoc.PlatformId, logDoc.ProjectId), "-", "_", -1)
		collection := session.DB(config.database).C(collectionName)

		_, err = collection.UpsertId(logDoc.Id, logDoc)
		if err != nil {
			fmt.Printf("FLB_RETRY: %s\n", err.Error())
			return output.FLB_RETRY
		}

		err = collection.EnsureIndexKey("job_execution_id")
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
