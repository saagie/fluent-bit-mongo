package main

import (
	"C"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/fluent/fluent-bit-go/output"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"strings"
	"unsafe"
	"github.com/spaolacci/murmur3"
)

type configType struct {
	connectionString string
	database  string
	collectionFormat string
}

type logDocument struct {
	Id bson.ObjectId `bson:"_id,omitempty"`
	Log string `bson:"log"`
	Stream string `bson:"stream"`
	Time string `bson:"time"`
	JobExecutionId string `bson:"job_execution_id"`
	ProjectId string `bson:"project_id"`
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
	config.connectionString = output.FLBPluginConfigKey(ctx, "connection_string")
	config.database = output.FLBPluginConfigKey(ctx, "database")
	config.collectionFormat = output.FLBPluginConfigKey(ctx, "collection_format")
	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	var ret int
	var record map[interface{}]interface{}

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))
	session, err := mgo.Dial(config.connectionString)
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
		if (err != nil) {
			fmt.Printf(err.Error())
			return output.FLB_ERROR
		}

		projectName := string(record["project_id"].([]uint8))
		collectionName := strings.Replace(fmt.Sprintf(config.collectionFormat, projectName), "-", "_", -1)
		collection := session.DB(config.database).C(collectionName)

		_, err = collection.UpsertId(logDoc.Id, logDoc)
		if err != nil {
			fmt.Println(err.Error())
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

func recordToDocument(record map[interface{}]interface{}) (logDocument, error) {
	logDoc := logDocument{
		Log: string(record["log"].([]uint8)),
		Stream: string(record["stream"].([]uint8)),
		Time: string(record["time"].([]uint8)),
		JobExecutionId: string(record["job_execution_id"].([]uint8)),
		ProjectId: string(record["project_id"].([]uint8)),
	}
	id, err := getObjectID(logDoc)
	if (err != nil) {
		return logDocument{}, err
	}
	logDoc.Id = id
	return logDoc, nil
}

func getObjectID(document logDocument) (bson.ObjectId, error) {
	logJson, err := json.Marshal(document)
	if err != nil {
		return bson.NewObjectId(), err
	}

	h64bytes, h32bytes, err := getBytesFromDocument(logJson)
	if err != nil {
		return bson.NewObjectId(), err
	}

	id := fmt.Sprintf("%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
		h64bytes[0], h64bytes[1], h64bytes[2], h64bytes[3], h64bytes[4], h64bytes[5], h64bytes[6], h64bytes[7],
		h32bytes[0], h32bytes[1], h32bytes[2], h32bytes[3],
	)

	return bson.ObjectIdHex(id), nil
}

func getBytesFromDocument(data []byte) ([]byte, []byte, error) {
	huint64 := murmur3.Sum64WithSeed(data, 42)
	huint32 := murmur3.Sum32WithSeed(data, 42)
	h64bytes, err := uint64ToBytes(huint64)
	if err != nil {
		return nil, nil, err
	}
	h32bytes, err := uint32ToBytes(huint32)
	if err != nil {
		return nil, nil, err
	}
	return h64bytes, h32bytes, nil
}

func uint64ToBytes(i uint64) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, i)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func uint32ToBytes(i uint32) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, i)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {
}
