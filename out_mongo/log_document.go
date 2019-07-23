package main

import (
	"encoding/json"
	"fmt"
	"gopkg.in/mgo.v2/bson"
)

type logDocument struct {
	Id             bson.ObjectId `bson:"_id,omitempty"`
	Log            string        `bson:"log"`
	Stream         string        `bson:"stream"`
	Time           string        `bson:"time"`
	JobExecutionId string        `bson:"job_execution_id"`
	TryId          string        `bson:"try_id"`
	ProjectId      string        `bson:"project_id"`
	Customer       string        `bson:"customer"`
	PlatformId     string        `bson:"platform_id"`
}

func recordToDocument(record map[interface{}]interface{}) (logDocument, error) {
	logDoc := logDocument{
		Log:            extractStringValue(record, "log"),
		Stream:         extractStringValue(record, "stream"),
		Time:           extractStringValue(record, "time"),
		JobExecutionId: extractStringValue(record, "job_execution_id"),
		TryId:          extractStringValue(record, "try_id"),
		ProjectId:      extractStringValue(record, "project_id"),
		Customer:       extractStringValue(record, "customer"),
		PlatformId:     extractStringValue(record, "platform_id"),
	}
	err := logDoc.generateObjectID()
	if err != nil {
		return logDocument{}, err
	}
	return logDoc, nil
}

func (d *logDocument) generateObjectID() error {
	logJson, err := json.Marshal(d)
	if err != nil {
		return err
	}

	h64bytes, h32bytes, err := getHashesFromBytes(logJson)
	if err != nil {
		return err
	}

	id := fmt.Sprintf("%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
		h64bytes[0], h64bytes[1], h64bytes[2], h64bytes[3], h64bytes[4], h64bytes[5], h64bytes[6], h64bytes[7],
		h32bytes[0], h32bytes[1], h32bytes[2], h32bytes[3],
	)
	d.Id = bson.ObjectIdHex(id)
	return nil
}
