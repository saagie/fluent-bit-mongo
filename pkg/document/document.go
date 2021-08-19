package document

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/saagie/fluent-bit-mongo/pkg/parse"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Document struct {
	Id             bson.ObjectId `bson:"_id,omitempty"`
	Log            string        `bson:"log"`
	Stream         string        `bson:"stream"`
	Time           string        `bson:"time"`
	JobExecutionId string        `bson:"job_execution_id"`
	ProjectId      string        `bson:"project_id"`
	Customer       string        `bson:"customer"`
	PlatformId     string        `bson:"platform_id"`
}

func New(record map[interface{}]interface{}) (*Document, error) {
	doc := &Document{}

	if err := doc.Populate(record); err != nil {
		if errors.Is(err, parse.ErrKeyNotFound) {
			keys := make([]interface{}, 0, len(record))
			for key := range record {
				keys = append(keys, key)
			}

			return nil, fmt.Errorf("keys %v: %w", keys, err)
		}

		return nil, err
	}

	return doc, nil
}

func (d *Document) Populate(record map[interface{}]interface{}) (err error) {
	d.Log, err = parse.ExtractStringValue(record, "log")
	if err != nil {
		return fmt.Errorf("parse %s: %w", "log", err)
	}

	d.Stream, err = parse.ExtractStringValue(record, "stream")
	if err != nil {
		return fmt.Errorf("parse %s: %w", "stream", err)
	}

	d.Time, err = parse.ExtractStringValue(record, "time")
	if err != nil {
		return fmt.Errorf("parse %s: %w", "time", err)
	}

	d.JobExecutionId, err = parse.ExtractStringValue(record, "job_execution_id")
	if err != nil {
		return fmt.Errorf("parse %s: %w", "job_execution_id", err)
	}

	d.ProjectId, err = parse.ExtractStringValue(record, "project_id")
	if err != nil {
		return fmt.Errorf("parse %s: %w", "project_id", err)
	}

	d.Customer, err = parse.ExtractStringValue(record, "customer")
	if err != nil {
		return fmt.Errorf("parse %s: %w", "customer", err)
	}

	d.PlatformId, err = parse.ExtractStringValue(record, "platform_id")
	if err != nil {
		return fmt.Errorf("parse %s: %w", "platform_id", err)
	}

	return d.generateObjectID()
}

func (d *Document) generateObjectID() error {
	logJson, err := json.Marshal(d)
	if err != nil {
		return err
	}

	h64bytes, h32bytes, err := parse.GetHashesFromBytes(logJson)
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

func (d *Document) CollectionName() string {
	return strings.Replace(fmt.Sprintf("%s_%s_%s", d.Customer, d.PlatformId, d.ProjectId), "-", "_", -1)
}

func (d *Document) SaveTo(collection *mgo.Collection) error {
	if _, err := collection.UpsertId(d.Id, d); err != nil {
		return fmt.Errorf("upsert %s: %w", d.Id, err)
	}

	indexes := []string{"job_execution_id", "time"}

	if err := collection.EnsureIndexKey(indexes...); err != nil {
		return fmt.Errorf("ensure indexes %v: %w", indexes, err)
	}

	return nil
}
