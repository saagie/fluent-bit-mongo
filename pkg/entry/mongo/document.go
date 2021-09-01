package mongo

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

func Convert(record map[interface{}]interface{}) (*Document, error) {
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

const (
	LogKey            = "log"
	StreamKey         = "stream"
	TimeKey           = "time"
	JobExecutionIDKey = "job_execution_id"
	ProjectIDKey      = "project_id"
	CustomerKey       = "customer"
	PlatformIDKey     = "platform_id"
)

func (d *Document) Populate(record map[interface{}]interface{}) (err error) {
	d.Log, err = parse.ExtractStringValue(record, LogKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", LogKey, err)
	}

	d.Stream, err = parse.ExtractStringValue(record, StreamKey)
	if err != nil {
		if !errors.Is(err, parse.ErrKeyNotFound) {
			return fmt.Errorf("parse %s: %w", StreamKey, err)
		}

		d.Stream = "stdout"
	}

	d.Time, err = parse.ExtractStringValue(record, TimeKey)
	if err != nil && !errors.Is(err, parse.ErrKeyNotFound) {
		return fmt.Errorf("parse %s: %w", TimeKey, err)
	}

	d.JobExecutionId, err = parse.ExtractStringValue(record, JobExecutionIDKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", JobExecutionIDKey, err)
	}

	d.ProjectId, err = parse.ExtractStringValue(record, ProjectIDKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", ProjectIDKey, err)
	}

	d.Customer, err = parse.ExtractStringValue(record, CustomerKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", CustomerKey, err)
	}

	d.PlatformId, err = parse.ExtractStringValue(record, PlatformIDKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", PlatformIDKey, err)
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
