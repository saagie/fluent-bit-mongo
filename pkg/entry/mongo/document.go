package mongo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/saagie/fluent-bit-mongo/pkg/log"
	"github.com/saagie/fluent-bit-mongo/pkg/parse"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const TimeFormat = time.RFC3339Nano

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

func Convert(ctx context.Context, ts time.Time, record map[interface{}]interface{}) (*Document, error) {
	doc := &Document{}

	if !ts.IsZero() {
		doc.Time = ts.Format(TimeFormat)
	}

	if err := doc.Populate(ctx, record); err != nil {
		return nil, fmt.Errorf("populate document: %w", err)
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

func (d *Document) Populate(ctx context.Context, record map[interface{}]interface{}) (err error) {
	logger, err := log.GetLogger(ctx)
	if err != nil {
		return fmt.Errorf("get logger: %w", err)
	}

	d.Log, err = parse.ExtractStringValue(record, LogKey)
	if err != nil {
		if !errors.Is(err, &parse.ErrKeyNotFound{}) {
			return fmt.Errorf("parse %s: %w", LogKey, err)
		}

		logger.Info("Key not found", map[string]interface{}{
			"error": err,
		})

		d.Log = ""
	}

	d.Stream, err = parse.ExtractStringValue(record, StreamKey)
	if err != nil {
		if !errors.Is(err, &parse.ErrKeyNotFound{}) {
			return fmt.Errorf("parse %s: %w", StreamKey, err)
		}

		logger.Info("Key not found, use stdout", map[string]interface{}{
			"error": err,
		})

		d.Stream = "stdout"
	}

	ts, err := parse.ExtractStringValue(record, TimeKey)
	if err != nil {
		if !errors.Is(err, &parse.ErrKeyNotFound{}) {
			return fmt.Errorf("parse %s: %w", TimeKey, err)
		}

		logger.Info("Key not found", map[string]interface{}{
			"error": err,
		})
	} else {
		d.Time = ts
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
