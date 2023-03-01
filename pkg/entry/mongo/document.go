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

type LogEntry interface {
	Populate(ctx context.Context, ts time.Time, record map[interface{}]interface{}) error
	CollectionName() string
	SaveTo(collection *mgo.Collection) error
	GetID() bson.ObjectId
}

type LogDocument struct {
	Id         bson.ObjectId `bson:"_id,omitempty"`
	Log        string        `bson:"log"`
	Stream     string        `bson:"stream"`
	Time       string        `bson:"time"`
	ProjectId  string        `bson:"project_id"`
	Customer   string        `bson:"customer"`
	PlatformId string        `bson:"platform_id"`
}

func (d *LogDocument) GetID() bson.ObjectId {
	return d.Id
}

type JobLogDocument struct {
	LogDocument    `bson:",inline"`
	JobExecutionId string `bson:"job_execution_id"`
}

type AppLogDocument struct {
	LogDocument    `bson:",inline"`
	AppExecutionId string `bson:"app_execution_id"`
	AppId          string `bson:"app_id"`
	ContainerId    string `bson:"container_id"`
}

type ConditionPipelineLogDocument struct {
	LogDocument          `bson:",inline"`
	ConditionExecutionId string `bson:"condition_execution_id"`
	ConditionNodeId      string `bson:"condition_node_id"`
	PipelineExecutionId  string `bson:"pipeline_execution_id"`
}

func Convert(ctx context.Context, ts time.Time, record map[interface{}]interface{}) (LogEntry, error) {
	var doc LogEntry

	if isJobLog(record) {
		doc = &JobLogDocument{}
	} else if isAppLog(record) {
		doc = &AppLogDocument{}
	} else {
		doc = &ConditionPipelineLogDocument{}
	}

	if err := doc.Populate(ctx, ts, record); err != nil {
		return nil, fmt.Errorf("populate document: %w", err)
	}

	return doc, nil
}

const (
	LogKey                  = "log"
	StreamKey               = "stream"
	TimeKey                 = "time"
	LogPrefixKey            = "log_prefix"
	JobExecutionIDKey       = "job_execution_id"
	ContainerIDKey          = "container_id"
	AppExecutionIDKey       = "app_execution_id"
	AppIDKey                = "app_id"
	ConditionExecutionIDKey = "condition_execution_id"
	ConditionNodeIDKey      = "condition_node_id"
	PipelineExecutionIDKey  = "pipeline_execution_id"
	ProjectIDKey            = "project_id"
	CustomerKey             = "customer"
	PlatformIDKey           = "platform_id"
)

func isJobLog(record map[interface{}]interface{}) bool {
	_, err := parse.ExtractStringValue(record, JobExecutionIDKey)
	return err == nil
}

func isAppLog(record map[interface{}]interface{}) bool {
	_, err := parse.ExtractStringValue(record, AppExecutionIDKey)
	return err == nil
}

func (d *AppLogDocument) Populate(ctx context.Context, ts time.Time, record map[interface{}]interface{}) error {
	err := d.LogDocument.Populate(ctx, ts, record)
	if err != nil {
		return fmt.Errorf("populate: %w", err)
	}

	d.AppExecutionId, err = parse.ExtractStringValue(record, AppExecutionIDKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", AppExecutionIDKey, err)
	}

	d.AppId, err = parse.ExtractStringValue(record, AppIDKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", AppIDKey, err)
	}

	d.ContainerId, err = parse.ExtractStringValue(record, ContainerIDKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", ContainerIDKey, err)
	}

	return d.generateObjectID()
}

func (d *JobLogDocument) Populate(ctx context.Context, ts time.Time, record map[interface{}]interface{}) error {
	err := d.LogDocument.Populate(ctx, ts, record)
	if err != nil {
		return fmt.Errorf("populate: %w", err)
	}

	d.JobExecutionId, err = parse.ExtractStringValue(record, JobExecutionIDKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", JobExecutionIDKey, err)
	}

	return d.generateObjectID()
}

func (d *ConditionPipelineLogDocument) Populate(ctx context.Context, ts time.Time, record map[interface{}]interface{}) error {
	err := d.LogDocument.Populate(ctx, ts, record)
	if err != nil {
		return fmt.Errorf("populate: %w", err)
	}

	d.ConditionExecutionId, err = parse.ExtractStringValue(record, ConditionExecutionIDKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", ConditionExecutionIDKey, err)
	}

	d.ConditionNodeId, err = parse.ExtractStringValue(record, ConditionNodeIDKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", ConditionNodeIDKey, err)
	}

	d.PipelineExecutionId, err = parse.ExtractStringValue(record, PipelineExecutionIDKey)
	if err != nil {
		return fmt.Errorf("parse %s: %w", PipelineExecutionIDKey, err)
	}

	return d.generateObjectID()
}

func cleanLogContent(content string) string {
	return strings.TrimSuffix(content, "\n")
}

func (d *LogDocument) Populate(ctx context.Context, ts time.Time, record map[interface{}]interface{}) error {
	logger, err := log.GetLogger(ctx)
	if err != nil {
		return fmt.Errorf("get logger: %w", err)
	}

	logContent, err := parse.ExtractStringValue(record, LogKey)
	if err != nil {
		if !errors.Is(err, &parse.ErrKeyNotFound{
			LookingFor: LogKey,
		}) {
			return fmt.Errorf("parse %s: %w", LogKey, err)
		}

		logger.Debug("Key not found", map[string]interface{}{
			"error": err,
		})

		d.Log = ""
	}
	d.Log = cleanLogContent(logContent)

	logPrefix, err := parse.ExtractStringValue(record, LogPrefixKey)
	if err != nil {
		if !errors.Is(err, &parse.ErrKeyNotFound{
			LookingFor: LogPrefixKey,
		}) {
			return fmt.Errorf("parse %s: %w", LogPrefixKey, err)
		}

		logger.Debug("Key not found, log_prefix is inactivated", map[string]interface{}{
			"error": err,
		})
	}

	d.Stream, err = parse.ExtractStringValue(record, StreamKey)
	if err != nil {
		if !errors.Is(err, &parse.ErrKeyNotFound{
			LookingFor: StreamKey,
		}) {
			return fmt.Errorf("parse %s: %w", StreamKey, err)
		}

		logger.Debug("Key not found, use stdout", map[string]interface{}{
			"error": err,
		})
		d.Stream = "stdout"
	}

	if logPrefix != "" {
		logPrefixPattern := "[" + logPrefix + "]"
		if strings.HasPrefix(logContent, logPrefixPattern) {
			d.Stream = logPrefix + "_" + d.Stream
			d.Log = strings.Replace(d.Log, logPrefixPattern, "", 1)
		}
	}

	recordTime, err := parse.ExtractStringValue(record, TimeKey)
	if err != nil {
		if !errors.Is(err, &parse.ErrKeyNotFound{
			LookingFor: TimeKey,
		}) {
			return fmt.Errorf("parse %s: %w", TimeKey, err)
		}

		logger.Debug("Key not found, use value from fluentbit processor", map[string]interface{}{
			"error": err,
		})

		if !ts.IsZero() {
			d.Time = ts.Format(TimeFormat)
		}
	} else {
		d.Time = recordTime
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

	return nil
}

func (d *LogDocument) generateObjectID() error {
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

func (d *LogDocument) CollectionName() string {
	return strings.Replace(fmt.Sprintf("%s_%s_%s", d.Customer, d.PlatformId, d.ProjectId), "-", "_", -1)
}

func (d *JobLogDocument) SaveTo(collection *mgo.Collection) error {
	if _, err := collection.UpsertId(d.Id, d); err != nil {
		return fmt.Errorf("upsert %s: %w", d.Id, err)
	}

	indexes := []string{"job_execution_id", "time"}

	if err := collection.EnsureIndexKey(indexes...); err != nil {
		return fmt.Errorf("ensure indexes %v: %w", indexes, err)
	}

	return nil
}

func (d *AppLogDocument) SaveTo(collection *mgo.Collection) error {
	if _, err := collection.UpsertId(d.Id, d); err != nil {
		return fmt.Errorf("upsert %s: %w", d.Id, err)
	}

	indexes := []string{"app_execution_id", "container_id", "time"}

	if err := collection.EnsureIndexKey(indexes...); err != nil {
		return fmt.Errorf("ensure indexes %v: %w", indexes, err)
	}

	return nil
}

func (d *ConditionPipelineLogDocument) SaveTo(collection *mgo.Collection) error {
	if _, err := collection.UpsertId(d.Id, d); err != nil {
		return fmt.Errorf("upsert %s: %w", d.Id, err)
	}

	indexes := []string{"condition_execution_id", "time"}

	if err := collection.EnsureIndexKey(indexes...); err != nil {
		return fmt.Errorf("ensure indexes %v: %w", indexes, err)
	}

	return nil
}
