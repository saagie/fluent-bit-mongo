package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/saagie/fluent-bit-mongo/pkg/entry"
	"github.com/saagie/fluent-bit-mongo/pkg/log"
	mgo "gopkg.in/mgo.v2"
)

type processor struct {
	mongoSession *mgo.Session
}

func New(session *mgo.Session) entry.Processor {
	return &processor{
		mongoSession: session,
	}
}

const MongoDefaultDB = ""

func (p *processor) ProcessRecord(ctx context.Context, ts time.Time, record map[interface{}]interface{}) error {
	logger, err := log.GetLogger(ctx)
	if err != nil {
		return fmt.Errorf("get logger: %w", err)
	}

	logDoc, err := Convert(ctx, ts, record)
	if err != nil {
		logger.Error("Failed to convert record to document", map[string]interface{}{
			"error": err,
		})

		return fmt.Errorf("new document: %w", err)
	}

	collection := p.mongoSession.DB(MongoDefaultDB).C(logDoc.CollectionName())

	logger.Debug("Flushing to mongo", map[string]interface{}{
		"document.id": logDoc.GetID(),
	})

	if err := logDoc.SaveTo(collection); err != nil {
		logger.Error("Failed to save document", map[string]interface{}{
			"document":   logDoc,
			"collection": collection.FullName,
			"error":      err,
		})

		return &entry.ErrRetry{Cause: err}
	}

	return nil
}
