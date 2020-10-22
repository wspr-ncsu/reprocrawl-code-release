package syncdb

import (
	"database/sql"
	"log"
	"reflect"
	"time"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// ------------------------------------------------------------------------------
// Sync targetSquashed events
// ------------------------------------------------------------------------------

// syncSquashedTargetInputRecord identifies/holds the skeleton of information extracted from a Mongo `events` record with event=targetSquashed
type syncSquashedTargetInputRecord struct {
	MongoID    bson.ObjectId `bson:"_id"`
	PageID     bson.ObjectId `bson:"page"`
	LoggedWhen time.Time     `bson:"date"`
	TargetURL  string        `bson:"url"`
}

// squashedTargetsImportFields holds the in-order list of field names used for bulk-inserting crawl records into our temp `squashed_targets_import_schema` clone
var squashedTargetsImportFields = [...]string{
	"mongo_oid",
	"page_mongo_oid",
	"target_url_sha256",
	"logged_when",
}

// getSyncSquashedTargetIter looks up the latest imported squashed_targets in <sqlDb> and generates an iterator over newer squashed_targets in <db>
func getSyncSquashedTargetIter(db *mgo.Database, sqlDb *sql.DB) (*mgo.Iter, error) {
	sourceMatch := bson.M{
		"event": "targetSquashed",
	}

	// optionally add date-range filtering on `date`
	dateRange, err := getBeforeAfterFilter()
	if err != nil {
		return nil, err
	} else if len(dateRange) > 0 {
		sourceMatch["date"] = dateRange
	}

	// Build a projection map for just the fields we need for deserialization of our record types
	sourceProject, err := BuildProjection(reflect.TypeOf(syncSquashedTargetInputRecord{}))
	if err != nil {
		return nil, err
	}

	// Query and return the records of interest
	return db.C("events").Find(sourceMatch).Select(sourceProject).Iter(), nil
}

func syncSquashedTargets(db *mgo.Database, sqlDb *sql.DB) error {
	log.Println("syncSquashedTargets: getting new-squashed-targets iterator...")
	iter, err := getSyncSquashedTargetIter(db, sqlDb)
	if err != nil {
		return err
	}
	defer func() {
		log.Println("syncSquashedTargets: closing new-squashed-targets iterator...")
		iter.Close()
	}()

	log.Println("syncSquashedTargets: creating temp table 'import_squashed_targets'...")
	err = CreateImportTable(sqlDb, "squashed_targets_import_schema", "import_squashed_targets")
	if err != nil {
		log.Printf("syncSquashedTargets: createImportTable(...) failed: %v\n", err)
		return err
	}

	ub := NewURLBakery()

	log.Println("syncSquashedTargets: bulk-inserting...")
	importRows, err := BulkInsertRows(sqlDb, "syncSquashedTargets", "import_squashed_targets", squashedTargetsImportFields[:], func() ([]interface{}, error) {
		var record syncSquashedTargetInputRecord
		if iter.Next(&record) {
			var nullableURLSha256 []byte
			if record.TargetURL != "" {
				urlHash := ub.URLToHash(record.TargetURL)
				nullableURLSha256 = urlHash[:]
			}
			values := []interface{}{
				[]byte(record.MongoID),
				[]byte(record.PageID),
				nullableURLSha256,
				record.LoggedWhen,
			}
			return values, nil
		}
		log.Printf("syncSquashedTargets: closing iterator and committing transation...\n")
		err := iter.Close()
		if err != nil {
			return nil, err // signal error/abort
		}
		return nil, nil // signal end-of-stream
	})
	if err != nil {
		return err
	}

	log.Printf("syncSquashedTargets: inserting cooked URLs referenced by inserted squashed target events...")
	err = ub.InsertBakedURLs(sqlDb)
	if err != nil {
		return err
	}

	log.Println("syncSquashedTargets: copy-inserting from temp table...")
	result, err := sqlDb.Exec(`
INSERT INTO squashed_targets (
		mongo_oid, page_id,
		target_url_id, logged_when)
	SELECT 
		it.mongo_oid, p.id,
		u.id, it.logged_when
	FROM import_squashed_targets AS it
		LEFT JOIN pages AS p
			ON (p.mongo_oid = it.page_mongo_oid)
		LEFT JOIN urls AS u
			ON (u.sha256 = it.target_url_sha256)
ON CONFLICT DO NOTHING;
`)
	if err != nil {
		return err
	}
	insertRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	log.Printf("syncSquashedTargets: inserted %d (out of %d) import rows\n", insertRows, importRows)

	return nil
}
