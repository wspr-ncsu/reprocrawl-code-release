package syncdb

import (
	"database/sql"
	"log"
	"reflect"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// ------------------------------------------------------------------------------
// Sync frames
// ------------------------------------------------------------------------------

// syncFrameInputRecord identifies/holds the skeleton of information extracted from a Mongo `frames` record
type syncFrameInputRecord struct {
	MongoID       bson.ObjectId `bson:"_id"`
	PageID        bson.ObjectId `bson:"page"`
	FrameID       string        `bson:"frameId"`
	ParentFrameID string        `bson:"parentFrameId"`
	MainFrame     bool          `bson:"mainFrame"`
}

// framesImportFields holds the in-order list of field names used for bulk-inserting crawl records into our temp `frames_import_schema` clone
var framesImportFields = [...]string{
	"mongo_oid",
	"page_mongo_oid",
	"token",
	"parent_token",
	"is_main",
}

// getSyncFrameIter looks up the latest imported frames in <sqlDb> and generates an iterator over newer frames in <db>
func getSyncFrameIter(db *mgo.Database, sqlDb *sql.DB) (*mgo.Iter, error) {
	sourceMatch := bson.M{}

	// optionally add date-range filtering on _id (the timestamp sub-field)
	dateRange, err := getBeforeAfterFilterOid()
	if err != nil {
		return nil, err
	} else if len(dateRange) > 0 {
		sourceMatch["_id"] = dateRange
	}

	// Build a projection map for just the fields we need for deserialization of our record types
	sourceProject, err := BuildProjection(reflect.TypeOf(syncFrameInputRecord{}))
	if err != nil {
		return nil, err
	}

	// Query and return the records of interest
	return db.C("frames").Find(sourceMatch).Select(sourceProject).Iter(), nil
}

func syncFrames(db *mgo.Database, sqlDb *sql.DB) error {
	log.Println("syncFrames: getting new-frames iterator...")
	iter, err := getSyncFrameIter(db, sqlDb)
	if err != nil {
		return err
	}
	defer func() {
		log.Println("syncFrames: closing new-frames iterator...")
		iter.Close()
	}()

	log.Println("syncFrames: creating temp table 'import_frames'...")
	err = CreateImportTable(sqlDb, "frames_import_schema", "import_frames")
	if err != nil {
		log.Printf("syncFrames: createImportTable(...) failed: %v\n", err)
		return err
	}

	log.Println("syncFrames: bulk-inserting...")
	importRows, err := BulkInsertRows(sqlDb, "syncFrames", "import_frames", framesImportFields[:], func() ([]interface{}, error) {
		var record syncFrameInputRecord
		if iter.Next(&record) {
			values := []interface{}{
				[]byte(record.MongoID),
				[]byte(record.PageID),
				record.FrameID,
				record.ParentFrameID,
				record.MainFrame,
			}
			return values, nil
		}
		log.Printf("syncFrames: closing iterator and committing transation...\n")
		err := iter.Close()
		if err != nil {
			return nil, err // signal error/abort
		}
		return nil, nil // signal end-of-stream
	})
	if err != nil {
		return err
	}

	log.Println("syncFrames: copy-inserting from temp table...")
	result, err := sqlDb.Exec(`
INSERT INTO frames (mongo_oid, page_id, token, is_main)
	SELECT it.mongo_oid, p.id, it.token, it.is_main
	FROM import_frames AS it
		LEFT JOIN pages AS p
			ON (p.mongo_oid = it.page_mongo_oid)
ON CONFLICT DO NOTHING;
`)
	if err != nil {
		return err
	}
	insertRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	log.Printf("syncFrames: inserted %d (out of %d) import rows\n", insertRows, importRows)

	log.Println("syncFrames: updating parent-frame-id records...")
	result, err = sqlDb.Exec(`
UPDATE frames AS f1
SET parent_id = f2.id
FROM import_frames AS it, frames AS f2
WHERE (f1.mongo_oid = it.mongo_oid) and (it.parent_token = f2.token)
`)
	if err != nil {
		return err
	}
	updateRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	log.Printf("syncFrames: updated %d (out of %d) import rows\n", updateRows, importRows)

	return nil
}
