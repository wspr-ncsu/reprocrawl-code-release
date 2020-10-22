package syncdb

import (
	"database/sql"
	"encoding/hex"
	"log"
	"reflect"
	"time"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// ------------------------------------------------------------------------------
// Sync scriptParsed events
// ------------------------------------------------------------------------------

// syncParsedScriptInputRecord identifies/holds the skeleton of information extracted from a Mongo `events` record with event=scriptParsed
type syncParsedScriptInputRecord struct {
	MongoID    bson.ObjectId `bson:"_id"`
	PageID     bson.ObjectId `bson:"page"`
	LoggedWhen time.Time     `bson:"date"`
	ScriptURL  string        `bson:"url"`
	ScriptHash string        `bson:"blobHash"`
}

// parsedScriptsImportFields holds the in-order list of field names used for bulk-inserting crawl records into our temp `request_inits_import_schema` clone
var parsedScriptsImportFields = [...]string{
	"mongo_oid",
	"page_mongo_oid",
	"script_url_sha256",
	"script_hash",
	"logged_when",
}

// getSyncParsedScriptIter looks up the latest imported parsed_scripts in <sqlDb> and generates an iterator over newer parsed_scripts in <db>
func getSyncParsedScriptIter(db *mgo.Database, sqlDb *sql.DB) (*mgo.Iter, error) {
	sourceMatch := bson.M{
		"event": "scriptParsed",
	}

	// optionally add date-range filtering on `date`
	dateRange, err := getBeforeAfterFilter()
	if err != nil {
		return nil, err
	} else if len(dateRange) > 0 {
		sourceMatch["date"] = dateRange
	}

	// Build a projection map for just the fields we need for deserialization of our record types
	sourceProject, err := BuildProjection(reflect.TypeOf(syncParsedScriptInputRecord{}))
	if err != nil {
		return nil, err
	}

	// Query and return the records of interest
	return db.C("events").Find(sourceMatch).Select(sourceProject).Iter(), nil
}

func syncParsedScripts(db *mgo.Database, sqlDb *sql.DB) error {
	log.Println("syncParsedScripts: getting new-parsed-scripts iterator...")
	iter, err := getSyncParsedScriptIter(db, sqlDb)
	if err != nil {
		return err
	}
	defer func() {
		log.Println("syncParsedScripts: closing new-parsed-scripts iterator...")
		iter.Close()
	}()

	log.Println("syncParsedScripts: creating temp table 'import_parsed_scripts'...")
	err = CreateImportTable(sqlDb, "parsed_scripts_import_schema", "import_parsed_scripts")
	if err != nil {
		log.Printf("syncParsedScripts: createImportTable(...) failed: %v\n", err)
		return err
	}

	ub := NewURLBakery()

	log.Println("syncParsedScripts: bulk-inserting...")
	importRows, err := BulkInsertRows(sqlDb, "syncParsedScripts", "import_parsed_scripts", parsedScriptsImportFields[:], func() ([]interface{}, error) {
		var record syncParsedScriptInputRecord
		if iter.Next(&record) {
			var nullableScriptHash []byte
			if record.ScriptHash != "" {
				nullableScriptHash, err = hex.DecodeString(record.ScriptHash)
				if err != nil {
					return nil, err
				}
			}

			var nullableScriptURLSha256 []byte
			if record.ScriptURL != "" {
				urlHash := ub.URLToHash(record.ScriptURL)
				nullableScriptURLSha256 = urlHash[:]
			}

			values := []interface{}{
				[]byte(record.MongoID),
				[]byte(record.PageID),
				nullableScriptURLSha256,
				nullableScriptHash,
				record.LoggedWhen,
			}
			return values, nil
		}
		log.Printf("syncParsedScripts: closing iterator and committing transation...\n")
		err := iter.Close()
		if err != nil {
			return nil, err // signal error/abort
		}
		return nil, nil // signal end-of-stream
	})
	if err != nil {
		return err
	}

	log.Printf("syncParsedScripts: inserting cooked URLs referenced by inserted script parse events...")
	err = ub.InsertBakedURLs(sqlDb)
	if err != nil {
		return err
	}

	log.Println("syncParsedScripts: copy-inserting from temp table...")
	result, err := sqlDb.Exec(`
INSERT INTO parsed_scripts (
		mongo_oid, page_id,
		script_url_id, script_hash, logged_when)
	SELECT 
		it.mongo_oid, p.id,
		u.id, it.script_hash, it.logged_when
	FROM import_parsed_scripts AS it
		LEFT JOIN pages AS p
			ON (p.mongo_oid = it.page_mongo_oid)
		LEFT JOIN urls AS u
			ON (u.sha256 = it.script_url_sha256)
ON CONFLICT DO NOTHING;
`)
	if err != nil {
		return err
	}
	insertRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	log.Printf("syncParsedScripts: inserted %d (out of %d) import rows\n", insertRows, importRows)

	return nil
}
