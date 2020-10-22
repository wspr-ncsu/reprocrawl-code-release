package syncdb2020

import (
	"database/sql"
	"log"
	"reflect"
	"vpp/syncdb"

	"github.com/lib/pq"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// ------------------------------------------------------------------------------
// Sync scriptParsed events
// ------------------------------------------------------------------------------

// syncJSAPIUsageInputRecord identifies/holds the skeleton of information extracted from a Mongo `events` record with event=scriptParsed
type syncJSAPIUsageInputRecord struct {
	MongoID       bson.ObjectId `bson:"_id"`
	PageID        bson.ObjectId `bson:"pageId"`
	FeatureOrigin struct {
		Origin   string   `bson:"origin"`
		Features []string `bson:"features"`
	} `bson:"featureOrigins"`
}

// jsAPIUsageImportFields holds the in-order list of field names used for bulk-inserting crawl records into our temp `js_api_usage_import_schema` clone
var jsAPIUsageImportFields = [...]string{
	"mongo_oid",
	"origin_url_sha256",
	"page_mongo_oid",
	"js_apis",
	"logged_when",
}

// getSyncJSAPIUsageIter returns all matching js_api_features records from MongoDB
func getSyncJSAPIUsageIter(db *mgo.Database, pageOid bson.ObjectId) (*mgo.Iter, error) {
	sourceMatch := bson.M{
		"pageId": pageOid,
	}

	// Build a projection map for just the fields we need for deserialization of our record types
	sourceProject, err := syncdb.BuildProjection(reflect.TypeOf(syncJSAPIUsageInputRecord{}))
	if err != nil {
		return nil, err
	}

	// Use an aggregation pipeline to crack apart per-page records into per-feature-origin records
	// Build a big honking aggregation pipeline to include blob lookups for DOM/screenshot
	bigHonkingQuery := []bson.M{
		{"$match": sourceMatch},
		{"$project": sourceProject},
		{"$unwind": "$featureOrigins"},
	}

	// Query and return the records of interest
	return db.C("js_api_features").Pipe(bigHonkingQuery).AllowDiskUse().Iter(), nil
}

func syncJSAPIUsage(db *mgo.Database, sqlDb *sql.DB, pageOid bson.ObjectId) error {
	log.Println("syncJSAPIUsage: getting per-page JS API usage iterator...")
	iter, err := getSyncJSAPIUsageIter(db, pageOid)
	if err != nil {
		return err
	}
	defer func() {
		log.Println("syncJSAPIUsage: closing iterator...")
		iter.Close()
	}()

	log.Println("syncJSAPIUsage: creating temp table 'import_js_api_usage'...")
	err = syncdb.CreateImportTable(sqlDb, "js_api_usage_import_schema", "import_js_api_usage")
	if err != nil {
		log.Printf("syncJSAPIUsage: createImportTable(...) failed: %v\n", err)
		return err
	}

	ub := syncdb.NewURLBakery()

	log.Println("syncJSAPIUsage: bulk-inserting...")
	importRows, err := syncdb.BulkInsertRows(sqlDb, "syncJSAPIUsage", "import_js_api_usage", jsAPIUsageImportFields[:], func() ([]interface{}, error) {
		var record syncJSAPIUsageInputRecord
		if iter.Next(&record) {
			originHash := ub.URLToHash(record.FeatureOrigin.Origin)
			values := []interface{}{
				[]byte(record.MongoID),
				originHash[:],
				[]byte(record.PageID),
				pq.Array(record.FeatureOrigin.Features),
				record.MongoID.Time(),
			}
			return values, nil
		}
		log.Printf("syncJSAPIUsage: closing iterator and committing transation...\n")
		err := iter.Close()
		if err != nil {
			return nil, err // signal error/abort
		}
		return nil, nil // signal end-of-stream
	})
	if err != nil {
		return err
	}

	log.Printf("syncJSAPIUsage: inserting cooked URLs referenced by inserted JS API usage lists...")
	err = ub.InsertBakedURLs(sqlDb)
	if err != nil {
		return err
	}

	log.Println("syncJSAPIUsage: copy-inserting from temp table...")
	result, err := sqlDb.Exec(`
INSERT INTO js_api_usage (
		page_id, mongo_oid,
		origin_url_id, js_apis, logged_when)
	SELECT 
		p.id, it.mongo_oid,
		u.id, it.js_apis, it.logged_when
	FROM import_js_api_usage AS it
		LEFT JOIN pages AS p
			ON (p.mongo_oid = it.page_mongo_oid)
		LEFT JOIN urls AS u
			ON (u.sha256 = it.origin_url_sha256)
ON CONFLICT DO NOTHING;
`)
	if err != nil {
		return err
	}
	insertRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	log.Printf("syncJSAPIUsage: inserted %d (out of %d) import rows\n", insertRows, importRows)

	return nil
}
