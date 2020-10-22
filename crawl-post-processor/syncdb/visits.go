package syncdb

import (
	"database/sql"
	"log"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/lib/pq"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// ------------------------------------------------------------------------------
// Sync visitChain events
// ------------------------------------------------------------------------------

// syncVisitChainInputRecord identifies/holds the skeleton of information extracted from a Mongo `events` record with event=visitChain
type syncVisitChainInputRecord struct {
	PageID     bson.ObjectId `bson:"_id"`
	VisitLinks []string      `bson:"urls"`
	LoggedWhen time.Time     `bson:"last_when"`
}

// visitChainsImportFields holds the in-order list of field names used for bulk-inserting crawl records into our temp `visit_chains_import_schema` clone
var visitChainsImportFields = [...]string{
	"page_mongo_oid",
	"visit_links",
	"logged_when",
}

// getSyncVisitChainIter looks up the latest imported visit_chains in <sqlDb> and generates an iterator over newer visit_chains in <db>
func getSyncVisitChainIter(db *mgo.Database, sqlDb *sql.DB) (*mgo.Iter, error) {
	sourceMatch := bson.M{
		"event": "visit",
	}

	// optionally add date-range filtering on `date`
	dateRange, err := getBeforeAfterFilter()
	if err != nil {
		return nil, err
	} else if len(dateRange) > 0 {
		sourceMatch["date"] = dateRange
	}

	// Build a projection map for just the fields we need for deserialization of our record types
	sourceProject, err := BuildProjection(reflect.TypeOf(syncVisitChainInputRecord{}))
	if err != nil {
		return nil, err
	}

	// Build a big honking aggregation pipeline to include blob lookups for DOM/screenshot
	bigHonkingQuery := []bson.M{
		{"$match": sourceMatch},
		{"$group": bson.M{
			"_id":       "$page",
			"last_when": bson.M{"$max": "$date"},
			"links":     bson.M{"$sum": 1},
			"urls":      bson.M{"$push": "$url"},
		}},
		{"$match": bson.M{"links": bson.M{"$gt": 1}}},
		{"$project": sourceProject},
	}
	if rawSkip, ok := os.LookupEnv("SKIP"); ok {
		if skip, err := strconv.Atoi(rawSkip); err == nil {
			bigHonkingQuery = append(bigHonkingQuery, bson.M{"$skip": skip})
		} else {
			log.Printf("getSyncVisitChainIter: WARNING, malformed 'SKIP' ENV var '%s' (%v)\n", rawSkip, err)
		}
	}
	if rawLimit, ok := os.LookupEnv("LIMIT"); ok {
		if limit, err := strconv.Atoi(rawLimit); err == nil {
			bigHonkingQuery = append(bigHonkingQuery, bson.M{"$limit": limit})
		} else {
			log.Printf("getSyncVisitChainIter: WARNING, malformed 'LIMIT' ENV var '%s' (%v)\n", rawLimit, err)
		}
	}
	return db.C("events").Pipe(bigHonkingQuery).AllowDiskUse().Iter(), nil
}

func syncVisitChains(db *mgo.Database, sqlDb *sql.DB) error {
	log.Println("syncVisitChains: getting new-visit-chains iterator...")
	iter, err := getSyncVisitChainIter(db, sqlDb)
	if err != nil {
		return err
	}
	defer func() {
		log.Println("syncVisitChains: closing new-visit-chains iterator...")
		iter.Close()
	}()

	log.Println("syncVisitChains: creating temp table 'import_visit_chains'...")
	err = CreateImportTable(sqlDb, "visit_chains_import_schema", "import_visit_chains")
	if err != nil {
		log.Printf("syncVisitChains: createImportTable(...) failed: %v\n", err)
		return err
	}

	log.Println("syncVisitChains: bulk-inserting...")
	importRows, err := BulkInsertRows(sqlDb, "syncVisitChains", "import_visit_chains", visitChainsImportFields[:], func() ([]interface{}, error) {
		var record syncVisitChainInputRecord
		if iter.Next(&record) {
			values := []interface{}{
				[]byte(record.PageID),
				pq.Array(record.VisitLinks),
				record.LoggedWhen,
			}
			return values, nil
		}
		log.Printf("syncVisitChains: closing iterator and committing transation...\n")
		err := iter.Close()
		if err != nil {
			return nil, err // signal error/abort
		}
		return nil, nil // signal end-of-stream
	})
	if err != nil {
		return err
	}

	log.Println("syncVisitChains: copy-inserting from temp table...")
	result, err := sqlDb.Exec(`
INSERT INTO visit_chains (page_id, visit_links, logged_when)
	SELECT p.id, it.visit_links, it.logged_when
	FROM import_visit_chains AS it
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
	log.Printf("syncVisitChains: inserted %d (out of %d) import rows\n", insertRows, importRows)

	return nil
}
