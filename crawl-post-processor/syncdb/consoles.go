package syncdb

import (
	"database/sql"
	"encoding/json"
	"log"
	"time"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// ------------------------------------------------------------------------------
// Sync consoleError events
// ------------------------------------------------------------------------------

// syncConsoleErrorInputRecord identifies/holds the skeleton of information extracted from an aggregated events query result
type syncConsoleErrorInputRecord struct {
	Key struct {
		PageID bson.ObjectId `bson:"page"`
		Type   string        `bson:"type"`
	} `bson:"_id"`
	Count    int       `bson:"count"`
	LastWhen time.Time `bson:"last_when"`
}

// consoleErrorsImportFields holds the in-order list of field names used for bulk-inserting summary records into our temp `console_error_summary_import_schema` clone
var consoleErrorsImportFields = [...]string{
	"page_mongo_oid",
	"total",
	"categories",
	"last_when",
}

// getSyncConsoleErrorIter looks up the latest imported console_errors in <sqlDb> and generates an iterator over newer console_errors in <db>
// (it also performs MongoDB aggregation to return only per-page/type counts)
func getSyncConsoleErrorIter(db *mgo.Database, sqlDb *sql.DB) (*mgo.Iter, error) {
	sourceMatch := bson.M{
		"event": "consoleError",
	}

	// optionally add date-range filtering on `date`
	dateRange, err := getBeforeAfterFilter()
	if err != nil {
		return nil, err
	} else if len(dateRange) > 0 {
		sourceMatch["date"] = dateRange
	}

	// Query and return the records of interest
	bigHonkingQuery := []bson.M{
		{"$match": sourceMatch},
		{"$project": bson.M{"_id": 0, "page": 1, "type": 1, "date": 1}},
		{"$group": bson.M{
			"_id":       bson.M{"page": "$page", "type": "$type"},
			"last_when": bson.M{"$max": "$date"},
			"count":     bson.M{"$sum": 1},
		}},
	}
	return db.C("events").Pipe(bigHonkingQuery).AllowDiskUse().Iter(), nil
}

func syncConsoleErrors(db *mgo.Database, sqlDb *sql.DB) error {
	log.Println("syncConsoleErrors: getting new-console-errors iterator...")
	iter, err := getSyncConsoleErrorIter(db, sqlDb)
	if err != nil {
		return err
	}
	defer func() {
		log.Println("syncConsoleErrors: closing new-console-errors iterator...")
		iter.Close()
	}()

	// Slurp up all the records, aggregating in memory
	type errorAggRecord struct {
		pageOid    bson.ObjectId
		total      int
		categories bson.M
		lastWhen   time.Time
	}
	pageMap := make(map[bson.ObjectId]*errorAggRecord, 1024)
	var record syncConsoleErrorInputRecord
	for iter.Next(&record) {
		pageHits, ok := pageMap[record.Key.PageID]
		if !ok {
			pageHits = &errorAggRecord{
				pageOid:    record.Key.PageID,
				total:      0,
				categories: make(bson.M, 8),
				lastWhen:   record.LastWhen,
			}
			pageMap[record.Key.PageID] = pageHits
		}
		pageHits.total += record.Count
		pageHits.categories[record.Key.Type] = record.Count
		if record.LastWhen.After(pageHits.lastWhen) {
			pageHits.lastWhen = record.LastWhen
		}
	}
	if err = iter.Close(); err != nil {
		log.Printf("syncConsoleErrors: iterator input error (%v)\n", err)
		return err
	}

	// Now bulk-insert that all into a temp table in PG
	log.Println("syncConsoleErrors: creating temp table 'import_console_errors'...")
	err = CreateImportTable(sqlDb, "console_errors_import_schema", "import_console_errors")
	if err != nil {
		log.Printf("syncConsoleErrors: createImportTable(...) failed: %v\n", err)
		return err
	}

	// Implement callback-driven-iteration-over-map via channel/goroutine
	chanIter := make(chan *errorAggRecord)
	go func() {
		for _, pageHits := range pageMap {
			chanIter <- pageHits
		}
		close(chanIter)
	}()
	log.Println("syncConsoleErrors: bulk-inserting...")
	importRows, err := BulkInsertRows(sqlDb, "syncConsoleErrors", "import_console_errors", consoleErrorsImportFields[:], func() ([]interface{}, error) {
		record, ok := <-chanIter
		if ok {
			jsonBlob, err := json.Marshal(record.categories)
			if err != nil {
				return nil, err
			}
			return []interface{}{
				[]byte(record.pageOid),
				record.total,
				string(jsonBlob),
				record.lastWhen,
			}, nil
		}
		return nil, nil
	})

	log.Println("syncConsoleErrors: copy-inserting from temp table...")
	result, err := sqlDb.Exec(`
INSERT INTO console_errors (
		page_id,
		total, categories, last_when)
	SELECT 
		p.id, it.total,
		to_jsonb(it.categories::json),
		it.last_when
	FROM import_console_errors AS it
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
	log.Printf("syncConsoleErrors: inserted %d (out of %d) import rows\n", insertRows, importRows)

	return nil
}
