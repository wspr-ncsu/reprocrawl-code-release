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
// Sync pages
// ------------------------------------------------------------------------------

// syncPageInputRecord identifies/holds the skeleton of information extracted from a Mongo page record
type syncPageInputRecord struct {
	MongoID bson.ObjectId `bson:"_id"`
	Context struct {
		Position string `bson:"position"`
	} `bson:"context"`
	Visit struct {
		URL string `bson:"url"`
	} `bson:"visit"`
	Status struct {
		State   string    `bson:"state"`
		Ended   time.Time `bson:"lastWhen"` // always missing/0; turns out we didn't properly implement "lastWhen" for pages :-(
		Created struct {
			When time.Time `bson:"when"`
		} `bson:"created"`
		PreVisitCompleted struct {
			When time.Time `bson:"when"`
		} `bson:"preVisitCompleted"`
		NavigationCompleted struct {
			When time.Time `bson:"when"`
		} `bson:"navigationCompleted"`
		GremlinInteractionStarted struct {
			When time.Time `bson:"when"`
		} `bson:"gremlinInteractionStarted"`
		Aborted struct {
			Info struct {
				Msg string `bson:"msg"`
			} `bson:"info"`
		} `bson:"aborted"`
	} `bson:"status"`
	// nullable field (0 value/nil means no such value)
	FrameNavTime         int `bson:"mainFrameNavigationTime"`
	PageLoadTime         int `bson:"pageLoadTime"`
	MainFrameContentBlob struct {
		Size int `bson:"orig_size"`
	} `bson:"mainFrameContentBlob"`
	MainFrameContentHash []byte `bson:"mainFrameContentHash"`
	PageScreenshotBlob   struct {
		Size int `bson:"orig_size"`
	} `bson:"pageScreenshotBlob"`
	PageScreenshotHash []byte `bson:"pageScreenshotHash"`
}

// pagesImportFields holds the in-order list of field names used for bulk-inserting crawl records into our temp `pages_import_schema` clone
var pagesImportFields = [...]string{
	"mongo_oid",
	"position",
	"visit_url_sha256",
	"nav_time_ms",
	"fetch_time_ms",
	"load_time_ms",
	"final_content_sha256",
	"final_content_size",
	"screenshot_sha256",
	"screenshot_size",
	"status_state",
	"status_abort_msg",
	"status_created",
	"status_browser_ready",
	"status_nav_complete",
	"status_gremlins_launched",
	"status_ended",
}

// getSyncPageIter looks up the latest imported pages in <sqlDb> and generates an iterator over newer pages in <db>
func getSyncPageIter(db *mgo.Database, sqlDb *sql.DB) (*mgo.Iter, error) {
	sourceMatch := bson.M{
		"status.state": bson.M{"$in": []string{"aborted", "completed"}},
	}

	// optionally add date-range filtering on status.created.when
	dateRange, err := getBeforeAfterFilter()
	if err != nil {
		return nil, err
	} else if len(dateRange) > 0 {
		sourceMatch["status.created.when"] = dateRange
	}

	// Build a projection map for just the fields we need for deserialization of our record types
	sourceProject, err := BuildProjection(reflect.TypeOf(syncPageInputRecord{}))
	if err != nil {
		return nil, err
	}

	// Build a big honking aggregation pipeline to include blob lookups for DOM/screenshot
	bigHonkingQuery := []bson.M{
		{"$match": sourceMatch},
		{"$lookup": bson.M{"from": "blobs", "localField": "mainFrameContentBlob", "foreignField": "_id", "as": "mainFrameContentBlob"}},
		{"$lookup": bson.M{"from": "blobs", "localField": "pageScreenshotBlob", "foreignField": "_id", "as": "pageScreenshotBlob"}},
		{"$unwind": bson.M{"path": "$mainFrameContentBlob", "preserveNullAndEmptyArrays": true}},
		{"$unwind": bson.M{"path": "$pageScreenshotBlob", "preserveNullAndEmptyArrays": true}},
		{"$project": sourceProject},
	}
	return db.C("pages").Pipe(bigHonkingQuery).Iter(), nil
}

func syncPages(db *mgo.Database, sqlDb *sql.DB) error {
	log.Println("syncPages: getting new-pages iterator...")
	iter, err := getSyncPageIter(db, sqlDb)
	if err != nil {
		return err
	}
	defer func() {
		log.Println("syncPages: closing new-pages iterator...")
		iter.Close()
	}()

	log.Println("syncPages: creating temp table 'import_pages'...")
	err = CreateImportTable(sqlDb, "pages_import_schema", "import_pages")
	if err != nil {
		log.Printf("syncPages: createImportTable(...) failed: %v\n", err)
		return err
	}

	ub := NewURLBakery()

	log.Println("syncPages: bulk-inserting...")
	importRows, err := BulkInsertRows(sqlDb, "syncPages", "import_pages", pagesImportFields[:], func() ([]interface{}, error) {
		var record syncPageInputRecord
		if iter.Next(&record) {
			// Nullable types (0-values or empty slices mean not-present)
			var navTime, fetchTime, loadTime, mainFrameHash, mainFrameSize, screenshotHash, screenshotSize interface{}
			if record.FrameNavTime > 0 {
				navTime = record.FrameNavTime
			}
			if !record.Status.NavigationCompleted.When.IsZero() && !record.Status.PreVisitCompleted.When.IsZero() {
				fetchTime = int(record.Status.NavigationCompleted.When.Sub(record.Status.PreVisitCompleted.When).Seconds() * 1000)
			}
			if record.PageLoadTime > 0 {
				loadTime = record.PageLoadTime
			}
			if len(record.MainFrameContentHash) != 0 {
				mainFrameHash = record.MainFrameContentHash
			}
			if record.MainFrameContentBlob.Size > 0 {
				mainFrameSize = record.MainFrameContentBlob.Size
			}
			if len(record.PageScreenshotHash) != 0 {
				screenshotHash = record.PageScreenshotHash
			}
			if record.PageScreenshotBlob.Size > 0 {
				screenshotSize = record.PageScreenshotBlob.Size
			}
			urlHash := ub.URLToHash(record.Visit.URL)
			values := []interface{}{
				[]byte(record.MongoID),
				record.Context.Position,
				urlHash[:],
				navTime,
				fetchTime,
				loadTime,
				mainFrameHash,
				mainFrameSize,
				screenshotHash,
				screenshotSize,
				record.Status.State,
				NullableString(record.Status.Aborted.Info.Msg),
				record.Status.Created.When,
				NullableTimestamp(record.Status.PreVisitCompleted.When),
				NullableTimestamp(record.Status.NavigationCompleted.When),
				NullableTimestamp(record.Status.GremlinInteractionStarted.When),
				record.Status.Ended,
			}
			return values, nil
		}
		log.Printf("syncPages: closing iterator and committing transation...\n")
		err := iter.Close()
		if err != nil {
			return nil, err // signal error/abort
		}
		return nil, nil // signal end-of-stream
	})
	if err != nil {
		return err
	}

	log.Printf("syncPages: inserting cooked URLs referenced by inserted pages...")
	err = ub.InsertBakedURLs(sqlDb)
	if err != nil {
		return err
	}

	log.Println("syncPages: copy-inserting from temp table...")
	result, err := sqlDb.Exec(`
INSERT INTO pages (
		mongo_oid, "position",
		visit_url_id, nav_time_ms, fetch_time_ms, load_time_ms,
		final_content_sha256, final_content_size,
		screenshot_sha256, screenshot_size,
		status_state, status_abort_msg, status_created, status_ended)
	SELECT ip.mongo_oid, ip.position,
		u.id, ip.nav_time_ms, ip.fetch_time_ms, ip.load_time_ms,
		ip.final_content_sha256, ip.final_content_size,
		ip.screenshot_sha256, ip.screenshot_size,
		ip.status_state, ip.status_abort_msg, ip.status_created, ip.status_ended
	FROM import_pages AS ip
		INNER JOIN urls AS u
			ON (u.sha256 = ip.visit_url_sha256)
ON CONFLICT DO NOTHING;
`)
	if err != nil {
		return err
	}
	insertRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	log.Printf("syncPages: inserted %d (out of %d) import rows\n", insertRows, importRows)

	return nil
}
