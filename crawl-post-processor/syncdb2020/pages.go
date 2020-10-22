package syncdb2020

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"time"
	"vpp/syncdb"

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
		AlexaRank     int    `bson:"alexaRank"`
		RootDomain    string `bson:"rootDomain"`
		VantagePoint  string `bson:"vantagePoint"`
		BrowserConfig string `bson:"browserConfig"`
		Rep           int    `bson:"rep"`
	} `bson:"context"`
	Visit struct {
		URL  string `bson:"url"`
		Sync struct {
			WaitTime float64 `bson:"wait_time"`
		} `bson:"sync"`
	} `bson:"visit"`
	Status struct {
		State   string    `bson:"state"`
		Ended   time.Time `bson:"lastWhen"`
		Created struct {
			When time.Time `bson:"when"`
		} `bson:"created"`
		PreVisitCompleted struct {
			When time.Time `bson:"when"`
		} `bson:"preVisitCompleted"`
		NavigationCompleted struct {
			When time.Time `bson:"when"`
		} `bson:"navigationCompleted"`
		LoiterStarted struct {
			When time.Time `bson:"when"`
		} `bson:"loiterStarted"`
		Aborted struct {
			Info struct {
				Msg string `bson:"msg"`
			} `bson:"info"`
		} `bson:"aborted"`
		PostVisitStarted struct {
			When time.Time `bson:"when"`
		} `bson:"postVisitStarted"`
		Completed struct {
			When time.Time `bson:"when"`
			Info struct {
				Logs    int    `bson:"logs"`
				Proc    string `bson:"proc"`
				ProcErr string `bson:"procErr"`
			} `bson:"info"`
		} `bson:"completed"`
	} `bson:"status"`
	// nullable field (0 value/nil means no such value)
	FrameNavTime         int `bson:"mainFrameNavigationTime"`
	PageLoadTime         int `bson:"pageLoadTime"`
	MainFrameContentBlob struct {
		Size int `bson:"orig_size"`
	} `bson:"mainFrameContentBlob"`
	MainFrameContentHash string `bson:"mainFrameContentHash"`
	PageScreenshotBlob   struct {
		Size int `bson:"orig_size"`
	} `bson:"pageScreenshotBlob"`
	PageScreenshotHash string `bson:"pageScreenshotHash"`

	// Private copy of _whole_ page BSON record as-is from Mongo
	originalRecord bson.M
}

// getSyncPage looks up a single Mongo page record by OID
func getSyncPage(db *mgo.Database, oid bson.ObjectId) (*syncPageInputRecord, error) {
	// Build a big honking aggregation pipeline to include blob lookups for DOM/screenshot
	bigHonkingQuery := []bson.M{
		{"$match": bson.M{"_id": oid}},
		{"$lookup": bson.M{"from": "blobs", "localField": "mainFrameContentBlob", "foreignField": "_id", "as": "mainFrameContentBlob"}},
		{"$lookup": bson.M{"from": "blobs", "localField": "pageScreenshotBlob", "foreignField": "_id", "as": "pageScreenshotBlob"}},
		{"$unwind": bson.M{"path": "$mainFrameContentBlob", "preserveNullAndEmptyArrays": true}},
		{"$unwind": bson.M{"path": "$pageScreenshotBlob", "preserveNullAndEmptyArrays": true}},
	}

	originalRecord := make(bson.M)
	if err := db.C("pages").Pipe(bigHonkingQuery).One(&originalRecord); err != nil {
		return nil, fmt.Errorf("syncdb2020/getSyncPage: failed to query for page record _id=%v (%w)", oid, err)
	}
	andAgain, err := bson.Marshal(originalRecord)
	if err != nil {
		return nil, fmt.Errorf("syncdb2020/getSyncPage: error round-tripping the BSON?! (%w)", err)
	}
	record := new(syncPageInputRecord)
	if err := bson.Unmarshal(andAgain, record); err != nil {
		return nil, fmt.Errorf("syncdb2020/getSyncPage: error round-tripping the BSON?! (%w)", err)
	}
	record.originalRecord = originalRecord
	return record, nil
}

func insertPageRecord(sqlDb *sql.DB, page *syncPageInputRecord) (int, error) {
	ub := syncdb.NewURLBakery()
	visitURLHash := ub.URLToHash(page.Visit.URL)
	// TODO final URL hash by finding the initial document request
	if err := ub.InsertBakedURLs(sqlDb); err != nil {
		return -1, fmt.Errorf("syncdb2020/insertPageRecord: failed to insert baked URLs (%w)", err)
	}

	// Nullable types (0-values or empty slices mean not-present)
	var syncTime, navTime, fetchTime, loadTime, finalDomBlobHash, finalDomBlobSize, screenshotBlobHash, screenshotBlobSize interface{}
	if page.Visit.Sync.WaitTime > 0 {
		syncTime = int(math.Round(page.Visit.Sync.WaitTime * 1000))
	}
	if page.FrameNavTime > 0 {
		navTime = page.FrameNavTime
	}
	if !page.Status.NavigationCompleted.When.IsZero() && !page.Status.PreVisitCompleted.When.IsZero() {
		fetchTime = int(page.Status.NavigationCompleted.When.Sub(page.Status.PreVisitCompleted.When).Seconds() * 1000)
	}
	if page.PageLoadTime > 0 {
		loadTime = page.PageLoadTime
	}
	if page.MainFrameContentHash != "" {
		raw, err := hex.DecodeString(page.MainFrameContentHash)
		if err != nil {
			return -1, fmt.Errorf("syncdb2020/insertPageRecord: failed to decode content hash (%w)", err)
		}
		finalDomBlobHash = raw
		finalDomBlobSize = page.MainFrameContentBlob.Size
	}
	if page.PageScreenshotHash != "" {
		raw, err := hex.DecodeString(page.PageScreenshotHash)
		if err != nil {
			return -1, fmt.Errorf("syncdb2020/insertPageRecord: failed to decode content hash (%w)", err)
		}
		screenshotBlobHash = raw
		screenshotBlobSize = page.PageScreenshotBlob.Size
	}

	originalRecordRaw, err := json.Marshal(page.originalRecord)
	if err != nil {
		return -1, fmt.Errorf("syncdb2020/insertPageRecord: failed to JSON marshal original record (%w)", err)
	}
	originalRecord := string(originalRecordRaw)

	row := sqlDb.QueryRow(`
WITH inputs(
		mongo_oid, domain, alexa_rank, vantage_point, browser_config, rep,
		visit_url_sha256, sync_time_ms, nav_time_ms, fetch_time_ms, load_time_ms,
		final_content_sha256, final_content_size, screenshot_sha256, screenshot_size,
		status_state, status_abort_msg, status_created, status_ended,
		original_record
	) AS (VALUES (
		$1::bytea, $2, $3::int, $4, $5, $6::int,
		$7::bytea, $8::int, $9::int, $10::int, $11::int,
		$12::bytea, $13::int, $14::bytea, $15::int,
		$16, $17, $18::timestamptz, $19::timestamptz,
		to_jsonb($20::json)
	))
INSERT INTO pages (
	mongo_oid, domain, alexa_rank, vantage_point, browser_config, rep,
	visit_url_id, sync_time_ms, nav_time_ms, fetch_time_ms, load_time_ms,
	final_content_sha256, final_content_size, screenshot_sha256, screenshot_size,
	status_state, status_abort_msg, status_created, status_ended,
	original_record)
SELECT
	i.mongo_oid, i.domain, i.alexa_rank, i.vantage_point, i.browser_config, i.rep,
	vu.id, i.sync_time_ms, i.nav_time_ms, i.fetch_time_ms, i.load_time_ms,
	i.final_content_sha256, i.final_content_size, i.screenshot_sha256, i.screenshot_size,
	i.status_state, i.status_abort_msg, i.status_created, i.status_ended,
	i.original_record
FROM inputs AS i
	INNER JOIN urls AS vu ON (vu.sha256 = i.visit_url_sha256)
RETURNING id;`,
		[]byte(page.MongoID), page.Context.RootDomain, page.Context.AlexaRank, page.Context.VantagePoint, page.Context.BrowserConfig, page.Context.Rep,
		visitURLHash[:], syncTime, navTime, fetchTime, loadTime,
		finalDomBlobHash, finalDomBlobSize, screenshotBlobHash, screenshotBlobSize,
		page.Status.State, page.Status.Aborted.Info.Msg, page.Status.Created.When, page.Status.Ended,
		originalRecord,
	)
	var pid int
	if err := row.Scan(&pid); err != nil {
		return -1, fmt.Errorf("syncdb2020/insertPageRecord: failed get inserted page ID (%w)", err)
	}
	return pid, nil
}
