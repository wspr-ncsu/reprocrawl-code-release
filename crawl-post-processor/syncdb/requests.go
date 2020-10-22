package syncdb

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"time"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// ------------------------------------------------------------------------------
// Sync requestWillBeSent events
// ------------------------------------------------------------------------------

// syncRequestInitInputRecord identifies/holds the skeleton of information extracted from a Mongo `events` record with event=requestWillBeSent
type syncRequestInitInputRecord struct {
	MongoID       bson.ObjectId `bson:"_id"`
	PageID        bson.ObjectId `bson:"page"`
	FrameID       string        `bson:"frameId"`
	LoggedWhen    time.Time     `bson:"date"`
	ResourceType  string        `bson:"resourceType"`
	InitiatorType string        `bson:"initiatorType"`
	HTTPMethod    string        `bson:"httpMethod"`
	DocumentURL   string        `bson:"documentUrl"`
	RequestURL    string        `bson:"url"`
}

// requestInitsImportFields holds the in-order list of field names used for bulk-inserting crawl records into our temp `request_inits_import_schema` clone
var requestInitsImportFields = [...]string{
	"mongo_oid",
	"page_mongo_oid",
	"frame_token",
	"document_url_sha256",
	"http_method",
	"request_url_sha256",
	"resource_type",
	"initiator_type",
	"logged_when",
}

// getSyncRequestInitIter looks up the latest imported request_inits in <sqlDb> and generates an iterator over newer request_inits in <db>
func getSyncRequestInitIter(db *mgo.Database, sqlDb *sql.DB) (*mgo.Iter, error) {
	sourceMatch := bson.M{
		"event": "requestWillBeSent",
	}

	// optionally add date-range filtering on `date`
	dateRange, err := getBeforeAfterFilter()
	if err != nil {
		return nil, err
	} else if len(dateRange) > 0 {
		sourceMatch["date"] = dateRange
	}

	// Build a projection map for just the fields we need for deserialization of our record types
	sourceProject, err := BuildProjection(reflect.TypeOf(syncRequestInitInputRecord{}))
	if err != nil {
		return nil, err
	}

	// Query and return the records of interest
	return db.C("events").Find(sourceMatch).Select(sourceProject).Iter(), nil
}

func syncRequestInits(db *mgo.Database, sqlDb *sql.DB) error {
	log.Println("syncRequestInits: getting new-request-inits iterator...")
	iter, err := getSyncRequestInitIter(db, sqlDb)
	if err != nil {
		return err
	}
	defer func() {
		log.Println("syncRequestInits: closing new-request-inits iterator...")
		iter.Close()
	}()

	log.Println("syncRequestInits: creating temp table 'import_request_inits'...")
	err = CreateImportTable(sqlDb, "request_inits_import_schema", "import_request_inits")
	if err != nil {
		log.Printf("syncRequestInits: createImportTable(...) failed: %v\n", err)
		return err
	}

	ub := NewURLBakery()

	log.Println("syncRequestInits: bulk-inserting...")
	importRows, err := BulkInsertRows(sqlDb, "syncRequestInits", "import_request_inits", requestInitsImportFields[:], func() ([]interface{}, error) {
		var record syncRequestInitInputRecord
		if iter.Next(&record) {
			docURLHash := ub.URLToHash(record.DocumentURL)
			reqURLHash := ub.URLToHash(record.RequestURL)
			values := []interface{}{
				[]byte(record.MongoID),
				[]byte(record.PageID),
				record.FrameID,
				docURLHash[:],
				record.HTTPMethod,
				reqURLHash[:],
				record.ResourceType,
				record.InitiatorType,
				record.LoggedWhen,
			}
			return values, nil
		}
		log.Printf("syncRequestInits: closing iterator and committing transation...\n")
		err := iter.Close()
		if err != nil {
			return nil, err // signal error/abort
		}
		return nil, nil // signal end-of-stream
	})
	if err != nil {
		return err
	}

	log.Printf("syncRequestInits: inserting cooked URLs referenced by inserted requests...")
	err = ub.InsertBakedURLs(sqlDb)
	if err != nil {
		return err
	}

	log.Println("syncRequestInits: copy-inserting from temp table...")
	result, err := sqlDb.Exec(`
INSERT INTO request_inits (
		mongo_oid, page_id, frame_id,
		document_url_id, http_method, request_url_id,
		resource_type, initiator_type, logged_when)
	SELECT 
		it.mongo_oid, p.id, f.id,
		uDoc.id, it.http_method, uReq.id,
		it.resource_type, it.initiator_type, it.logged_when
	FROM import_request_inits AS it
		LEFT JOIN pages AS p
			ON (p.mongo_oid = it.page_mongo_oid)
		LEFT JOIN frames AS f
			ON (f.token = it.frame_token)
		INNER JOIN urls AS uDoc
			ON (uDoc.sha256 = it.document_url_sha256)
		INNER JOIN urls AS uReq
			ON (uReq.sha256 = it.request_url_sha256)
ON CONFLICT DO NOTHING;
`)
	if err != nil {
		return err
	}
	insertRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	log.Printf("syncRequestInits: inserted %d (out of %d) import rows\n", insertRows, importRows)

	return nil
}

// ------------------------------------------------------------------------------
// Sync requestResponse events
// ------------------------------------------------------------------------------

// syncRequestResponseInputRecord identifies/holds the skeleton of information extracted from a Mongo `events` record with event=requestResponse
type syncRequestResponseInputRecord struct {
	MongoID    bson.ObjectId `bson:"_id"`
	PageID     bson.ObjectId `bson:"page"`
	LoggedWhen time.Time     `bson:"date"`
	URL        string        `bson:"url"`
	Meta       struct {
		Headers      [][]string `bson:"headers"`
		Method       string     `bson:"method"`
		ResourceType string     `bson:"resourceType"`
		Response     struct {
			Status    int        `bson:"status"`
			FromCache bool       `bson:"fromCache"`
			Headers   [][]string `bson:"headers"`
			Remote    struct {
				IP   string `bson:"ip"`
				Port int    `bson:"port"`
			} `bson:"remoteAddress"`
			Protocol        string `bson:"protocol"`
			SecurityDetails bson.M `bson:"securityDetails"`
		} `bson:"response"`
		Redirects []struct {
			URL    string `bson:"url" json:"url"`
			Status int    `bson:"status" json:"status"`
		} `bson:"redirectChain"`
	} `bson:"meta"`
	BodyBlobHash string `bson:"blobHash"`
}

// requestResponseImportFields holds the in-order list of field names used for bulk-inserting crawl records into our temp `request_responses_import_schema` clone
var requestResponseImportFields = [...]string{
	"mongo_oid",
	"page_mongo_oid",
	"resource_type",
	"request_url_sha256",
	"request_method",
	"request_headers",
	"redirect_chain",
	"response_from_cache",
	"response_status",
	"response_headers",
	"response_body_sha256",
	"server_ip",
	"server_port",
	"protocol",
	"security_details",
	"logged_when",
}

// getSyncRequestResponsesIter looks up the latest imported request_responses in <sqlDb> and generates an iterator over newer request_responses in <db>
func getSyncRequestResponsesIter(db *mgo.Database, sqlDb *sql.DB) (*mgo.Iter, error) {
	sourceMatch := bson.M{
		"event": "requestResponse",
	}

	// optionally add date-range filtering on `date`
	dateRange, err := getBeforeAfterFilter()
	if err != nil {
		return nil, err
	} else if len(dateRange) > 0 {
		sourceMatch["date"] = dateRange
	}

	// Build a projection map for just the fields we need for deserialization of our record types
	sourceProject, err := BuildProjection(reflect.TypeOf(syncRequestResponseInputRecord{}))
	if err != nil {
		return nil, err
	}

	// Query and return the records of interest
	return db.C("events").Find(sourceMatch).Select(sourceProject).Iter(), nil
}

func syncRequestResponses(db *mgo.Database, sqlDb *sql.DB) error {
	log.Println("syncRequestResponses: getting new-request-responses iterator...")
	iter, err := getSyncRequestResponsesIter(db, sqlDb)
	if err != nil {
		return err
	}
	defer func() {
		log.Println("syncRequestResponses: closing new-request-responses iterator...")
		iter.Close()
	}()

	log.Println("syncRequestResponses: creating temp table 'import_request_responses'...")
	err = CreateImportTable(sqlDb, "request_responses_import_schema", "import_request_responses")
	if err != nil {
		log.Printf("syncRequestResponses: createImportTable(...) failed: %v\n", err)
		return err
	}

	ub := NewURLBakery()

	log.Println("syncRequestResponses: bulk-inserting...")
	importRows, err := BulkInsertRows(sqlDb, "syncRequestResponses", "import_request_responses", requestResponseImportFields[:], func() ([]interface{}, error) {
		var record syncRequestResponseInputRecord
		if iter.Next(&record) {
			var requestHeaders interface{}
			if record.Meta.Headers != nil {
				headerMap := make(map[string]string)
				for _, pair := range record.Meta.Headers {
					if len(pair) != 2 {
						log.Printf("syncRequestResponses: WARNING -- request header record with %d elements (not 2)? [ignoring]\n", len(pair))
					} else {
						headerMap[pair[0]] = pair[1]
					}
				}
				requestHeadersRaw, err := json.Marshal(headerMap)
				if err != nil {
					return nil, err
				}
				requestHeaders = string(requestHeadersRaw)
			}

			var responseHeaders interface{}
			if len(record.Meta.Response.Headers) > 0 {
				headerMap := make(map[string]string)
				for _, pair := range record.Meta.Response.Headers {
					if len(pair) != 2 {
						log.Printf("syncRequestResponses: WARNING -- response header record with %d elements (not 2)? [ignoring]\n", len(pair))
					} else {
						headerMap[pair[0]] = pair[1]
					}
				}
				responseHeadersRaw, err := json.Marshal(headerMap)
				if err != nil {
					return nil, err
				}
				responseHeaders = string(responseHeadersRaw)
			}

			var securityDetails interface{}
			if len(record.Meta.Response.SecurityDetails) > 0 {
				securityDetailsRaw, err := json.Marshal(record.Meta.Response.SecurityDetails)
				if err != nil {
					return nil, err
				}
				securityDetails = string(securityDetailsRaw)
			}

			var bodyHash interface{}
			if record.BodyBlobHash != "" {
				bodyHash, err = hex.DecodeString(record.BodyBlobHash)
				if err != nil {
					return nil, err
				}
			}

			var redirectChain interface{}
			if len(record.Meta.Redirects) > 0 {
				chainRaw, err := json.Marshal(record.Meta.Redirects)
				if err != nil {
					return nil, err
				}
				redirectChain = string(chainRaw)
			}

			urlHash := ub.URLToHash(record.URL)
			values := []interface{}{
				[]byte(record.MongoID),
				[]byte(record.PageID),
				record.Meta.ResourceType,
				urlHash[:],
				record.Meta.Method,
				requestHeaders,
				redirectChain,
				record.Meta.Response.FromCache,
				record.Meta.Response.Status,
				responseHeaders,
				bodyHash,
				NullableString(record.Meta.Response.Remote.IP),
				NullableInt(record.Meta.Response.Remote.Port),
				NullableString(record.Meta.Response.Protocol),
				securityDetails,
				record.LoggedWhen,
			}
			return values, nil
		}
		log.Printf("syncRequestResponses: closing iterator and committing transation...\n")
		err := iter.Close()
		if err != nil {
			return nil, err // signal error/abort
		}
		return nil, nil // signal end-of-stream
	})
	if err != nil {
		return err
	}

	log.Printf("syncRequestResponses: inserting cooked URLs referenced by inserted responses...")
	err = ub.InsertBakedURLs(sqlDb)
	if err != nil {
		return err
	}

	log.Println("syncRequestResponses: copy-inserting from temp table...")
	result, err := sqlDb.Exec(`
INSERT INTO request_responses (
		mongo_oid, page_id, logged_when, resource_type,
		request_url_id, request_method, request_headers, redirect_chain,
		response_from_cache, response_status, response_headers, response_body_sha256,
		server_ip, server_port, protocol, security_details)
	SELECT 
		it.mongo_oid, p.id, it.logged_when, it.resource_type,
		u.id, it.request_method, to_jsonb(it.request_headers::json), to_jsonb(it.redirect_chain::json),
		it.response_from_cache, it.response_status, to_jsonb(it.response_headers::json), it.response_body_sha256,
		it.server_ip, it.server_port, it.protocol, to_jsonb(it.security_details::json)
	FROM import_request_responses AS it
		LEFT JOIN pages AS p
			ON (p.mongo_oid = it.page_mongo_oid)
		INNER JOIN urls AS u
			ON (u.sha256 = it.request_url_sha256)
ON CONFLICT DO NOTHING;
`)
	if err != nil {
		_, bkupErr := sqlDb.Exec(fmt.Sprintf("CREATE TABLE \"%s_bkup\" AS SELECT * FROM \"%s\";", "import_request_responses", "import_request_responses"))
		if bkupErr != nil {
			log.Printf("syncRequestResponses: sorry, something went wrong but I couldn't back up the import table (%v)\n", bkupErr)
		}
		return err
	}
	insertRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	log.Printf("syncRequestResponses: inserted %d (out of %d) import rows\n", insertRows, importRows)

	return nil
}

// ------------------------------------------------------------------------------
// Sync requestFailure events
// ------------------------------------------------------------------------------

// syncRequestFailureInputRecord identifies/holds the skeleton of information extracted from a Mongo `events` record with event=requestFailure
type syncRequestFailureInputRecord struct {
	MongoID    bson.ObjectId `bson:"_id"`
	PageID     bson.ObjectId `bson:"page"`
	LoggedWhen time.Time     `bson:"date"`
	URL        string        `bson:"url"`
	Meta       struct {
		Headers      [][]string `bson:"headers"`
		Method       string     `bson:"method"`
		ResourceType string     `bson:"resourceType"`
		Failure      string     `bson:"failure"`
	} `bson:"meta"`
}

// requestFailureImportFields holds the in-order list of field names used for bulk-inserting crawl records into our temp `request_failures_import_schema` clone
var requestFailureImportFields = [...]string{
	"mongo_oid",
	"page_mongo_oid",
	"resource_type",
	"request_url_sha256",
	"request_method",
	"request_headers",
	"failure",
	"logged_when",
}

// getSyncRequestFailuresIter looks up the latest imported request_failures in <sqlDb> and generates an iterator over newer request_failures in <db>
func getSyncRequestFailuresIter(db *mgo.Database, sqlDb *sql.DB) (*mgo.Iter, error) {
	sourceMatch := bson.M{
		"event": "requestFailure",
	}

	// optionally add date-range filtering on `date`
	dateRange, err := getBeforeAfterFilter()
	if err != nil {
		return nil, err
	} else if len(dateRange) > 0 {
		sourceMatch["date"] = dateRange
	}

	// Build a projection map for just the fields we need for deserialization of our record types
	sourceProject, err := BuildProjection(reflect.TypeOf(syncRequestFailureInputRecord{}))
	if err != nil {
		return nil, err
	}

	// Query and return the records of interest
	return db.C("events").Find(sourceMatch).Select(sourceProject).Iter(), nil
}

func syncRequestFailures(db *mgo.Database, sqlDb *sql.DB) error {
	log.Println("syncRequestFailures: getting new-request-failures iterator...")
	iter, err := getSyncRequestFailuresIter(db, sqlDb)
	if err != nil {
		return err
	}
	defer func() {
		log.Println("syncRequestFailures: closing new-request-failures iterator...")
		iter.Close()
	}()

	log.Println("syncRequestFailures: creating temp table 'import_request_failures'...")
	err = CreateImportTable(sqlDb, "request_failures_import_schema", "import_request_failures")
	if err != nil {
		log.Printf("syncRequestFailures: createImportTable(...) failed: %v\n", err)
		return err
	}

	ub := NewURLBakery()

	log.Println("syncRequestFailures: bulk-inserting...")
	importRows, err := BulkInsertRows(sqlDb, "syncRequestFailures", "import_request_failures", requestFailureImportFields[:], func() ([]interface{}, error) {
		var record syncRequestFailureInputRecord
		if iter.Next(&record) {
			var requestHeaders interface{}
			if record.Meta.Headers != nil {
				headerMap := make(map[string]string)
				for _, pair := range record.Meta.Headers {
					if len(pair) != 2 {
						log.Printf("syncRequestFailures: WARNING -- request header record with %d elements (not 2)? [ignoring]\n", len(pair))
					} else {
						headerMap[pair[0]] = pair[1]
					}
				}
				requestHeadersRaw, err := json.Marshal(headerMap)
				if err != nil {
					return nil, err
				}
				requestHeaders = string(requestHeadersRaw)
			}

			urlHash := ub.URLToHash(record.URL)
			values := []interface{}{
				[]byte(record.MongoID),
				[]byte(record.PageID),
				record.Meta.ResourceType,
				urlHash[:],
				record.Meta.Method,
				requestHeaders,
				record.Meta.Failure,
				record.LoggedWhen,
			}
			return values, nil
		}
		log.Printf("syncRequestFailures: closing iterator and committing transation...\n")
		err := iter.Close()
		if err != nil {
			return nil, err // signal error/abort
		}
		return nil, nil // signal end-of-stream
	})
	if err != nil {
		return err
	}

	log.Printf("syncRequestFailures: inserting cooked URLs referenced by inserted failures...")
	err = ub.InsertBakedURLs(sqlDb)
	if err != nil {
		return err
	}

	log.Println("syncRequestFailures: copy-inserting from temp table...")
	result, err := sqlDb.Exec(`
INSERT INTO request_failures (
		mongo_oid, page_id, logged_when, resource_type,
		request_url_id, request_method, request_headers, failure)
	SELECT 
		it.mongo_oid, p.id, it.logged_when, it.resource_type,
		u.id, it.request_method, to_jsonb(it.request_headers::json), it.failure
	FROM import_request_failures AS it
		LEFT JOIN pages AS p
			ON (p.mongo_oid = it.page_mongo_oid)
		INNER JOIN urls AS u
			ON (u.sha256 = it.request_url_sha256)
ON CONFLICT DO NOTHING;
`)
	if err != nil {
		_, bkupErr := sqlDb.Exec(fmt.Sprintf("CREATE TABLE \"%s_bkup\" AS SELECT * FROM \"%s\";", "import_request_failures", "import_request_failures"))
		if bkupErr != nil {
			log.Printf("syncRequestFailures: sorry, something went wrong but I couldn't back up the import table (%v)\n", bkupErr)
		}
		return err
	}
	insertRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	log.Printf("syncRequestFailures: inserted %d (out of %d) import rows\n", insertRows, importRows)

	sqlDb.Exec(`create table foo as select * from import_request_failures;`)

	return nil
}
