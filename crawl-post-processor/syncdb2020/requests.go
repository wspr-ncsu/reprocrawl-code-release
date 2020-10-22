package syncdb2020

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"time"
	"vpp/syncdb"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type requestEventCluster struct {
	RequestID string         `bson:"_id"`
	Events    []requestEvent `bson:"events"`
}

type redirectLink struct {
	URL    string `bson:"url" json:"url"`
	Status int    `bson:"status" json:"status"`
}

type requestEvent struct {
	MongoID   bson.ObjectId `bson:"_id"`
	URL       string        `bson:"url"`
	RequestID string        `bson:"requestId"`
	Event     string        `bson:"event"`
	When      time.Time     `bson:"date"`
	Request   struct {
		IsNavigation bool       `bson:"navigationRequest"`
		ResourceType string     `bson:"resourceType"`
		Method       string     `bson:"method"`
		Headers      [][]string `bson:"headers"`
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
		Redirects []redirectLink `bson:"redirectChain"`
		Failure   string         `bson:"failure"`
	} `bson:"meta"`
	BodyBlobOid  bson.ObjectId `bson:"blobOid"`
	BodyBlobHash string        `bson:"blobHash"`

	ResourceType  string `bson:"resourceType"`
	DocumentURL   string `bson:"documentUrl"`
	Method        string `bson:"httpMethod"`
	InitiatorType string `bson:"initiatorType"`
	Initiator     struct {
		URL  string `bson:"url"`
		Line int    `bson:"line"`
	} `bson:"initiator"`
	FrameID  string `bson:"frameId"`
	LoaderID string `bson:"loaderId"`
}

type requestEventByWhen []requestEvent

func (a requestEventByWhen) Len() int           { return len(a) }
func (a requestEventByWhen) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a requestEventByWhen) Less(i, j int) bool { return a[i].When.Before(a[j].When) }

type requestSummary struct {
	RequestID         string
	PageOid           bson.ObjectId
	FrameID           string
	LoaderID          string
	ResourceType      string
	InitiatorType     string
	InitiatorURL      string
	InitiatorLine     int
	InitialRequestURL string
	DocumentURL       string
	WhenQueued        time.Time

	SentRequest     bool
	IsNavigation    bool
	RequestMethod   string
	RequestHeaders  [][]string
	FinalRequestURL string
	RedirectChain   []redirectLink

	GotResponse      bool
	FromCache        bool
	ResponseFailure  string // If we didn't get a response, this string indicates failure mode
	ResponseStatus   int
	ResponseHeaders  [][]string
	ResponseBodyHash string
	ResponseBodySize int
	ServerIP         string
	ServerPort       int
	Protocol         string
	SecurityDetails  bson.M
	WhenDone         time.Time
}

func getBlobSize(db *mgo.Database, blobOid bson.ObjectId) (int, error) {
	var doc struct {
		OriginalSize int `bson:"orig_size"`
	}
	err := db.C("blobs").FindId(blobOid).Select(bson.M{"orig_size": 1}).One(&doc)
	if err != nil {
		return -1, err
	}
	return doc.OriginalSize, nil
}

func getRequestSummaries(db *mgo.Database, pageOid bson.ObjectId) ([]requestSummary, error) {
	bigHonkingQuery := []bson.M{
		{"$match": bson.M{"page": pageOid}},
		{"$group": bson.M{
			"_id":    "$requestId",
			"events": bson.M{"$push": "$$CURRENT"},
			"start":  bson.M{"$min": "$date"},
		}},
		{"$sort": bson.M{"start": 1}},
	}
	iter := db.C("request_events").Pipe(bigHonkingQuery).AllowDiskUse().Iter()

	summaries := make([]requestSummary, 0, 32)
	var cluster requestEventCluster
	for iter.Next(&cluster) {
		sort.Sort(requestEventByWhen(cluster.Events))
		var firstWillBeSent, lastResponseOrFailure *requestEvent
		for i, event := range cluster.Events {
			if firstWillBeSent == nil && event.Event == "requestWillBeSent" {
				firstWillBeSent = &cluster.Events[i]
			} else if event.Event == "requestResponse" || event.Event == "requestFailure" {
				lastResponseOrFailure = &cluster.Events[i]
			}
		}

		summary := requestSummary{
			RequestID: cluster.RequestID,
			PageOid:   pageOid,
		}

		if firstWillBeSent != nil {
			summary.FrameID = firstWillBeSent.FrameID
			summary.LoaderID = firstWillBeSent.LoaderID
			summary.ResourceType = firstWillBeSent.ResourceType
			summary.InitiatorType = firstWillBeSent.InitiatorType
			summary.InitiatorURL = firstWillBeSent.Initiator.URL
			summary.InitiatorLine = firstWillBeSent.Initiator.Line
			summary.InitialRequestURL = firstWillBeSent.URL
			summary.DocumentURL = firstWillBeSent.DocumentURL
			summary.WhenQueued = firstWillBeSent.When
		}

		if lastResponseOrFailure != nil {
			summary.SentRequest = true
			summary.IsNavigation = lastResponseOrFailure.Request.IsNavigation
			summary.RequestMethod = lastResponseOrFailure.Request.Method
			summary.FinalRequestURL = lastResponseOrFailure.URL
			summary.RequestHeaders = lastResponseOrFailure.Request.Headers
			summary.GotResponse = (lastResponseOrFailure.Event == "requestResponse")
			summary.WhenDone = lastResponseOrFailure.When
			if summary.GotResponse {
				summary.FromCache = lastResponseOrFailure.Request.Response.FromCache
				summary.ResponseHeaders = lastResponseOrFailure.Request.Response.Headers
				summary.ResponseStatus = lastResponseOrFailure.Request.Response.Status
				summary.ResponseBodyHash = lastResponseOrFailure.BodyBlobHash
				if originalSize, err := getBlobSize(db, lastResponseOrFailure.BodyBlobOid); err != nil {
					log.Printf("syncdb2020/getRequestSummaries: error looking up size of blob %s (%v)\n", lastResponseOrFailure.BodyBlobOid.String(), err)
					summary.ResponseBodySize = -1
				} else {
					summary.ResponseBodySize = originalSize
				}
				summary.RedirectChain = lastResponseOrFailure.Request.Redirects
				summary.ServerIP = lastResponseOrFailure.Request.Response.Remote.IP
				summary.ServerPort = lastResponseOrFailure.Request.Response.Remote.Port
				summary.Protocol = lastResponseOrFailure.Request.Response.Protocol
				summary.SecurityDetails = lastResponseOrFailure.Request.Response.SecurityDetails
			} else {
				summary.ResponseFailure = lastResponseOrFailure.Request.Failure
			}
		}

		summaries = append(summaries, summary)
	}
	return summaries, nil
}

var requestImportFields = [...]string{
	"unique_id",
	"page_oid",
	"frame_id",
	"loader_id",
	"resource_type",
	"initiator_type",
	"initiator_url_sha256",
	"initiator_line",
	"first_request_url_sha256",
	"redirect_chain",
	"final_request_url_sha256",
	"http_method",
	"request_headers",
	"response_received",
	"response_failure",
	"response_from_cache",
	"response_status",
	"response_headers",
	"response_body_sha256",
	"response_body_size",
	"server_ip",
	"server_port",
	"protocol",
	"security_details",
	"when_queued",
	"when_replied",
}

func insertRequestSummaries(sqlDb *sql.DB, pageOid bson.ObjectId, summaries []requestSummary, flm frameLookupMap) error {
	log.Println("syncdb2020/insertRequestSummaries: creating temp table 'import_request_summaries'...")
	err := syncdb.CreateImportTable(sqlDb, "requests_import_schema", "import_request_summaries")
	if err != nil {
		return fmt.Errorf("syncdb2020/insertRequestSummaries: createImportTable(...) failed: %w", err)
	}

	sumChan := make(chan *requestSummary)
	go func() {
		for i := range summaries {
			sumChan <- &summaries[i]
		}
		close(sumChan)
	}()

	log.Println("syncdb2020/insertRequestSummaries: bulk-inserting...")
	ub := syncdb.NewURLBakery()
	tempRows, err := syncdb.BulkInsertRows(sqlDb, "syncdb2020/insertRequestSummaries", "import_request_summaries", requestImportFields[:], func() ([]interface{}, error) {
		sum, ok := <-sumChan
		if !ok {
			return nil, nil // end-of-stream
		}

		var uid string
		if sum.LoaderID == sum.RequestID {
			uid = fmt.Sprintf("%s/", sum.LoaderID)
		} else {
			uid = fmt.Sprintf("%s/%s", sum.LoaderID, sum.RequestID)
		}
		flr, err := flm.Lookup(sum.LoaderID, sum.FrameID)
		if err != nil {
			log.Printf("syncdb2020/insertRequestSummaries: unknown frame=%s/loader=%s for request=%s; no frame FK possible\n", sum.FrameID, sum.LoaderID, uid)
			flr = &frameLoaderRecord{
				FrameID:  sum.FrameID,
				LoaderID: sum.LoaderID,
			}
		}

		var initURLHash interface{}
		if sum.InitiatorURL != "" {
			temp := ub.URLToHash(sum.InitiatorURL)
			initURLHash = temp[:]
		}
		var firstRequestURLHash interface{}
		if sum.InitialRequestURL != "" {
			temp := ub.URLToHash(sum.InitialRequestURL)
			firstRequestURLHash = temp[:]
		}
		var finalRequestURLHash interface{}
		if sum.FinalRequestURL != sum.InitialRequestURL {
			temp := ub.URLToHash(sum.FinalRequestURL)
			finalRequestURLHash = temp[:]
		} else {
			finalRequestURLHash = firstRequestURLHash
		}

		var requestHeaders interface{}
		if sum.RequestHeaders != nil {
			headerMap := make(map[string]string)
			for _, pair := range sum.RequestHeaders {
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
		if len(sum.ResponseHeaders) > 0 {
			headerMap := make(map[string]string)
			for _, pair := range sum.ResponseHeaders {
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
		if len(sum.SecurityDetails) > 0 {
			securityDetailsRaw, err := json.Marshal(sum.SecurityDetails)
			if err != nil {
				return nil, err
			}
			securityDetails = string(securityDetailsRaw)
		}

		var bodyHash interface{}
		if sum.ResponseBodyHash != "" {
			bodyHash, err = hex.DecodeString(sum.ResponseBodyHash)
			if err != nil {
				return nil, err
			}
		}

		var bodySize interface{}
		if sum.ResponseBodySize >= 0 {
			bodySize = sum.ResponseBodySize
		}

		var redirectChain interface{}
		if len(sum.RedirectChain) > 0 {
			chainRaw, err := json.Marshal(sum.RedirectChain)
			if err != nil {
				return nil, err
			}
			redirectChain = string(chainRaw)
		}

		values := []interface{}{
			uid,
			[]byte(pageOid),
			flr.FrameID,
			flr.LoaderID,
			sum.ResourceType,
			sum.InitiatorType,
			initURLHash,
			syncdb.NullableInt(sum.InitiatorLine),
			firstRequestURLHash,
			redirectChain,
			finalRequestURLHash,
			sum.RequestMethod,
			requestHeaders,
			sum.GotResponse,
			syncdb.NullableString(sum.ResponseFailure),
			sum.FromCache,
			syncdb.NullableInt(sum.ResponseStatus),
			responseHeaders,
			bodyHash,
			bodySize,
			syncdb.NullableString(sum.ServerIP),
			syncdb.NullableInt(sum.ServerPort),
			syncdb.NullableString(sum.Protocol),
			securityDetails,
			sum.WhenQueued,
			syncdb.NullableTimestamp(sum.WhenDone),
		}
		return values, nil
	})
	if err != nil {
		return fmt.Errorf("syncdb2020/insertRequestSummaries: bulk insert into import-table failed (%w)", err)
	}

	if err := ub.InsertBakedURLs(sqlDb); err != nil {
		return fmt.Errorf("syncdb2020/insertRequestSummaries: failed to insert baked URLs (%w)", err)
	}

	log.Println("syncdb2020/insertRequestSummaries: copy-upserting from temp table...")
	result, err := sqlDb.Exec(`
INSERT INTO requests (
	page_id, frame_loader_id, unique_id, resource_type,
	initiator_type, initiator_url_id, initiator_line, first_request_url_id,
	redirect_chain, final_request_url_id, http_method, request_headers,
	response_received, response_failure, response_from_cache, response_status,
	response_headers, response_body_sha256, response_body_size,
	server_ip, server_port, protocol, security_details,
	when_queued, when_replied)
SELECT
	p.id, fl.id, it.unique_id, it.resource_type,
	it.initiator_type, iu.id, it.initiator_line, ru.id,
	to_jsonb(it.redirect_chain::json), rru.id, it.http_method, to_jsonb(it.request_headers::json),
	it.response_received, it.response_failure, it.response_from_cache, it.response_status,
	to_jsonb(it.response_headers::json), it.response_body_sha256, it.response_body_size,
	it.server_ip, it.server_port, it.protocol, to_jsonb(it.security_details::json),
	it.when_queued, it.when_replied
FROM import_request_summaries AS it
	INNER JOIN pages AS p ON (p.mongo_oid = it.page_oid)
	INNER JOIN urls AS ru ON (ru.sha256 = it.first_request_url_sha256)
	LEFT JOIN frame_loaders AS fl ON ((fl.frame_id = it.frame_id) AND (fl.loader_id = it.loader_id))
	LEFT JOIN urls AS iu ON (iu.sha256 = it.initiator_url_sha256)
	LEFT JOIN urls AS rru ON (rru.sha256 = it.final_request_url_sha256)
`)
	if err != nil {
		return fmt.Errorf("syncdb2020/insertRequestSummaries: copy-upsert failed (%w)", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("syncdb2020/insertRequestSummaries: RowsAffected() failed (%w)", err)
	}
	log.Printf("syncdb2020/insertRequestSummaries: upserted %d of %d rows", rows, tempRows)

	return nil
}
