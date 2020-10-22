package syncdb2020

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sort"
	"time"
	"vpp/syncdb"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// ------------------------------------------------------------------------------
// Sync frames
// ------------------------------------------------------------------------------

type callFrame struct {
	ScriptID string `bson:"scriptId"`
	URL      string `bson:"url"`
	Line     int    `bson:"lineNumber"`
	Column   int    `bson:"columnNumber"`
}

type frameEvent struct {
	Type           string `bson:"type"`
	URL            string `bson:"url"`
	LoaderID       string `bson:"loaderId"`
	SecurityOrigin string `bson:"securityOrigin"`
	Stack          struct {
		CallFrames []callFrame `bson:"callFrames"`
	} `bson:"stack"`
	When time.Time `bson:"when"`
}

// syncFrameInputRecord identifies/holds the skeleton of information extracted from a Mongo `frames` record
type syncFrameInputRecord struct {
	MongoID       bson.ObjectId `bson:"_id"`
	PageID        bson.ObjectId `bson:"page"`
	FrameID       string        `bson:"frameId"`
	ParentFrameID string        `bson:"parentFrameId"`
	MainFrame     bool          `bson:"mainFrame"`
	FrameEvents   []frameEvent  `bson:"frameEvents"`
}

// getSyncFrameIter looks up all frames associated with a given page OID
func getSyncFrameIter(db *mgo.Database, pageOid bson.ObjectId) (*mgo.Iter, error) {
	sourceMatch := bson.M{
		"page": pageOid,
	}

	// Build a projection map for just the fields we need for deserialization of our record types
	sourceProject, err := syncdb.BuildProjection(reflect.TypeOf(syncFrameInputRecord{}))
	if err != nil {
		return nil, fmt.Errorf("syncdb2020/getSyncFrameITer: failed to build projection for synFrameInputRecord (%w)", err)
	}

	// Query and return the records of interest
	return db.C("frames").Find(sourceMatch).Select(sourceProject).Iter(), nil
}

type frameLoaderRecord struct {
	FrameID           string
	LoaderID          string
	ParentFrameID     string
	NavigationURL     string
	SecurityOriginURL string
	AttachmentScript  *callFrame
	SinceWhen         time.Time
}

func (flr frameLoaderRecord) IsMain() bool {
	return flr.ParentFrameID == ""
}

type frameLookupMap struct {
	fidSliceMap map[string][]frameLoaderRecord
	fidLidMap   map[string]map[string]*frameLoaderRecord
	lidMap      map[string]*frameLoaderRecord
}

// Lookup finds the closest matching frameLoaderRecord matching the IDs given
// for non-navigated (i.e., same-origin) frames with no distinct SOP URL, it performs
// a recursive lookup to the parent frame's active SOP URL at the time of attachment
func (flm frameLookupMap) Lookup(loaderID, frameID string) (*frameLoaderRecord, error) {
	flr, ok := flm.lidMap[loaderID]
	if !ok {
		ilm, ok := flm.fidLidMap[frameID]
		if !ok {
			return nil, fmt.Errorf("syncdb2020/frameLookupMap: no such frame=%s", frameID)
		}
		flr, ok = ilm[loaderID]
		if !ok {
			log.Printf("syncdb2020/frameLookupMap: frame=%s has no match for loader=%s; using default", frameID, loaderID)
			flr, ok = ilm[""]
			if !ok {
				return nil, fmt.Errorf("syncdb2020/frameLookupMap: no default record for frame=%s", frameID)
			}
		}
	}
	if flr.SecurityOriginURL == "" {
		var sop string
		fid := flr.ParentFrameID
		for sop == "" {
			fidSlice, ok := flm.fidSliceMap[fid]
			if !ok {
				return nil, fmt.Errorf("syncdb2020/frameLookupMap: no such frame=%s", frameID)
			}
			for i := len(fidSlice) - 1; i >= 0; i-- {
				if fidSlice[i].SinceWhen.Before(flr.SinceWhen) {
					// This parent navigation/loader was active at the time of our attachment, so take it as our SOP
					sop = fidSlice[i].SecurityOriginURL
					break
				}
			}
			fid = fidSlice[0].ParentFrameID
		}
		flr.SecurityOriginURL = sop
	}
	return flr, nil
}

type frameEventByWhen []frameEvent

func (a frameEventByWhen) Len() int           { return len(a) }
func (a frameEventByWhen) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a frameEventByWhen) Less(i, j int) bool { return a[i].When.Before(a[j].When) }

// generateFrameLoaders turns syncFrameInputRecords from a Mongo iterator into a slice of frameLoaderRecords
func generateFrameLoaders(frameIter *mgo.Iter) (frameLookupMap, error) {
	flm := frameLookupMap{
		fidSliceMap: make(map[string][]frameLoaderRecord),
		fidLidMap:   make(map[string]map[string]*frameLoaderRecord),
		lidMap:      make(map[string]*frameLoaderRecord),
	}

	var frame syncFrameInputRecord
	for frameIter.Next(&frame) {
		if len(frame.FrameEvents) > 0 {
			sort.Sort(frameEventByWhen(frame.FrameEvents))
			for _, event := range frame.FrameEvents {
				flr := frameLoaderRecord{
					FrameID:       frame.FrameID,
					ParentFrameID: frame.ParentFrameID,
					SinceWhen:     event.When,
				}
				if event.Type == "navigation" {
					flr.LoaderID = event.LoaderID
					flr.NavigationURL = event.URL
					if event.SecurityOrigin != "://" {
						flr.SecurityOriginURL = event.SecurityOrigin
					}
				} else if (event.Type == "attached") && (len(event.Stack.CallFrames) > 0) {
					flr.AttachmentScript = &event.Stack.CallFrames[len(event.Stack.CallFrames)-1]
				} else if event.Type == "detached" {
					// rare bird!
					flr.LoaderID = "<detached>"
				}

				oldSlice := flm.fidSliceMap[flr.FrameID]
				flm.fidSliceMap[flr.FrameID] = append(oldSlice, flr)
				pflr := &flm.fidSliceMap[flr.FrameID][len(oldSlice)]

				ilm, ok := flm.fidLidMap[flr.FrameID]
				if !ok {
					ilm = make(map[string]*frameLoaderRecord)
					flm.fidLidMap[flr.FrameID] = ilm
				}
				ilm[flr.LoaderID] = pflr

				if flr.LoaderID != "" {
					flm.lidMap[flr.LoaderID] = pflr
				}
			}

			// Ensure a ""-loader entry in fidLidMap
			if _, ok := flm.fidLidMap[frame.FrameID][""]; !ok {
				flm.fidLidMap[frame.FrameID][""] = &flm.fidSliceMap[frame.FrameID][0]
			}
		}
	}

	return flm, nil
}

var flrImportFields = [...]string{
	"page_oid",
	"frame_id",
	"loader_id",
	"parent_frame_id",
	"is_main",
	"security_origin_url_sha256",
	"navigation_url_sha256",
	"attachment_script",
	"since_when",
}

func insertFrameLoaders(sqlDb *sql.DB, pageOid bson.ObjectId, flm frameLookupMap) error {
	log.Println("syncdb2020/insertFrameLoaders: creating temp table 'import_frame_loaders'...")
	err := syncdb.CreateImportTable(sqlDb, "frame_loaders_import_schema", "import_frame_loaders")
	if err != nil {
		log.Printf("syncdb2020/insertFrameLoaders: createImportTable(...) failed: %v\n", err)
		return err
	}

	flrChan := make(chan *frameLoaderRecord)
	go func() {
		for _, slice := range flm.fidSliceMap {
			for i := range slice {
				flrChan <- &slice[i]
			}
		}
		close(flrChan)
	}()

	log.Println("syncdb2020/insertFrameLoaders: bulk-inserting...")
	ub := syncdb.NewURLBakery()
	tempRows, err := syncdb.BulkInsertRows(sqlDb, "syncdb2020/insertFrameLoaders", "import_frame_loaders", flrImportFields[:], func() ([]interface{}, error) {
		var flr *frameLoaderRecord
		var ok bool

		ready := false
		for !ready {
			flr, ok = <-flrChan
			if !ok {
				return nil, nil // end-of-stream
			}
			flr, err := flm.Lookup(flr.LoaderID, flr.FrameID) // use Lookup to guarantee SOP URL patchup on same-SOP frames
			if err != nil {
				return nil, err
			}
			ready = flr.LoaderID != "<detached>"
		}

		sopURLHash := ub.URLToHash(flr.SecurityOriginURL)
		var navURLHash interface{}
		if flr.NavigationURL != "" {
			tempHash := ub.URLToHash(flr.NavigationURL)
			navURLHash = tempHash[:]
		}
		var attachmentScript interface{}
		if flr.AttachmentScript != nil {
			rawAttachmentScript, err := json.Marshal(flr.AttachmentScript)
			if err != nil {
				return nil, fmt.Errorf("bad attachment script info for frame=%s (%w)", flr.FrameID, err)
			}
			attachmentScript = string(rawAttachmentScript)
		}

		values := []interface{}{
			[]byte(pageOid),
			flr.FrameID,
			flr.LoaderID,
			syncdb.NullableString(flr.ParentFrameID),
			flr.IsMain(),
			sopURLHash[:],
			navURLHash,
			attachmentScript,
			flr.SinceWhen,
		}
		return values, nil
	})
	if err != nil {
		return fmt.Errorf("syncdb2020/insertFrameLoaders: bulk insert into import-table failed (%w)", err)
	}

	if err := ub.InsertBakedURLs(sqlDb); err != nil {
		return fmt.Errorf("syncdb2020/insertFrameLoaders: failed to insert baked URLs (%w)", err)
	}

	log.Println("syncdb2020/insertFrameLoaders: copy-upserting from temp table...")
	result, err := sqlDb.Exec(`
INSERT INTO frame_loaders (
	page_id, frame_id, loader_id, parent_frame_id, is_main,
	security_origin_url_id, navigation_url_id, attachment_script, since_when)
SELECT
	p.id, it.frame_id, it.loader_id, it.parent_frame_id, it.is_main,
	sou.id, nu.id, to_jsonb(it.attachment_script::json), it.since_when
FROM import_frame_loaders AS it
	INNER JOIN pages AS p ON (p.mongo_oid = it.page_oid)
	INNER JOIN urls AS sou ON (sou.sha256 = it.security_origin_url_sha256)
	LEFT JOIN urls AS nu ON (nu.sha256 = it.navigation_url_sha256);
`)
	if err != nil {
		return fmt.Errorf("syncdb2020/insertFrameLoaders: copy-upsert failed (%w)", err)
	}
	finalRows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("syncdb2020/insertFrameLoaders: failed to get rows upserted (%w)", err)
	}
	log.Printf("syncdb2020/insertFrameLoaders: upserted %d/%d records\n", finalRows, tempRows)
	return nil
}
