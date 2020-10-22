package syncdb2020

import (
	"database/sql"
	"fmt"
	"log"
	"vpp/config"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func handlePageRecord(db *mgo.Database, sqlDb *sql.DB, oid bson.ObjectId) error {
	page, err := getSyncPage(db, oid)
	if err != nil {
		return fmt.Errorf("syncdb2020/handlePageRecord: failed to lookup page _id=%s (%w)", oid, err)
	}
	log.Printf("page _id=%s:\n-----------\n%+v\n", oid, *page)

	pid, err := insertPageRecord(sqlDb, page)
	if err != nil {
		return fmt.Errorf("syncdb2020/handlePageRecord: failed to import page _id=%s (%w)", oid, err)
	}
	log.Printf("syncdb2020/handlePageRecord: inserted page record for oid %s (pages.id=%d)\n", oid, pid)

	frameIter, err := getSyncFrameIter(db, oid)
	if err != nil {
		return fmt.Errorf("syncdb2020/handlePageRecord: failed to lookup frames for page _id=%s (%w)", oid, err)
	}

	flm, err := generateFrameLoaders(frameIter)
	if err != nil {
		return fmt.Errorf("syncdb2020/handlePageRecord: failed to generate frame-loader records for page _id=%s (%w)", oid, err)
	}

	err = insertFrameLoaders(sqlDb, oid, flm)
	if err != nil {
		return fmt.Errorf("syncdb2020/handlePageRecord: failed to insert frame-loader records for pages.id=%d (%w)", pid, err)
	}

	summaries, err := getRequestSummaries(db, oid)
	if err != nil {
		return fmt.Errorf("syncdb2020/handlePageRecord: failed to find request-summaries for page _id=%s (%w)", oid, err)
	}

	err = insertRequestSummaries(sqlDb, oid, summaries, flm)
	if err != nil {
		return fmt.Errorf("syncdb2020/handlePageRecord: failed to insert request-summaries for page _id=%s (%w)", oid, err)
	}

	// this assumes the page record is already in the DB
	err = syncJSAPIUsage(db, sqlDb, oid)
	if err != nil {
		return fmt.Errorf("syncdb2020/handlePageRecord: failed to sync JS API summary for page _id=%s (%w)", oid, err)
	}

	return nil
}

// HandleSyncDB2020 because we just couldn't get it right in 2019...
func HandleSyncDB2020(c config.VppConfig) error {
	db := c.Mongo.Session.DB(c.Mongo.DBName)
	sqlDb, err := sql.Open("postgres", "") // We rely on the PGxxx ENV variables to be set for auth/etc.
	if err != nil {
		return err
	}
	defer func() {
		log.Println("syncdb2020: closing Postgres connection...")
		err := sqlDb.Close()
		if err != nil {
			log.Printf("syncdb2020: error closing Postgres connection (%v)\n", err)
		}
		log.Println("syncdb2020: DONE")
	}()

	for _, arg := range c.Args {
		if !bson.IsObjectIdHex(arg) {
			return fmt.Errorf("syncdb2020: provided page OID (%s) is invalid", arg)
		}
		oid := bson.ObjectIdHex(arg)
		if err := handlePageRecord(db, sqlDb, oid); err != nil {
			return fmt.Errorf("syncdb2020: error processing page _id=%s (%w)", oid, err)
		}
	}
	return nil
}
