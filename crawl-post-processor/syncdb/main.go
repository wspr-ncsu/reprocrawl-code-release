package syncdb

import (
	"database/sql"
	"log"

	"vpp/config"

	"gopkg.in/mgo.v2"
)

// ------------------------------------------------------------------------------
// Main sync driver entry point
// ------------------------------------------------------------------------------

type syncJobThing struct {
	description string
	driver      func(*mgo.Database, *sql.DB) error
}

var syncJobs = map[string]syncJobThing{
	"pages":             {"synching `pages` table", syncPages},
	"frames":            {"synching `frames` table", syncFrames},
	"request_inits":     {"synching `request_inits` table", syncRequestInits},
	"request_responses": {"synching `request_responses` table", syncRequestResponses},
	"request_failures":  {"synching `request_failures` table", syncRequestFailures},
	"parsed_scripts":    {"synching `parsed_scripts` table", syncParsedScripts},
	"squashed_targets":  {"synching `squashed_targets` table", syncSquashedTargets},
	"console_errors":    {"synching `console_errors` table", syncConsoleErrors},
	"visit_chains":      {"synching `visit_chains` table", syncVisitChains},
	"js_api_usage":      {"synching `js_api_usage` table", syncJSAPIUsage},
}

var syncJobOrder = []string{
	"pages", "frames",
	"request_inits", "request_responses", "request_failures",
	"parsed_scripts", "squashed_targets", "console_errors",
	"visit_chains", "js_api_usage",
}

// HandleSyncDB copies the configured source collections from Mongo into Postgres
func HandleSyncDB(c config.VppConfig) error {
	var jobSet map[string]syncJobThing
	if len(c.Args) > 0 {
		if c.Args[0] == "?" {
			log.Println("Collections Available to Sync: ")
			for _, name := range syncJobOrder {
				log.Printf("%s: for %s\n", name, syncJobs[name].description)
			}
			return nil
		}
		jobSet = make(map[string]syncJobThing)
		for _, name := range c.Args {
			_, ok := syncJobs[name]
			if ok {
				jobSet[name] = syncJobs[name]
			}
		}
	} else {
		jobSet = syncJobs
	}

	db := c.Mongo.Session.DB(c.Mongo.DBName)
	sqlDb, err := sql.Open("postgres", "") // We rely on the PGxxx ENV variables to be set for auth/etc.
	if err != nil {
		return err
	}
	defer func() {
		log.Println("syncdb: closing Postgres connection...")
		err := sqlDb.Close()
		if err != nil {
			log.Printf("syncdb: error closing Postgress connection (%v)\n", err)
		}
		log.Println("syncdb: DONE")
	}()

	for _, name := range syncJobOrder {
		if job, ok := jobSet[name]; ok {
			log.Println(job.description)
			err = job.driver(db, sqlDb)
			if err != nil {
				return err
			}
		}
	}

	log.Println("syncdb: complete; cleaning up...")
	return nil
}
