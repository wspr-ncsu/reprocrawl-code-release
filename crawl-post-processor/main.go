// vim: noet:ts=4:sw=4:sts=-1:lbr:bri:
package main

import (
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"

	"vpp/config"
	"vpp/mongoz"
	"vpp/syncdb"
	"vpp/syncdb2020"
)

// Version is set during build (to the git hash of the compiled code)
var Version string

func parseConfig(args []string) (config.VppConfig, error) {
	var c config.VppConfig
	if len(args) == 0 {
		return c, fmt.Errorf("Missing CMD")
	}
	c.Cmd = args[0]
	restStart := 1

	switch c.Cmd {
	case "version":
		c.Handler = handleShowVersion
		c.NoMongo = true
	case "old-syncdb":
		c.Handler = syncdb.HandleSyncDB
	case "syncdb2020":
		c.Handler = syncdb2020.HandleSyncDB2020
	case "syncdb2020-webhook":
		c.Handler = syncdb2020.HandleSyncDB2020Webhook
	default:
		return c, fmt.Errorf("Unknown command '%s'", c.Cmd)
	}

	c.Args = args[restStart:]
	return c, nil
}

func usage() {
	log.Println(`
usage: ./vpp CMD [args...]

ENV vars used for Mongo config:
	MONGODB_HOST (default 127.0.0.1)
	MONGODB_PORT (default 27017)
	MONGODB_DB   (default test)

	[optional auth via "admin" DB]
	MONGODB_USER (default n/a)
	MONGODB_PWD  (default n/a)

ENV vars used for syncdb
	PGHOST     (default n/a)
	PGPORT     (default n/a)
	PGDATABASE (default n/a)
	PGUSER     (default n/a)
	PGPASSWORD (default n/a)

	[optional RFC3339 datetimes for filtering query results]
	BEFORE     (default n/a)
	AFTER      (default n/a)

CMDs:
	version
		show program build hash/version

	syncdb2020 [PAGE_ID1 [PAGE_ID2 [...]]]
		import records from 2020 Mongo schema into Postgres aggregation tables
		on a page-record-by-page-record basis (to be on-demand/work-queue friendly)
	
	syncdb2020-webhook [[HOST]:PORT]
		listen on PORT for kpw-style HTTP posts (to "/kpw/vpc-post-processor")
	 
	old-syncdb [COL1 [COL2 [...]]]
		import records from 2019 Mongo schema into Postgres aggregation tables;
		by default syncs *everything*, but you can specify specific collections
		(use ? to discover)
	
	`)

	os.Exit(0)
}

func handleShowVersion(c config.VppConfig) error {
	fmt.Println(Version)
	return nil
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		log.Println(err)
		usage()
	}

	if !cfg.NoMongo {
		cfg.Mongo, err = mongoz.DialMongo()
		if err != nil {
			log.Panicf("couldn't dial mongo: %s", err)
		}
		log.Printf("connect to %s", cfg.Mongo.String())
		defer (func() {
			cfg.Mongo.Session.Close()
			log.Printf("disconnected from %s", cfg.Mongo.String())
		})()
	}

	err = cfg.Handler(cfg)
	if err != nil {
		log.Panicf("error from handler: %v", err)
	}
}
