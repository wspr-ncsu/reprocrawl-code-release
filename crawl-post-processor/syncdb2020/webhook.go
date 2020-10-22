package syncdb2020

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"vpp/config"

	"gopkg.in/mgo.v2/bson"
)

type kpwInvocation struct {
	PageID bson.ObjectId `json:"pageId"`
}

// HandleSyncDB2020Webhook for a kpw-friendly webhook server...
// handles POSTs (body content-type: JSON) to "/kpw/vv8-post-processor" (calling invoke)
// handles GETs to "/ready" (no body, always HTTP 200; used for readiness check/heartbeat)
func HandleSyncDB2020Webhook(c config.VppConfig) error {
	listen := ":8080"
	if len(c.Args) >= 1 {
		listen = c.Args[0]
	}

	http.HandleFunc("/kpw/vpc-post-processor", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "must POST a {pageId: ...} JSON document", http.StatusBadRequest)
			log.Printf("kpw-worker: non-POST received at dispatcher endpoint?!\n")
			return
		}
		var msg kpwInvocation
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			http.Error(w, fmt.Sprintf("JSON decode error (%v)\n", err), http.StatusBadRequest)
			log.Printf("kpw-worker: JSON decode error (%v)\n", err)
			return
		}

		altConfig := config.VppConfig{
			Args:  []string{msg.PageID.Hex()},
			Mongo: c.Mongo,
		}

		// try to re-establish the connection pool if it got disrupted by a network hiccup
		c.Mongo.Session.Refresh()

		err := HandleSyncDB2020(altConfig)
		if err != nil {
			http.Error(w, fmt.Sprintf("runtime error (%v)\n", err), http.StatusInternalServerError)
			log.Printf("kpw-worker: runtime error (%v)\n", err)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		// Nothing to do here but say "OK" (it tells them we're alive)
		log.Printf("kpw-worker: /ready OK (from %v)\n", r.RemoteAddr)
		w.WriteHeader(http.StatusOK)
	})
	log.Printf("webhook server listening at %s\n", listen)
	return http.ListenAndServe(listen, nil)
}
