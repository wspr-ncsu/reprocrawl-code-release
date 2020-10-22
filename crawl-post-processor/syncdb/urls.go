package syncdb

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"log"
	"net/url"

	pubsuf "golang.org/x/net/publicsuffix"
)

// hashBlock is storage for a SHA256 digest
type hashBlock [sha256.Size]byte

// bakedURL is a URL waiting for insertion into the `urls` table
type bakedURL struct {
	Sha256   hashBlock
	Full     string
	Scheme   string
	Hostname string
	Port     string
	Path     string
	Query    string
	Etld1    string
	Stemmed  string
}

// urlsFields holds the in-order list of field names used for bulk-inserting crawl records into a temp-clone of `urls_import_schema`
var urlImportFields = [...]string{
	"sha256",
	"url_full",
	"url_scheme",
	"url_hostname",
	"url_port",
	"url_path",
	"url_query",
	"url_etld1",
	"url_stemmed",
}

// URLBakery keeps a stash of cooked URLs pending insertion
type URLBakery struct {
	stash map[string]*bakedURL
}

func NewURLBakery() *URLBakery {
	return &URLBakery{
		stash: make(map[string]*bakedURL),
	}
}

// URLToHash takes a raw URL string and returns its SHA256 hash (after stashing it if it is new/unseen)
func (ub *URLBakery) URLToHash(rawurl string) hashBlock {
	curl, ok := ub.stash[rawurl]
	if !ok {
		curl = &bakedURL{
			Sha256: sha256.Sum256([]byte(rawurl)),
			Full:   rawurl,
		}

		purl, err := url.Parse(rawurl)
		if err != nil {
			log.Printf("urlBakery.toHash: error (%v) parsing '%s'; no fields available\n", err, rawurl)
		} else {
			curl.Scheme = purl.Scheme
			curl.Hostname = purl.Hostname()
			curl.Port = purl.Port()
			curl.Path = purl.EscapedPath()
			curl.Query = purl.RawQuery

			etld1, err := pubsuf.EffectiveTLDPlusOne(purl.Hostname())
			if err != nil {
				curl.Etld1 = purl.Hostname()
			} else {
				curl.Etld1 = etld1
			}
			curl.Stemmed = curl.Etld1 + curl.Path
		}
		ub.stash[rawurl] = curl
	}
	return curl.Sha256
}

// InsertBakedURLs performs a de-duping bulk insert of cooked URL records into PG's `urls` table
func (ub *URLBakery) InsertBakedURLs(sqlDb *sql.DB) error {
	if len(ub.stash) == 0 {
		log.Println("urlBakery.insertBakedURLs: no baked URLs in the oven; nothing to do!")
		return nil
	}

	log.Println("urlBakery.insertBakedURLs: creating temp table 'import_urls'...")
	err := CreateImportTable(sqlDb, "urls_import_schema", "import_urls")
	if err != nil {
		log.Printf("urlBakery.insertBakedURLs: createImportTable(...) failed: %v\n", err)
		return err
	}
	defer func() {
		log.Printf("urlBakery.insertBakedURLs: dropping temp import table...\n")
		_, err := sqlDb.Exec(`DROP TABLE import_urls;`)
		if err != nil {
			log.Printf("urlBakery.insertBakedURLs: error (%v) dropping `import_urls` temp table\n", err)
		}
	}()

	urlChan := make(chan *bakedURL)
	go func() {
		for _, curl := range ub.stash {
			urlChan <- curl
		}
		close(urlChan)
	}()

	log.Println("urlBakery.insertBakedURLs: bulk-inserting...")
	importRows, err := BulkInsertRows(sqlDb, "urlBakery.insertBakedURLs", "import_urls", urlImportFields[:], func() ([]interface{}, error) {
		curl, ok := <-urlChan
		if !ok {
			log.Printf("urlBakery.insertBakedURLs: iteration complete, committing transation...\n")
			return nil, nil // signal end-of-stream
		}

		values := []interface{}{
			curl.Sha256[:],
			curl.Full,
			NullableString(curl.Scheme),
			NullableString(curl.Hostname),
			NullableString(curl.Port),
			NullableString(curl.Path),
			NullableString(curl.Query),
			NullableString(curl.Etld1),
			NullableString(curl.Stemmed),
		}
		return values, nil
	})
	if err != nil {
		return err
	}

	// Wrap the next pair of operations in a serializable transaction
	tx, err := sqlDb.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer func() {
		if tx != nil {
			log.Println("urlBakery.insertBakedURLs: rolling back copy-upsert")
			if err := tx.Rollback(); err != nil {
				log.Printf("urlBakery.insertBakedURLs: copy-upsert rollback error (%v)", err)
			}
		}
	}()

	// Baked URLs are shared in common across all logs; concurrent upsert can lead to deadlock; GO NUCLEAR and lock the table
	// (auto released on transaction commit/rollback)
	if _, err = tx.Exec(`LOCK TABLE urls IN SHARE ROW EXCLUSIVE MODE;`); err != nil {
		return err
	}

	log.Println("urlBakery.insertBakedURLs: copy-inserting from temp table...")
	result, err := tx.Exec(`
INSERT INTO urls (
		sha256, url_full, url_scheme, url_hostname, url_port,
		url_path, url_query, url_etld1, url_stemmed)
	SELECT
		iu.sha256, iu.url_full, iu.url_scheme, iu.url_hostname, iu.url_port,
		iu.url_path, iu.url_query, iu.url_etld1, iu.url_stemmed
	FROM import_urls AS iu
ON CONFLICT DO NOTHING;
`)
	if err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	tx = nil // No more deferred-rollback (we're committed!)

	insertRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	log.Printf("urlBakery.insertBakedURLs: inserted %d (out of %d) import rows\n", insertRows, importRows)

	return nil
}
