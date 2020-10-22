package syncdb

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/lib/pq"
	"gopkg.in/mgo.v2/bson"
)

// NullableString returns either <val> (if not "") or nil
func NullableString(val string) interface{} {
	var nullable interface{}
	if val != "" {
		nullable = val
	}
	return nullable
}

// NullableBytes returns either <val> (if not length 0) or nil
func NullableBytes(val []byte) interface{} {
	var nullable interface{}
	if len(val) == 0 {
		nullable = val
	}
	return nullable
}

// NullableInt returns either <val> (if not 0) or nil
func NullableInt(val int) interface{} {
	var nullable interface{}
	if val != 0 {
		nullable = val
	}
	return nullable
}

// NullableTimestamp returns either <val> (if not .IsZero()) or nil
func NullableTimestamp(val time.Time) interface{} {
	var nullable interface{}
	if !val.IsZero() {
		nullable = val
	}
	return nullable
}

// BuildProjection builds a Mongo projection map to retrieve only what a given struct-type will need on deserialization
func BuildProjection(structType reflect.Type) (bson.M, error) {
	var spider func(bson.M, string, reflect.Type) (bool, error)
	spider = func(pmap bson.M, stem string, t reflect.Type) (bool, error) {
		found := false
		if t.Kind() == reflect.Struct {
			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i)
				if bson, ok := field.Tag.Lookup("bson"); ok {
					found = true
					nested, err := spider(pmap, stem+bson+".", field.Type)
					if err != nil {
						return false, err
					}
					if !nested {
						pmap[stem+bson] = 1
					}
				}
			}
		}
		return found, nil
	}
	pmap := make(bson.M)
	_, err := spider(pmap, "", structType)
	if err != nil {
		return nil, err
	}
	return pmap, nil
}

// getBeforeAfterFilter constructs a Mongo query filter expression for a single date field from BEFORE/AFTER env variables [if available]
func getBeforeAfterFilter() (bson.M, error) {
	res := bson.M{}
	if rawBefore, hasBefore := os.LookupEnv("BEFORE"); hasBefore {
		cookedBefore, err := time.Parse(time.RFC3339, rawBefore)
		if err != nil {
			return nil, err
		}
		res["$lt"] = cookedBefore
	}
	if rawAfter, hasAfter := os.LookupEnv("AFTER"); hasAfter {
		cookedAfter, err := time.Parse(time.RFC3339, rawAfter)
		if err != nil {
			return nil, err
		}
		res["$gt"] = cookedAfter
	}
	return res, nil
}

// getBeforeAfterFilterOid is like getBeforeAfterFilter, but for querying on ObjectID fields instead of dates
func getBeforeAfterFilterOid() (bson.M, error) {
	res := bson.M{}
	if rawBefore, hasBefore := os.LookupEnv("BEFORE"); hasBefore {
		cookedBefore, err := time.Parse(time.RFC3339, rawBefore)
		if err != nil {
			return nil, err
		}
		res["$lt"] = bson.NewObjectIdWithTime(cookedBefore)
	}
	if rawAfter, hasAfter := os.LookupEnv("AFTER"); hasAfter {
		cookedAfter, err := time.Parse(time.RFC3339, rawAfter)
		if err != nil {
			return nil, err
		}
		res["$gt"] = bson.NewObjectIdWithTime(cookedAfter)
	}
	return res, nil
}

// CreateImportTable creates a temp table cloning the schema of a given table
func CreateImportTable(sqlDb *sql.DB, likeTable, importTableName string) error {
	_, err := sqlDb.Exec(fmt.Sprintf(`CREATE TEMP TABLE "%s" (LIKE "%s" INCLUDING DEFAULTS INCLUDING INDEXES);`, importTableName, likeTable))
	if err != nil {
		return err
	}
	return nil
}

// BulkFieldGenerator generates fields in bulk
type BulkFieldGenerator func() ([]interface{}, error)

// BulkInsertRows performs a bulk-insert transaction, streaming callback-provided data into a temp import table
func BulkInsertRows(sqlDb *sql.DB, functionName, tableName string, fieldNames []string, generator BulkFieldGenerator) (int64, error) {
	var rowCount int64

	txn, err := sqlDb.Begin()
	if err != nil {
		return 0, err
	}
	defer func() {
		if txn != nil {
			log.Printf("%s: defer-triggered txn.Rollback()...", functionName)
			if rollbackErr := txn.Rollback(); rollbackErr != nil {
				log.Printf("%s: txn.Rollback() failed: %v\n", functionName, rollbackErr)
			}
		}
	}()

	stmt, err := txn.Prepare(pq.CopyIn(tableName, fieldNames...))
	if err != nil {
		return 0, fmt.Errorf("%s: txn.Prepare(...) failed: %w", functionName, err)
	}
	defer func() {
		if stmt != nil {
			log.Printf("%s: defer-triggered stmt.Close()...", functionName)
			if err := stmt.Close(); err != nil {
				log.Printf("%s: stmt.Close() failed: %v\n", functionName, err)
			}
		}
	}()

	lastProgressReport := time.Now()
	for {
		values, err := generator()
		if err != nil { // error/abort (rollback)
			return 0, fmt.Errorf("%s: generator(...) failed: %w", functionName, err)
		} else if values == nil { // end-of-stream (commit)
			break
		} else { // data (insert)
			_, err = stmt.Exec(values...)
			if err != nil {
				return 0, fmt.Errorf("%s: stmt.Exec(...) failed: %w", functionName, err)
			}
			rowCount++
			if time.Now().Sub(lastProgressReport) >= (time.Second * 5) {
				log.Printf("%s: processed %d records so far...\n", functionName, rowCount)
				lastProgressReport = time.Now()
			}
		}
	}
	log.Printf("%s: done processing after %d records\n", functionName, rowCount)

	_, err = stmt.Exec()
	if err != nil {
		return 0, fmt.Errorf("%s: final stmt.Exec() failed: %w", functionName, err)
	}
	err = stmt.Close()
	stmt = nil // nothing to close now
	if err != nil {
		return 0, fmt.Errorf("%s: stmt.Close() failed: %w", functionName, err)
	}
	err = txn.Commit()
	txn = nil // nothing to rollback now
	if err != nil {
		return 0, err
	}

	return rowCount, nil
}
