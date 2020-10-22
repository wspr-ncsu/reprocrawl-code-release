package mongoz

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"time"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// Construct (from environment tuning variables) a MongoDB connection URL
func getDialURL(dbName string) string {
	host := GetEnvDefault("MONGODB_HOST", "localhost")
	port := GetEnvDefault("MONGODB_PORT", "27017")
	return fmt.Sprintf("mongodb://%s:%s/%s", host, port, dbName)
}

// MongoConnection encapsulates the connection meta info and the session
type MongoConnection struct {
	URL     string       // connection URL dialed ("mongodb://$host:$port/$db")
	User    string       // username authenticated as (if applicable; "" otherwise)
	DBName  string       // database name separate from URL
	Session *mgo.Session // Active (and possibly authenticated) session to Mongo
}

func (mc MongoConnection) String() string {
	var active, auth string
	if mc.Session != nil {
		active = " (ACTIVE)"
	}
	if mc.User != "" {
		auth = fmt.Sprintf(" as %s", mc.User)
	}
	return fmt.Sprintf("%s%s%s", mc.URL, auth, active)
}

// DialMongo creates a possibly authenticated Mongo session based on ENV configs
// Returns a MongoConnection on success; error otherwise (in all cases, the `url` field of MongoConnection will be set)
func DialMongo() (MongoConnection, error) {
	var conn MongoConnection
	conn.DBName = GetEnvDefault("MONGODB_DB", "not_my_db")
	conn.URL = getDialURL(conn.DBName)
	session, err := mgo.Dial(conn.URL)
	if err != nil {
		return conn, err
	}

	conn.User = GetEnvDefault("MONGODB_USER", "")
	if conn.User != "" {
		creds := mgo.Credential{Username: conn.User}
		creds.Password = GetEnvDefault("MONGODB_PWD", "")
		creds.Source = GetEnvDefault("MONGODB_AUTHDB", "admin")
		err = session.Login(&creds)
		if err != nil {
			session.Close()
			return conn, err
		}
	}

	// Bump up the socket timeout to handle long pauses during big queries.
	// (The default 1-minute timeout was killing some batch queries...)
	session.SetSocketTimeout(1 * time.Hour)

	conn.Session = session
	return conn, nil
}

// A BlobRecord represents a single instance of a blob being seen (not stored)
type BlobRecord struct {
	Filename string        `bson:"filename"`
	Size     int           `bson:"size"`
	Sha256   string        `bson:"sha256"`
	Job      string        `bson:"job"` // DEPRECATED (old schema)
	Type     string        `bson:"type"`
	PageID   bson.ObjectId `bson:"pageId"`
}

// A BlobSetRecord represents a deduplicated entry in the store
type BlobSetRecord struct {
	Sha256     string        `bson:"sha256"`
	FileID     bson.ObjectId `bson:"file_id"`
	Data       []byte        `bson:"data"`
	Compressed bool          `bson:"z"`
}

func getBlobReaderByOid(db *mgo.Database, blobOid bson.ObjectId) (io.Reader, error) {
	var err error

	blobs := db.C("blobs")

	var blob BlobRecord
	err = blobs.FindId(blobOid).One(&blob)
	if err != nil {
		return nil, err
	}

	return getBlobReaderByHash(db, blob.Sha256, (blob.Type == "vv8logz"))
}

func getBlobReaderByHash(db *mgo.Database, hexSha256 string, forceUnzip bool) (io.Reader, error) {
	blobSet := db.C("blob_set")

	var entry BlobSetRecord
	err := blobSet.Find(bson.M{"sha256": hexSha256}).One(&entry)
	if err != nil {
		return nil, err
	}

	var reader io.Reader
	if entry.Data == nil {
		// GridFS record
		reader, err = db.GridFS("fs").OpenId(entry.FileID)
		if err != nil {
			return nil, err
		}
	} else {
		// Inline record
		reader = bytes.NewReader(entry.Data)
	}

	// Should it inflate on the fly?
	if entry.Compressed || forceUnzip {
		reader, err = gzip.NewReader(NewClosingReader(reader))
		if err != nil {
			return nil, err
		}
	}

	return NewClosingReader(reader), nil
}

func newBlobDataReader(db *mgo.Database, hexSha256 string) (io.Reader, error) {
	return getBlobReaderByHash(db, hexSha256, false)
}
