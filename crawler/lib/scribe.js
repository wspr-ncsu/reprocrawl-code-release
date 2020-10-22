'use strict';

// First-party imports
const crypto = require('crypto');
const os = require('os');

// Third-party imports
const streamBuffers = require('stream-buffers');
const { gzip, ungzip } = require('node-gzip');
const { MongoClient, GridFSBucket, ObjectId } = require('mongodb');

// MongoDB connection parameters
const MONGODB_HOST = process.env.MONGODB_HOST || 'localhost';
const MONGODB_PORT = process.env.MONGODB_PORT || '27017';
const MONGODB_DB = process.env.MONGODB_DB || 'not_my_db';

// MongoDB Auth (optional)
const MONGODB_USER = process.env.MONGODB_USER; // default undefined (no auth)
const MONGODB_PASS = process.env.MONGODB_PASS || process.env.MONGODB_PWD; // support alternate ENV name for compatability with legacy VPC stack/deployments
const MONGODB_AUTHDB = process.env.MONGODB_AUTHDB || 'admin';


// Mongo schema size constants
const INLINE_BLOB_SIZE = 1024 * 1024;

// Utility one-liner to get the hex-encoded sha256 hash of a buffer
function getSha256(buff) {
    return crypto.createHash("sha256").update(buff).digest('hex');
}

// Utility to promisify writing a static buffer of data to a stream
function shove(buff, stream) {
    return new Promise(function(resolve, reject) {
        stream.on('finish', (value) => resolve(value));
        stream.on('error', reject);
        stream.end(buff);
    });
}

// Utility to promisify slurping a stream into a memory buffer
function slurp(stream) {
    return new Promise(function(resolve, reject) {
        const buff = new streamBuffers.WritableStreamBuffer();
        buff.on('finish', () => resolve(buff.getContents()));
        buff.on('error', reject);
        stream.pipe(buff);
    });
}

// Stateful page logging
class PageLogger {
    constructor(db, pageId) {
        this._db = db;
        this._id = pageId;
    }

    static async create(db, properties) {
        // Create a page document
        const pageDoc = Object.create(null);
        Object.assign(pageDoc, properties);

        // Start the history with the status
        pageDoc.status = {
            state: "created",
            created: { when: new Date() },
        };

        const { insertedId } = await db._pages.insertOne(pageDoc);
        return new PageLogger(db, insertedId);
    }

    get id() {
        return this._id.toString();
    }

    updatePageStatus(event, info) {
        const whatDoc = {
            when: new Date(),
        };
        if (info) { whatDoc.info = info; }

        return this._db._pages.updateOne({ _id: this._id }, {
            $set: {
                "status.state": event,
                "status.lastWhen": whatDoc.when,
                [`status.${event}`]: whatDoc,
            },
        });
    }

    updatePageParsedUrl(cookedUrl) {
        return this._db._pages.updateOne({ _id: this._id }, {
            $set: {
                "cookedUrl": cookedUrl
            },
        });
    }

    updatePageMainFrameNavTime(time) {
        return this._db._pages.updateOne({ _id: this._id }, {
            $set: { "mainFrameNavigationTime": time }
        });
    }

    async updatePageMainFrameContent(url, content) {
        const [sourceDigest, contentOid] = await this._db.archiveBlob(url, content, {
            page: this._id,
            type: "mainFrameContent"
        });

        return this._db._pages.updateOne({ _id: this._id }, {
            $set: {
                "mainFrameContentBlob": contentOid,
                "mainFrameContentHash": sourceDigest
            }
        });
    }

    async updatePageScreenshot(url, content) {
        const [sourceDigest, contentOid] = await this._db.archiveBlob(url, content, {
            page: this._id,
            type: "pageScreenshotContent"
        });

        return this._db._pages.updateOne({ _id: this._id }, {
            $set: {
                "pageScreenshotBlob": contentOid,
                "pageScreenshotHash": sourceDigest
            }
        });
    }

    // Special page status update functions flagging finality of the page visit
    markPageCompleted(info) {
        return this.updatePageStatus("completed", info);
    }

    markPageAborted(info) {
        return this.updatePageStatus("aborted", info);
    }

    async runScriptParsed(url, scriptSource, stackTrace) {
        const doc = {
            url: url,
            caller: stackTrace[0]
        };

        if (scriptSource) {
            const [sourceDigest, sourceOid] = await this._db.archiveBlob(url, scriptSource, {
                page: this._id,
                type: "scriptParsed"
            });
            doc.blobOid = sourceOid;
            doc.blobHash = sourceDigest;
        } else {
            doc.warning = "n/a";
        }

        return this._db.logScriptEvent(this._id, 'scriptParsed', doc);
    }

    async runRequestResponse(url, requestId, requestMeta, responseBody) {
        const doc = {
            url: url,
            requestId: requestId,
            meta: requestMeta,
        };

        if (responseBody) {
            const [bodyDigest, bodyOid] = await this._db.archiveBlob(url, responseBody, {
                page: this._id,
                type: 'httpBody'
            });
            doc.blobOid = bodyOid;
            doc.blobHash = bodyDigest;
        }

        return this._db.logRequestEvent(this._id, 'requestResponse', doc);
    }

    runRequestFailure(url, requestId, requestMeta) {
        return this._db.logRequestEvent(this._id, 'requestFailure', {
            url: url,
            requestId: requestId,
            meta: requestMeta,
        });
    }

    runVisit(url) {
        return this._db.logPageEvent(this._id, 'visit', {
            url: url
        });
    }

    runPageLoad(time) {
        return this._db._pages.updateOne({ _id: this._id }, {
            $set: { "pageLoadTime": time }
        });
    }

    runConsoleError(type, text, stack) {
        const doc = {
            type: type,
            text: text,
        };
        if (stack) { doc.stack = stack; }
        return this._db.logScriptEvent(this._id, 'consoleError', doc);
    }

    runRequestWillBeSent(reqInfo) {
        return this._db.logRequestEvent(this._id, 'requestWillBeSent', reqInfo);
    }

    runTargetSquashed(url) {
        return this._db.logPageEvent(this._id, 'targetSquashed', {
            url: url
        });
    }

    saveFramesBulk(frameMap) {
        // Save the frame entries in the map.
        let frameIter = frameMap.keys();
        let frameId = frameIter.next();
        let frameDocs = [];
        while (!frameId.done) {
            let frameObj = frameMap.get(frameId.value);
            frameObj.page = this._id;
            frameDocs.push(frameObj);

            frameId = frameIter.next();
        }
        return this._db._frames.insertMany(frameDocs);
    }
}

// Encapsulated logic for updating our DB (exported)
class Scribe {
    constructor() {
        this.host = MONGODB_HOST;
        this.port = MONGODB_PORT;
        this.dbName = MONGODB_DB;
        this.auth = MONGODB_USER ? {
            user: MONGODB_USER,
            pwd: MONGODB_PASS,
            authDb: MONGODB_AUTHDB,
        } : null;
        this._client = null;
        this._db = null;

        this._fs = null;
        this._pages = null;
        this._request_events = null;
        this._page_events = null;
        this._script_events = null;
        this._blob_set = null;
        this._blobs = null;
        this._vv8logs = null;
        this._crawls = null;
        this._frames = null;
        this._system = null;
        this._jobs = null;
    }

    async connect() {
        console.log(`scribe: connecting to ${this.host}:${this.port}/${this.dbName}...`);
        const connectUrl = this.auth ?
            `mongodb://${this.auth.user}:${this.auth.pwd}@${this.host}:${this.port}/${this.auth.authDb}` :
            `mongodb://${this.host}:${this.port}`;

        // Connect	
        this._client = await MongoClient.connect(connectUrl, {
            useNewUrlParser: true,
            useUnifiedTopology: true
        });
        try {
            // Establish references to collections/buckets
            this._db = this._client.db(this.dbName);
            this._fs = new GridFSBucket(this._db);
            this._pages = this._db.collection("pages");
            this._request_events = this._db.collection("request_events");
            this._page_events = this._db.collection("page_events");
            this._script_events = this._db.collection("script_events");
            this._blob_set = this._db.collection("blob_set");
            this._blobs = this._db.collection("blobs");
            this._vv8logs = this._db.collection("vv8logs");
            this._crawls = this._db.collection("crawls");
            this._frames = this._db.collection("frames");
            this._system = this._db.collection("systemEvents");
            this._jobs = this._db.collection("jobs");

            // Create necessary indices (if they do not exist)
            await this._blob_set.createIndex('sha256', { unique: true });
            await this._vv8logs.createIndex({ root_name: 'hashed' });
            await this._frames.createIndex({ "page": 1, "frameId": 1 }, { unique: true });
            await this._request_events.createIndex({ "page": 1 });
            await this._page_events.createIndex({ "page": 1 });
            await this._script_events.createIndex({ "page": 1 });

            console.log(`scribe: connection successful`);
        } catch (err) {
            await this._client.close().catch((err) => console.error(`Scribe.connect: client.close() during catch threw '${err}'`));
            throw err;
        }
    }

    async close() {
        console.log(`scribe: closing connection to ${this.host}:${this.port}/${this.dbName}`);
        try {
            await this._client.close();
            console.log(`scribe: connection closed`);
        } catch (err) {
            console.error(`scribe: connection close failed`);
            console.error(err);
        }
    }

    async getGridFSFile(fileId) {
        const downStream = this._fs.openDownloadStream(fileId);
        const data = await slurp(downStream);
        return data;
    }

    async archiveBlob(name, data, meta) {
        meta = meta || {};
        if (!data) {
            // Special case for missing data: just a blobs record, no blob_set entry
            meta.filename = name;
            const oid = (await this._blobs.insertOne(meta)).insertedId;
            return [null, oid];
        }
        const digest = getSha256(data);

        const compress = ('compress' in meta) ? meta.compress : true;
        delete meta.compress;
        if (compress) {
            meta.orig_size = data.length;
            data = await gzip(data);
        }

        // Insert the blob-set entry; detect duplicate key errors
        let alreadyThere = false;
        let entryId = null;
        try {
            const result = await this._blob_set.insertOne({
                sha256: digest,
                z: compress
            });
            entryId = result.insertedId;
        } catch (err) {
            if (err.name === 'MongoError' && err.code === 11000) {
                alreadyThere = true;
            } else {
                // Let other errors propagate
                throw err;
            }
        }

        // Insert data only if we don't already have this blob on record
        if (!alreadyThere) {
            try {
                if (data.length < INLINE_BLOB_SIZE) {
                    // Easy case---direct data storage in the blob_set entry
                    await this._blob_set.updateOne({ _id: entryId }, { $set: { data: data } });
                } else {
                    // Ugly case---GridFS
                    const upStream = this._fs.openUploadStream(digest, { disableMD5: true });
                    const file = await shove(data, upStream);
                    await this._blob_set.updateOne({ _id: entryId }, { $set: { file_id: file._id } });
                }
            } catch (err) {
                console.error("scribe: error committing blob-set data (rollback)");
                console.error(err);
                await this._blob_set.deleteOne({ _id: entryId });
                throw err;
            }
        }

        // Time to create the blob instance record
        meta.filename = name;
        meta.size = data.length;
        meta.sha256 = digest;
        const { insertedId } = await this._blobs.insertOne(meta);
        return [digest, insertedId];
    }

    async extractBlob(blobDigest) {
        let blobContent = null;
        try {
            const blob = await this._blob_set.findOne({ sha256: blobDigest });
            if (!!blob) {
                if ('data' in blob) {
                    // Content is inline
                    var data = Buffer.from(blob['data']['buffer']);
                    if (blob['z']) {
                        // Content is compressed
                        blobContent = await ungzip(data);
                    } else {
                        // Content is uncompressed
                        blobContent = data;
                    }
                } else if ('file_id' in blob) {
                    // Content is in gridfs
                    if (blob['z']) {
                        // Content is compressed
                        let compBlobContent = await this.getGridFSFile(blob['file_id']);
                        blobContent = await ungzip(compBlobContent);
                    } else {
                        // Content is uncompressed
                        blobContent = await this.getGridFSFile(blob['file_id']);
                    }
                } else {
                    throw new Error('Script blob does not have expected fields');
                }
            }
        } catch (err) {
            console.error(`Error occurred during blob content extraction with sha256 ${blobDigest}`);
            console.error(err);
            throw err;
        }

        return blobContent;
    }

    // Insert various "run" event records
    logRequestEvent(page, event, doc) {
        doc.date = new Date();
        doc.page = page;
        doc.event = event;
        return this._request_events.insertOne(doc);
    }

    logPageEvent(page, event, doc) {
        doc.date = new Date();
        doc.page = page;
        doc.event = event;
        return this._page_events.insertOne(doc);
    }

    logScriptEvent(page, event, doc) {
        doc.date = new Date();
        doc.page = page;
        doc.event = event;
        return this._script_events.insertOne(doc);
    }

    // Factory to create a "Visit" logger
    createVisit(properties) {
        return PageLogger.create(this, properties);
    }

    // Log a system-wide event not tied to any one job (usually some kind of error)
    systemEvent(doc) {
        Object.assign(doc, {
            "when": new Date(),
            "reporter": os.hostname(),
        });
        return this._system.insertOne(doc);
    }

    // Mark job completed
    async markJobWithStatus(jobId, status) {
        if (!(jobId instanceof ObjectId)) {
            jobId = new ObjectId(jobId);
        }
        console.log(`marking job ${jobId} done...`);
        const updateDoc = {
            completed: {
                status: status,
                when: new Date(),
            },
        };
        return this._jobs.updateOne({
            _id: jobId,
            completed: null,
        }, { $set: updateDoc });
    }
}
exports.Scribe = Scribe;