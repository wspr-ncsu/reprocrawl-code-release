'use strict';

/*
Basic express based handler for the kpw pipeline, see here for more details:
https://github.ncsu.edu/jjuecks/kpw
*/

// npm imports
const express = require('express');

// Project imports
const scribe = require('./scribe');
const { visitPage } = require('./visitor');

const DEFAULT_NAV_TIMEOUT = 90.0;
const DEFAULT_LOITER_TIME = 30.0;

// Setup express
const app = express();

// Create a router
const router = express.Router()

// Add the json middleware for handling post data
router.use(express.json());

function setupKpwEndpoint(router) {
    router.post('/kpw/:endpoint', async function(req, res) {
        let db = null;
        try {
            // Treat the body as a complete page document (minus status, etc.); keep only "context" and "visit" keys (and the "original_job_id" so we can mark the originating "job" complete)
            // (We assume the contents are OK and let it blow up if now; livin' la YOLO Node.js, baby...)
            const { original_job_id, context, visit, __barrier__ } = req.body;

            if (__barrier__) {
                visit["sync"] = __barrier__;
            }

            const doc = {
                context,
                visit,
            }

            // Create a database instance and connect to it.
            db = new scribe.Scribe();
            await db.connect();

            // Do the visit
            await visitPage(doc, db);

            // Nothing so far, send a status of 200
            res.sendStatus(200);
            db.markJobWithStatus(original_job_id, "ok");
        } catch (err) {
            console.log("Error occurred during consumer request handling");
            console.log(err);
            res.status(500).send(`Error occurred during consumer request handling: ${err}`).end();
            db.markJobWithStatus(original_job_id, { error: err.toString() });
        } finally {
            // Close the database
            if (db) {
                db.close();
            }
        }
    });
}

function setupReadiness(router) {
    router.get("/healthz", function(req, res) {
        res.sendStatus(200);
    });
}

function setupWebhooks() {
    // Setup express
    const app = express();
    const PORT = parseInt(process.env.WEBHOOK_HTTP_PORT) || 3000;

    // Create a router
    const router = express.Router()

    // Add the json middleware for handling post data
    router.use(express.json());

    // Handler for visitor
    setupKpwEndpoint(router);

    // Readiness probe for KPW
    setupReadiness(router);

    // All kpw requests gets handled by this router
    app.use(router);
    app.listen(PORT, () => console.log(`Listening on ${PORT}`));

}

module.exports.setupWebhooks = setupWebhooks;
