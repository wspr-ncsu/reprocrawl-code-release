#!/usr/bin/env node

'use strict';

if (process.env.NODE_ENV === 'production') {
    require('dotenv').config();
}
if (process.env.WHY_IS_NODE_RUNNING) {
    const why = require('why-is-node-running');
    process.on('SIGUSR1', () => {
        why();
    });
    console.log(`why-is-node-running: kill -USR1 ${process.pid}`);
}

// Project imports
const scribe = require('./lib/scribe');
const { visitPage } = require('./lib/visitor');
const webhooks = require('./lib/webhooks')

// Tuning parameters
const DEFAULT_NAV_TIMEOUT = 45.0;
const DEFAULT_LOITER_TIME = 15.0;

// VantagePoint (NAME[@SOCKS_HOST[:SOCKS_PORT]]) parsing regex: [1] -> name, [2] -> socksHost, [3] -> socksPort
const VP_PARSE_PATTERN = /([^@]+)(?:@([^:]+)(?::(\d+))?)?/;

// For swallowing (err, logging to stderr) promise-chain errors in cases where there's nothing more to do
const CATCH_ALL = console.error.bind(console);

// CLI entry point
function main() {
    // Do-nothing SIGHUP handler (to handle Chrome-start/watchdog-timeout races that could unintentionally kill us)
    process.on('SIGHUP', _ => {
        console.warn(`Got SIGHUP @ ${new Date()}`);
    });

    const program = require('commander');
    program
        .version('1.0.0');
    program
        .command("webhook")
        .description("set up crawler webhooks for interacting with KPW")
        .action(webhooks.setupWebhooks);
    program
        .command("visit <URL>")
        .description("Visit the given URL, creating a page record and collecting all data")
        .option('-t, --navTimeout <T>', 'Time navigation out after T seconds', /\d+(\.\d+)?/, DEFAULT_NAV_TIMEOUT)
        .option('-l, --pageLoiter <T>', 'Loiter T seconds after DOM ready state', /\d+(\.\d+)?/, DEFAULT_LOITER_TIME)
        .option("-v, --vantagePoint <VP>", "Use VP (NAME[@SOCKS_HOST[:SOCKS_PORT]]) to crawl domain", VP_PARSE_PATTERN, "local")
        .option("-x, --useXvfb", "Spawn/use Xvfb to run Chrome in non-headless mode, headlessly. This requires xvfb to be already running on :99")
        .option("-u, --userAgent <STRING>", "Override chrome's default User-Agent string with STRING")
        .action(async function(url) {
            // Socks proxy setup
            let crawlProxy = undefined;
            const [_, vantagePoint, socksHost, socksPort] = VP_PARSE_PATTERN.exec(this.vantagePoint);
            if (socksHost) {
                crawlProxy = { host: socksHost };
                if (socksPort) { crawlProxy.port = socksPort; }
            }

            const doc = {
                context: {
                    experiment: this.experiment,
                    vantagePoint: vantagePoint,
                    rootDomain: url,
                },
                visit: {
                    url: url,
                    seed: this.syncData,
                    navTimeout: this.navTimeout,
                    pageLoiter: this.pageLoiter,
                    useXvfb: this.useXvfb,
                    userAgent: this.userAgent,
                    proxy: crawlProxy,
                },
            };

            const db = new scribe.Scribe();
            await db.connect();

            try {
                await visitPage(doc, db);
            } catch (e) {
                console.error(e);
            } finally {
                db.close();
            }
        });
    program.parse(process.argv);
}

main();
