'use strict';

// Node dependencies
const child_process = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { promisify } = require('util');
const { URL } = require('url');

// Third-party dependencies
const glob = require('glob');
const parseDomain = require('parse-domain');
const puppeteer = require('puppeteer-extra');
const rimraf = require('rimraf');
const adb = require('adbkit');
const puppeteerStealthPlugin = require('puppeteer-extra-plugin-stealth');

// Android emulator dependencies
const emulatedDevices = require('puppeteer/DeviceDescriptors');
const PIXEL_2 = emulatedDevices['Pixel 2'];

// Flags for mobile options
const IS_MOBILE = Boolean(process.env.IS_MOBILE);
const IS_PHYSICAL_DEVICE = Boolean(process.env.IS_PHYSICAL_DEVICE);
const MOBILE_PORT = process.env.ADB_DAEMON_PORT || 9222;

// Dimentions for a Pixel 2
const MOBILE_WIDTH = 411;
const MOBILE_HEIGHT = 731;

// Own dependencies
const scribe = require('./scribe');
const watchdog = require('./watchdog');
const kpw = require('./kpw');

// Promisified async functions
const asyncMkdtemp = promisify(fs.mkdtemp);
const asyncGlob = promisify(glob);
const asyncRimraf = promisify(rimraf);

// Path to chromium executable (and tuning options [for testing, not job queuing])
const CHROME_EXE = process.env.CHROME_EXE || '/opt/chromium.org/chromium/chrome';
const CHROME_HEADLESS = !!!JSON.parse(process.env.NOT_HEADLESS || '0');
const CHROME_NO_SANDBOX = !!!JSON.parse(process.env.WITH_SANDBOX || '1');
const CHROME_STEALTHY = Boolean(JSON.parse(process.env.CHROME_STEALTHY || 'false'))

// If this worker is running a stealthy verison of Chrome, make it so.
if (CHROME_STEALTHY) {
    console.log('puppeteer on this worker will be stealthy');
    puppeteer.use(puppeteerStealthPlugin());
}

// Tuning parameters
const PAGE_WATCHDOG_TIMEOUT = parseInt(process.env.PAGE_WATCHDOG_TIMEOUT || '15') * 1000;

// Path to vv8-post-processor executable
const VV8PP_EXE = process.env.VV8PP_EXE || path.join(__dirname, "vv8-post-processor");

// Return contents of bundled determinism.js asset (if present)
function getAssetFile(basename) {
    const dpath = path.join(__dirname, "assets", basename);
    return fs.existsSync(dpath) ? fs.readFileSync(dpath, { encoding: "utf-8" }) : undefined;
}

// Utility function to extract the eTLD from a url
function getCookedUrl(url) {
    try {
        let cookedUrl = new URL(url);
        const { domain, tld } = parseDomain(cookedUrl.hostname);
        cookedUrl.etldPlusOne = `${domain}.${tld}`;
        return cookedUrl;
    } catch (err) {
        // Log the failed attempt silently.
        console.log('Invalid URL attempting to perform basic domain extraction.');
        console.log(err);
    }
    // Should not have come here on success.
    return null;
}

// Utility to spawn a subprocess and wait for it to finish (success or fail) via Promise<void>
class RunningProgram {
    constructor(command, args, options) {
        this._promise = new Promise((resolve, reject) => {
            this._child = child_process.spawn(command, args, options);
            this._alive = true;
            this._child.on('error', (err) => {
                reject(err);
            });
            this._child.on('exit', (code, signal) => {
                // One of the code or the signal will be non-null. If the signal is SIGTERM
                // then we probably sent it and should resolve as we do with exit code 0 
                this._alive = false;
                if (code === 0 || signal === "SIGTERM" || signal === 'SIGINT') {
                    resolve();
                } else {
                    reject(new Error(`runProgram: exit(code=${code}, signal=${signal})`));
                }
            });
        });
    }

    get promise() {
        return this._promise;
    }

    get alive() {
        return this._alive;
    }

    catch (handler) {
        this._promise = this._promise.catch(handler);
        return this._promise;
    }

    kill(signal) {
        this._child.kill(signal);
    }
}

// Use a raw CDT session to create an isolated world in a given frame and inject a script
async function injectNamedScript(cdp, code, url, extra) {
    const {
        frame, // puppeteer Frame object; if given, create isolated world in here
        exeCtx // puppeteer ExecutionContext object; if given, use that isolated world
    } = extra || {};

    let xcid = null;
    if (frame) {
        const { executionContextId } = await cdp.send('Page.createIsolatedWorld', {
            frameId: frame._id, // EEEK implementation detail
            worldName: "fuelInjector",
            grantUniversalAccess: false
        });
        xcid = executionContextId;
    } else if (exeCtx) {
        xcid = exeCtx._contextId; // EEEK implementation detail	
    }

    if (typeof code === "function") {
        code = `(${code.toString()})();`;
    }
    let params = {
        expression: code,
        sourceURL: url,
        persistScript: true,
    };
    if (xcid) {
        params.executionContextId = xcid;
    }
    const { scriptId, exceptionDetails } = await cdp.send('Runtime.compileScript', params);

    if (exceptionDetails) {
        throw exceptionDetails;
    }

    params = {
        scriptId: scriptId,
        silent: true,
        returnByValue: true,
    };
    if (xcid) {
        params.executionContextId = xcid;
    }
    return await cdp.send('Runtime.runScript', params);
}

// MASTER ENTRY POINT: Visit a page, record instrumented artifacts, and return harvested links (via Promise)
async function visitPage(params, db) {
    // Extract from the visit-document our immediately needed parameters
    const {
        visit: {
            url,
            navTimeout,
            pageLoiter,
            proxy: {
                host: proxyHost,
                port: proxyPort = 9050,
            } = {},
            useXvfb,
            userAgent,
            networkConfig: {
                minRTT = 0,
                maxUp = 1000000000, // 1GBps default
                maxDown = 1000000000, // 1GBps default
            } = {}
        },
    } = params;

    // Connect to db (if not provided)
    let ownDb = false;
    if (db === undefined) {
        db = new scribe.Scribe()
        ownDb = true;
        await db.connect();
    }

    // Create scratch directory (to be cleaned up at the end, no matter what)
    let workDir;
    try {
        // Construct the working directory.
        workDir = await asyncMkdtemp(path.join(os.tmpdir(), "vpc"));
        console.log(`Workdir: ${workDir}`)
    } catch (err) {
        // Make sure to release an owned DB here (we're not in the major try/finally yet)
        if (ownDb) {
            await db.close().catch((err) => console.error(`visitPage: db.close() threw '${err}'`));
        }
        throw err;
    }


    // From this point on, we guarantee DB closure and CWD restoration at the end (on success/failure)
    // From this point on, all failures are considered page visit failures
    // (before the visit start event, any failures are just related to infrastructure)
    console.log("visitPage: creating page record, starting process...");
    const log = await db.createVisit(params);
    try {
        // Configure browser options/settings
        const launchOptions = {
            userDataDir: path.join(workDir, "userdata"),
            handleSIGHUP: true, // have pptr tear down Chrome on SIGHUP (we use this in our crawl-level watchdog handler)
            args: []
        };
        if (CHROME_EXE !== "__builtin__") {
            launchOptions.executablePath = CHROME_EXE;
        }
        if (CHROME_NO_SANDBOX) {
            launchOptions.args.push("--no-sandbox");
        }

        // Setup physcial mobile settings
        let adbClient;
        if (IS_MOBILE && IS_PHYSICAL_DEVICE) {
            launchOptions.browserWSEndpoint = 'ws://127.0.0.1:' + MOBILE_PORT + '/devtools/browser';
            launchOptions.defaultViewport = {
                width: MOBILE_WIDTH,
                height: MOBILE_HEIGHT,
                mobile: true,
            };

            adbClient = adb.createClient();
            const devices = await adbClient.listDevices();

            console.log("visitPage: Setting up portforwarding to mobile device...");
            await adbClient.forward(devices[0].id, "tcp:" + MOBILE_PORT, "localabstract:chrome_devtools_remote", function(err) {
                if (err) {
                    console.error(`vistPage: adbClient.forward() threw: '${err}'`);
                }
            });
        }

        // Xvfb makes sense only in non-headless mode
        if (useXvfb) {
            launchOptions.env = { DISPLAY: ":99" };
            launchOptions.headless = false;
        } else {
            launchOptions.headless = CHROME_HEADLESS;
            if (!CHROME_HEADLESS) {
                launchOptions.defaultViewport = false;
            }
        }

        // Handle traffic tunnelling (TCP/DNS via SOCKS5)
        if (proxyHost && proxyPort) {
            launchOptions.args.push(`--proxy-server=socks5://${proxyHost}:${proxyPort}`);
            launchOptions.args.push(`--host-resolver-rules=MAP * ~NOTFOUND , EXCLUDE ${proxyHost}`);
        }
        console.log(launchOptions);

        // Launch the browser (with the work directory as CWD)
        let browser;
        let oldCwd = process.cwd();
        try {
            process.chdir(workDir);
            if (IS_MOBILE && IS_PHYSICAL_DEVICE) {
                console.log("visitPage: Connecting to mobile device...");
                browser = await puppeteer.connect(launchOptions);
                console.log("visitPage: Successfully connected");
            } else {
                browser = await puppeteer.launch(launchOptions);
            }
        } finally {
            process.chdir(oldCwd);
        }
        await log.updatePageStatus("browserLaunched", launchOptions);

        // Open a page, configure UA overrides (if in the spec)

        const page = await browser.newPage();
        if (IS_MOBILE && !IS_PHYSICAL_DEVICE) {
            console.log("Emulating mobile device");
            await page.emulate(PIXEL_2);
        }
        const origUserAgent = await browser.userAgent();
        console.log(`User-Agent (original): ${origUserAgent}`);
        if (userAgent) {
            console.log(`User-Agent (modified): ${userAgent}`);
            await page.setUserAgent(userAgent);
        }
        const cdp = await page.target().createCDPSession();

        // Visit result placeholder
        let res = {};
        let pageOk = false;
        try {
            res = await visitDriver({
                browser,
                log,
                page,
                cdp,
                url,
                navTimeout,
                pageLoiter,
                minRTT,
                maxUp,
                maxDown,
                injectionScript: getAssetFile("deterministic.js"),
            });

            await log.updatePageStatus("postVisitStarted");
            pageOk = true;
        } catch (err) {
            if ('timeout' in err) {
                err = err.timeout ? new Error("PageWatchdogTimeout") : err.err;
            }
            let info = {
                reason: "exception",
                msg: err.toString(),
                stack: err.stack,
            };
            await log.markPageAborted(info);
            console.error("visitPage: aborting because ", err);
        }

        // Tear down browser
        await cdp.detach().catch((err) => console.error(`visitPage: cdp.detch() threw '${err}'`));
        await page.close({ runBeforeUnload: true }).catch((err) => console.error(`visitPage: page.close() threw '${err}'`));

        if (IS_MOBILE && IS_PHYSICAL_DEVICE) {
            await browser.disconnect();
        } else {
            await browser.close().catch((err) => console.error(`visitPage: browser.close() threw '${err}'`));
        }

        // If we were successful, harvest log files and log the page as "complete"
        if (pageOk) {
            const logDoc = await postprocVv8Logs(log, workDir);
            await log.markPageCompleted(logDoc);
        }
        return res;
    } finally {
        await kpw.enqueue({ pageId: log.id }).catch((err) => console.error(`visitPage: kpw-enqueue threw '${err}'`));
        if (ownDb) {
            await db.close().catch((err) => console.error(`visitPage: db.close() threw '${err}'`));
        }
        await asyncRimraf(workDir, { disableGlob: true }).catch((err) => console.error(`visitPage: rimraf('${workDir}', ...) threw '${err}'`));
    }
}

// Handle VV8 log postprocessing if any files are available
async function postprocVv8Logs(log, workDir) {
    const vv8logs = await asyncGlob(`${workDir}/vv8-*.log`).catch((err) => console.error(`visitPage: glob(...) threw '${err}'`));
    const logDoc = {
        logs: 0,
        proc: false,
    };
    if (vv8logs && vv8logs.length) {
        // Run the post-processor (uses our ENV vars to access Mongo) to archive the log file and extract simple feature usage stats
        const ppArgs = ['-archive', '-page-id', log.id, '-aggs', 'ufeatures+Mfeatures', '-output', 'mongresql'].concat(vv8logs);
        console.log(`ppArgs: ${ppArgs}`);
        await new RunningProgram(VV8PP_EXE, ppArgs, { stdio: 'inherit' })
            .catch((err) => {
                console.error(`visitPage/postprocVv8Logs: runProgram(VV8PP_EXE, ...) threw '${err}'`);
                logDoc.procErr = err.toString();
            });
        logDoc.logs = vv8logs.length;
        logDoc.proc = true;
    }
    return logDoc;
}

// Promise-factory for actually driving a visit
function visitDriver(params) {
    // Parse details out of "config" object (no validation--that's handled by experiment())
    const {
        browser, // The browser instance itself
        log, // per-page logger adapter (maintains state of the page-visit)
        page, // puppeteer Page instance for tab we're using to navigate
        cdp, // puppeteer CDPSession instance for our Page (low-level events)
        url, // raw (string) URL to visit
        navTimeout, // time (seconds) to wait for initial page navigation before timeout error
        pageLoiter, // [minimum] time (seconds) to stay on page after successful DomContentLoaded event
        injectionScript, // JS source of script to inject on every document load (before any navigation)
        interactionScript, // JS source of script to inject (once, stealthy) after dom-loaded
        minRTT, // minimum RTT for a network request
        maxUp, // maximum upload speed for the browser performing this visit
        maxDown, // maximum download speed for the browser performing this visit
    } = params;

    // Get parsed domain
    const cookedUrl = getCookedUrl(url);

    return new Promise(function(resolve, reject) {
        const cleanupThunks = [];

        function cleanup() {
            for (const thunk of cleanupThunks) {
                thunk();
            }
        }

        function onCleanup(thunk) {
            cleanupThunks.push(thunk);
        }

        function finish(arg) {
            cleanup();
            resolve(arg);
        }

        function panic(err) {
            cleanup();
            reject(err);
        }

        // Low-level API event handlers (to capture data not surfaced by the Puppeteer high-level API)
        cdp.on('Network.requestWillBeSent', async params => {
            try {
                // Capture the request info and log them
                let info = {
                    requestId: params.requestId,
                    loaderId: params.loaderId,
                    url: params.request.url,
                    documentUrl: params.documentURL,
                    httpMethod: params.request.method,
                    initiatorType: params.initiator.type
                };

                // Initiator details (optional; available for parser/script requests)
                if ('stack' in params.initiator) {
                    const cf = params.initiator.stack.callFrames[0];
                    info.initiator = {
                        url: cf.url,
                        line: cf.lineNumber,
                        col: cf.columnNumber,
                    };
                } else if (('url' in params.initiator) || ('lineNumber' in params.initiator)) {
                    info.initiator = {
                        url: params.initiator.url,
                        line: params.initiator.lineNumber,
                    };
                }

                // Type and frame ID of the request to be sent (optional)
                if (params.type) {
                    info.resourceType = params.type;
                }
                if (params.frameId) {
                    info.frameId = params.frameId;
                }
                await log.runRequestWillBeSent(info);
            } catch (err) {

                console.log(`Error encountered during network request will be sent interceptor.`);
                console.log(err);
            }
        });
        cdp.on('Debugger.scriptParsed', async params => {
            try {
                const { scriptId, url, stackTrace = [] } = params;
                let result = await cdp.send('Debugger.getScriptSource', { scriptId });
                await log.runScriptParsed(url, result.scriptSource, stackTrace);
            } catch (err) {
                console.log(`Error encountered during debugger script parsing.`);
                console.log(err);
            }
        });
        const frameMap = new Map();
        cdp.on('Page.frameAttached', params => {
            // Check if already entry in frame map
            let frameObj = Object.create(null);
            if (frameMap.has(params.frameId)) {
                Object.assign(frameObj, frameMap.get(params.frameId));
            }

            // Fill up the properties for the attached event.
            // Frame can be attached once, save the parent frame being attached to.
            frameObj.frameId = params.frameId;
            frameObj.parentFrameId = params.parentFrameId;

            let frameEventObj = { type: 'attached' };
            frameEventObj.parentFrameId = params.parentFrameId;
            if (params.stack) {
                frameEventObj.stack = params.stack;
            }
            frameEventObj.when = new Date();

            // Check if this is the first frame event for this frame.
            if (frameObj.frameEvents == null) {
                frameObj.frameEvents = [];
            }
            frameObj.frameEvents.push(frameEventObj);

            frameMap.set(params.frameId, frameObj);
        });
        cdp.on('Page.frameNavigated', async params => {
            // Check if already entry in frame map
            let frameObj = Object.create(null);
            if (frameMap.has(params.frame.id)) {
                Object.assign(frameObj, frameMap.get(params.frame.id));
            }
            frameObj.frameId = params.frame.id;
            // Check if this is the first frame event for this frame.
            if (frameObj.frameEvents == null) {
                frameObj.frameEvents = [];
            }
            frameObj.frameEvents.push({
                type: 'navigation',
                url: params.frame.url,
                loaderId: params.frame.loaderId,
                securityOrigin: params.frame.securityOrigin,
                when: new Date()
            });

            frameMap.set(params.frame.id, frameObj);
        });

        // High-level API event handlers
        // Dangling tabs (targets) culling overlord
        browser.on('targetcreated', async target => {
            try {
                // Save into the database for posterity.
                await log.runTargetSquashed(target.url());
                if (target !== page.target()) {
                    let targetPage = await target.page();
                    if (targetPage) {
                        await targetPage.close({ runBeforeUnload: true });
                    }
                }
                console.log(`Target with url ${target.url()} closed at ${new Date()}`);
            } catch (err) {
                console.log(`Error encountered during target squashing.`);
                console.log(err);
            }
        });
        page.on('error', panic); // Page crashed, we should panic here.
        page.on('dialog', dialog => {
            console.log(`Dismissing ${dialog.type()} dialog ("${dialog.message()}")`);
            dialog.dismiss().catch((err) => {
                console.log(`Error encountered during page dialog dismissal.`);
                console.log(err);
            });
        });
        let firstMainFrameRequest = null;
        page.on('request', async request => {
            try {
                let aborted = false;
                // Is navigation request?
                if (request.isNavigationRequest()) {
                    // From main frame?
                    if (request.frame() === page.mainFrame()) {
                        // Only allow when this the first main frame request (aka redirection on landing)
                        if (firstMainFrameRequest === null) {
                            console.log(`Allowing [initial] navigation request for main frame to ${request.url()}`);
                            firstMainFrameRequest = request;
                        } else {
                            // Or the redirection is within the top-level domain of the first main frame request. 
                            const chain = request.redirectChain();
                            if (chain.length && (chain[0] == firstMainFrameRequest)) {
                                const cookedRequestUrl = getCookedUrl(request.url());
                                if (cookedRequestUrl.etldPlusOne !== cookedUrl.etldPlusOne) {
                                    console.log(`Aborting navigation redirection for main frame to ${request.url()} (DEAD END)`);
                                    await request.abort('aborted');
                                    aborted = true; // Should not be used, but safety!!
                                    finish({
                                        caveat: {
                                            type: "deadend",
                                            info: request.url()
                                        },
                                    });
                                    return;
                                } else {
                                    console.log(`Allowing [redirected] navigation request for main frame to ${request.url()}`);
                                }
                            } else {
                                console.log(`Aborting navigation request from main frame to ${request.url()}`);
                                await request.abort('aborted');
                                aborted = true;
                            }
                        }
                        // Fall-through: if we haven't aborted/returned, log this as a visit
                        await log.runVisit(request.url());
                    }
                }
                // Have we already handled this request (through aborting)?
                if (aborted === false) {
                    // Nope, continue the request as if nothing happened
                    await request.continue();
                }
            } catch (err) {
                console.log(`Error encountered during page request.`);
                console.log(err);
            }
        });
        page.on('requestfinished', async request => {
            try {
                const response = request.response();
                const pBody = (response && response.ok()) ? response.buffer() : Promise.resolve(null);

                const body = await pBody.catch(error => {
                    console.error("visitPage: getResponseBody threw ", error);
                    return null; // missing body in this case
                });
                const meta = {
                    headers: Object.entries(request.headers()),
                    method: request.method(),
                    navigationRequest: request.isNavigationRequest(),
                    resourceType: request.resourceType(),
                    response: {
                        status: response.status(),
                        fromCache: response.fromCache(),
                        headers: Object.entries(response.headers()),
                        remoteAddress: response.remoteAddress(),
                    },
                };
                const details = response.securityDetails();
                if (details) {
                    meta.response.securityDetails = {
                        protocol: details.protocol(),
                        issuer: details.issuer(),
                        subjectName: details.subjectName(),
                        validRange: [details.validFrom(), details.validTo()]
                    };
                }
                const chain = request.redirectChain();
                if (chain.length > 0) {
                    meta.redirectChain = chain.map(link => ({
                        url: link.url(),
                        status: link.response().status(),
                    })).filter(jsonLink => jsonLink.url !== request.url());
                }

                await log.runRequestResponse(request.url(), request._requestId, meta, body);
            } catch (err) {
                console.log(`Error encountered during page request finish handling.`);
                console.log(err);
            }
        });
        page.on('requestfailed', async request => {
            try {
                const meta = {
                    method: request.method(),
                    navigationRequest: request.isNavigationRequest(),
                    resourceType: request.resourceType(),
                    headers: Object.entries(request.headers()),
                    failure: request.failure().errorText
                };
                const chain = request.redirectChain();
                if (chain.length > 0) {
                    meta.redirectChain = chain.map(link => ({
                        url: link.url(),
                        status: link.response().status(),
                    })).filter(jsonLink => jsonLink.url !== request.url());
                }
                await log.runRequestFailure(request.url(), request._requestId, meta);
            } catch (err) {
                console.log(`Error encountered during page request failure handling.`);
                console.log(err);
            }
        });
        page.on('console', async msg => {
            try {
                if (msg.type() === 'error' || msg.type() === 'warning') {
                    await log.runConsoleError(msg.type(), msg.text());
                }
            } catch (err) {
                console.log(`Error encountered during page console error handling.`);
                console.log(err);
            }
        });
        page.on('pageerror', async err => {
            try {
                await log.runConsoleError("uncaughtException", err.toString(), err.stack);
            } catch (err) {
                console.log(`Error encountered during page uncaught exception handling.`);
                console.log(err);
            }
        });
        page.on('framenavigated', async frame => {
            // Frame navigation completed, check if this is the main frame.
            try {
                if (frame === page.mainFrame()) {
                    // Check if already entry in frame map
                    let frameObj = Object.create(null);
                    if (frameMap.has(frame._id)) {
                        Object.assign(frameObj, frameMap.get(frame._id));
                    }
                    frameObj.frameId = frame._id;
                    frameObj.mainFrame = true;
                    frameMap.set(frame._id, frameObj);

                    await log.updatePageMainFrameNavTime(Date.now() - navStartTime);
                }
            } catch (err) {
                console.log(`Error encountered during page frame navigation handling.`);
                console.log(err);
            }
        });
        page.on('framedetached', frame => {
            // Check if already entry in frame map
            let frameObj = Object.create(null);
            if (frameMap.has(frame._id)) {
                Object.assign(frameObj, frameMap.get(frame._id));
            }
            frameObj.frameId = frame._id;
            // Check if this is the first frame event for this frame.
            if (frameObj.frameEvents == null) {
                frameObj.frameEvents = [];
            }
            frameObj.frameEvents.push({
                type: 'detached',
                when: new Date()
            });

            frameMap.set(frame._id, frameObj);
        });
        page.on('load', async() => {
            try {
                await log.runPageLoad(Date.now() - navStartTime);
            } catch (err) {
                console.log(`Error encountered during page load event handling.`);
                console.log(err);
            }
        });
        page.on('close', () => {
            // Page closed abruptly, we should panic!
            panic(new Error("page closed!"));
        });

        // Storage for beginning time of the frame loading
        let navStartTime = null;
        // First complete all setup calls (in any order)...
        Promise.all([
            cdp.send('Runtime.enable'),
            cdp.send('Debugger.enable'),
            cdp.send('Network.enable'),
            cdp.send('Network.emulateNetworkConditions', {
                offline: false,
                latency: minRTT,
                downloadThroughput: maxDown,
                uploadThroughput: maxUp,
            }),
            cdp.send('Page.enable'),
            page.setRequestInterception(true)
        ]).then(() => {
            if (cookedUrl) {
                // Save the parsed URL for analytical reasons
                return log.updatePageParsedUrl(cookedUrl);
            }
            // Otherwise resolve immediately
            return Promise.resolve();
        }).then(() => {
            // Inject a "job tracer" script into this frame (before any navigation)
            // This allows the post-processor to associate the resulting VV8 log with this job
            return injectNamedScript(cdp, function tracer() {
                console.log(window.origin);
            }, `visible-v8://${log.id}/id.js`);
        }).then(() => {
            // Check if the injection script (from controlling on page behavior eg. random number 
            // generation or timestamp retrieval) is provided
            if (injectionScript) {
                // If so, evaluate the script upon page navigation or child frame attach/navigation
                const patchedScript = injectionScript.replace("{{WPR_TIME_SEED_TIMESTAMP}}", new Date().valueOf().toString());
                return page.evaluateOnNewDocument(patchedScript);
            }
            // Otherwise, resolve immediately
            return Promise.resolve();
        }).then(() => {
            return log.updatePageStatus('preVisitCompleted');
        }).then(() => {
            // Mark the time
            navStartTime = Date.now();
            // Start navigation (return promise)
            return page.goto(url, {
                timeout: navTimeout * 1000,
                waitUntil: 'domcontentloaded'
            });
        }).then(() => {
            return log.updatePageStatus('navigationCompleted');
        }).then(() => {
            // Start visit timer (to trigger no-caveat "finish" on timeout)
            const startTime = Date.now();
            const token = setTimeout(() => {
                console.log(`loiter-timeout fired at ${new Date()}`);
                const teardownPromise = Promise.all([
                    log.saveFramesBulk(frameMap),
                    page.content().then((content) => {
                        console.log(`got page contents back at ${new Date()}`);
                        return log.updatePageMainFrameContent(url, content);
                    }),
                    page.screenshot().then((content) => {
                        console.log(`got screen-shot data at ${new Date()}`);
                        return log.updatePageScreenshot(url, content);
                    }),
                ]);

                // Wrap tear-down in a T second watchdog
                console.log(`installing ${PAGE_WATCHDOG_TIMEOUT}ms watchdog timeout...`);
                watchdog(teardownPromise, PAGE_WATCHDOG_TIMEOUT).then(() => {
                    console.log(`ALL DONE, logging page completion at ${new Date()}`);
                    finish({
                        timeOnTarget: (Date.now() - startTime) / 1000.0
                    });
                }).catch(panic);
            }, pageLoiter * 1000);
            onCleanup(() => clearTimeout(token));

            // Check if any interaction script is provided
            if (interactionScript) {
                // Otherwise inject the interaction script into the main frame
                return injectNamedScript(cdp, interactionScript,
                    `visible-v8://${log.id}/interact.js`, {
                        frame: page.mainFrame()
                    });
            }
            //  If not provided resolve immediately
            return Promise.resolve();
        }).then(() => {
            return log.updatePageStatus('loiterStarted');
        }).catch(panic);
    });
}

module.exports = {
    visitPage,
    getCookedUrl
};