-- Canonical table of (parsed, stemmed) URLs
CREATE TABLE IF NOT EXISTS urls (
    id SERIAL PRIMARY KEY,          -- Official ID (used for FKs)
    sha256 BYTEA UNIQUE NOT NULL,   -- SHA256(full), used for dedupe on insert
    url_full TEXT NOT NULL,         -- Full text of URL
    url_scheme TEXT,                -- Parsed field: scheme (e.g., "https")
    url_hostname TEXT,              -- Parsed field: hostname (e.g., "www.example.com")
    url_port TEXT,                  -- Parsed field: port (e.g., "8080")
    url_path TEXT,                  -- Parsed field: path (e.g., "/index.html")
    url_query TEXT,                 -- Parsed field: query-string (e.g., "?admin=1&q=get+cookies")
    url_etld1 TEXT,                 -- Synthetic field: eTLD+1(hostname) (e.g., "example.com")
    url_stemmed TEXT                -- Synthetic field: concatenation of url_etld1 and url_path (simplified form for grouping)
);

CREATE TABLE IF NOT EXISTS urls_import_schema (
    sha256 BYTEA PRIMARY KEY,
    url_full TEXT NOT NULL,
    url_scheme TEXT,
    url_hostname TEXT,
    url_port TEXT,
    url_path TEXT,
    url_query TEXT,
    url_etld1 TEXT,
    url_stemmed TEXT
);

-- Record of each page visit
CREATE TABLE IF NOT EXISTS pages (
    id SERIAL PRIMARY KEY,                                      -- PG ID for FKs from other tables
    mongo_oid BYTEA UNIQUE NOT NULL,                            -- Mongo OID of original record (prevent duplication)
    imported TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,    -- when we imported this record

    domain TEXT NOT NULL,           -- Alexa domain name seeding this page visit
    alexa_rank INT NOT NULL,        -- Alexa rank of this domain name
    vantage_point TEXT NOT NULL,    -- Text tag identifying network vantage point
    browser_config TEXT NOT NULL,   -- Text tag identifying set of browser configuration options used
    rep INT NOT NULL,               -- Repetition count (1, 2, ...) of a series of identical page visits

    visit_url_id INT REFERENCES urls(id) NOT NULL,  -- The planned/scheduled visit URL
    final_url_id INT REFERENCES urls(id),           -- The final visited URL (if successful, after any tracked redirects)

    sync_time_ms INT,           -- Time spend waiting for barrier sync, if any (milleseconds)
    nav_time_ms INT,            -- Time taken to commit the main document frame to the final navigation (milliseconds)
    fetch_time_ms INT,          -- Time taken until initial domcontent is downloaded
    load_time_ms INT,           -- Total time taken to the browser "load" event for the main document (nav time and load/startup time)
    first_content_sha256 BYTEA, -- SHA256 hash of the initial-HTML-document-content-blob (if available)
    first_content_size INT,     -- Size (in bytes) of initial-HTML-document-content-blob (if available)
    final_content_sha256 BYTEA, -- SHA256 hash of final-DOM-content-blob (if available)
    final_content_size INT,     -- Size (in bytes) of final-DOM-content-blob (if available)
    final_content_lang TEXT,    -- <html lang="..."> attribute from final-DOM-content-blob (if available/found)
    screenshot_sha256 BYTEA,    -- SHA256 hash of final-state-screenshot-PNG-blob (if available)
    screenshot_size INT,        -- Size (in bytes) of final-state-screenshot-PNG-blob (if available)

    -- Crawl status and milestone timestamps
    status_state TEXT NOT NULL,             -- Final status (created -> browserLaunched -> preVisitStarted -> navigationCompleted -> gremlinInteractionStarted -> postVisitStarted -> completed / aborted)
    status_abort_msg TEXT,                  -- Abort info.msg (if any; NULL if not an abort)
    status_created TIMESTAMPTZ NOT NULL,    -- Time of "created" milestone
    status_ended TIMESTAMPTZ NOT NULL,      -- Time of final milestone (whatever it was [completed, aborted, etc.])

    original_record JSONB                   -- raw dump of the Mongo BSON structure (for odd-ball analytics we didn't think of before postproc)
);


CREATE TABLE IF NOT EXISTS frame_loaders (
    id SERIAL PRIMARY KEY,
    page_id INT REFERENCES pages(id) NOT NULL,
    
    frame_id TEXT NOT NULL,
    loader_id TEXT NOT NULL,
    parent_frame_id TEXT,
    is_main BOOLEAN NOT NULL,
    
    security_origin_url_id INT REFERENCES urls(id) NOT NULL,
    navigation_url_id INT REFERENCES urls(id),
    attachment_script JSONB,
    since_when TIMESTAMPTZ NOT NULL,
    UNIQUE (frame_id, loader_id)
);

CREATE TABLE IF NOT EXISTS frame_loaders_import_schema (
    page_oid BYTEA NOT NULL,
    frame_id TEXT NOT NULL,
    loader_id TEXT NOT NULL,
    parent_frame_id TEXT,
    is_main BOOLEAN NOT NULL,
    
    security_origin_url_sha256 BYTEA NOT NULL,
    navigation_url_sha256 BYTEA,
    attachment_script TEXT,
    since_when TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (frame_id, loader_id)
);



-- Record of each request queued [and maybe succeeded/failed] during a page request
CREATE TABLE IF NOT EXISTS requests (
    id SERIAL PRIMARY KEY,
    page_id INT REFERENCES pages(id) NOT NULL,          -- Obviously, what page visit this was on
    frame_loader_id INT REFERENCES frame_loaders(id),   -- Where (what frame/parent/loader/securityOrigin) this was requested from
    unique_id TEXT NOT NULL,                            -- Chrome's internal unique-ish (per page) identify for this request (across all attempts/redirects)
    
    resource_type TEXT,
    initiator_type TEXT,                        -- Type name of load (parser, script, other)
    initiator_url_id INT REFERENCES urls(id),   -- Optional URL of resource that loaded it (parser/script only)
    initiator_line INT,                         -- Optional line-offset (0-based) in resource that loaded it (parser/script only)

    first_request_url_id INT REFERENCES urls(id),       -- Initial HTTP request URL (cooked)
    redirect_chain JSONB,                               -- If any redirects were encountered, list their URLs/status's (not including the final URL) here
    final_request_url_id INT REFERENCES urls(id),       -- Final HTTP request URL (cooked)
    http_method TEXT,                                   -- Final HTTP request method
    request_headers JSONB,                              -- Final HTTP request headers collection (JSON object)
    
    response_received BOOLEAN NOT NULL DEFAULT FALSE,   -- Was a response (ANY status, including 400/500/etc.) received?
    response_failure TEXT,                              -- If no response was received, why did this fail (according to Chrome)?
    
    response_from_cache BOOLEAN,    -- Was the response from local cache? (this, and all below, are success-only)
    response_status INT,            -- Final HTTP response code (200, etc.)
    response_headers JSONB,         -- Final HTTP response headers collection (JSON object)
    response_body_sha256 BYTEA,     -- SHA256 hash of final response body (if available)
    response_body_size INT,         -- Size of final response body in bytes (if available)
    server_ip INET,                 -- Final request remote server IP
    server_port INT,                -- Final request remote server port
    protocol TEXT,                  -- Final request HTTP protocol/version
    security_details JSONB,         -- Final request TLS details, if available

    when_queued TIMESTAMPTZ,    -- Timestamp of when the request was queued (requestWillBeSent)
    when_replied TIMESTAMPTZ    -- Timestamp of the response/failure (if any)
);

CREATE TABLE IF NOT EXISTS requests_import_schema (
    unique_id TEXT PRIMARY KEY,
    page_oid BYTEA NOT NULL,
    frame_id TEXT,
    loader_id TEXT,
    resource_type TEXT,
    initiator_type TEXT,
    initiator_url_sha256 BYTEA,
    initiator_line INT,
    first_request_url_sha256 BYTEA,
    redirect_chain TEXT,
    final_request_url_sha256 BYTEA,
    http_method TEXT,
    request_headers TEXT,
    response_received BOOLEAN NOT NULL DEFAULT FALSE,
    response_failure TEXT,
    response_from_cache BOOLEAN,
    response_status INT,
    response_headers TEXT,
    response_body_sha256 BYTEA,
    response_body_size INT,
    server_ip INET,
    server_port INT,
    protocol TEXT,
    security_details TEXT,
    when_queued TIMESTAMPTZ,
    when_replied TIMESTAMPTZ
);


CREATE TABLE IF NOT EXISTS js_api_usage (
    id SERIAL PRIMARY KEY,
    page_id INT REFERENCES pages(id),
    imported TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    mongo_oid BYTEA NOT NULL,
    origin_url_id INT REFERENCES urls(id) NOT NULL,
    js_apis TEXT[],
    logged_when TIMESTAMPTZ NOT NULL,
    UNIQUE(mongo_oid, origin_url_id)
);

CREATE TABLE IF NOT EXISTS js_api_usage_import_schema (
    mongo_oid BYTEA NOT NULL,
    origin_url_sha256 BYTEA NOT NULL,
    page_mongo_oid BYTEA NOT NULL,
    js_apis TEXT[],
    logged_when TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (mongo_oid, origin_url_sha256)
);