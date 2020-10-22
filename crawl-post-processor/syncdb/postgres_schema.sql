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
    id SERIAL PRIMARY KEY,              -- PG ID for FKs from other tables
    mongo_oid BYTEA UNIQUE NOT NULL,    -- Mongo OID of original record (prevent duplication)
    imported TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- when we imported this record

    visit_url_id INT REFERENCES urls(id) NOT NULL,   -- The planned/scheduled visit URL
                                -- TODO: the final visited URL (requires index on events/lookup of latest "visit" event for this page)
    nav_time_ms INT,            -- Time taken to commit the main document frame to the final navigation (milliseconds)
    fetch_time_ms INT,          -- Time taken until initial domcontent is downloaded
    load_time_ms INT,           -- Total time taken to "load" the main document (nav time and load/startup time)

    final_content_sha256 BYTEA, -- SHA256 hash of final-DOM-content-blob (if available)
    final_content_size INT,     -- Size (in bytes) of final-DOM-content-blob (if available)
    final_content_lang TEXT,    -- <html lang="..."> attribute from final-DOM-content-blob (if available/found)
    screenshot_sha256 BYTEA,    -- SHA256 hash of final-state-screenshot-PNG-blob (if available)
    screenshot_size INT,        -- Size (in bytes) of final-state-screenshot-PNG-blob (if available)

    -- Crawl status and milestone timestamps
    status_state TEXT NOT NULL,             -- Final status (created -> browserLaunched -> preVisitStarted -> navigationCompleted -> gremlinInteractionStarted -> postVisitStarted -> completed / aborted)
    status_abort_msg TEXT,                  -- Abort info.msg (if any; NULL if not an abort)
    status_created TIMESTAMPTZ NOT NULL,    -- Time of "created" milestone
    -- TODO: other milestone timestamps needed?
    status_ended TIMESTAMPTZ NOT NULL      -- Time of final milestone (whatever it was [completed, aborted, etc.])
);

-- Import-version of the pages table (used for bulk imports and stitching up FKs)
CREATE TABLE IF NOT EXISTS pages_import_schema (
    mongo_oid BYTEA PRIMARY KEY,
    position TEXT NOT NULL,
    visit_url_sha256 BYTEA NOT NULL,
    nav_time_ms INT,
    fetch_time_ms INT,
    load_time_ms INT,
    final_content_sha256 BYTEA,
    final_content_size INT,
    screenshot_sha256 BYTEA,
    screenshot_size INT,
    status_state TEXT NOT NULL,
    status_abort_msg TEXT,
    status_created TIMESTAMPTZ NOT NULL,
    status_browser_ready TIMESTAMPTZ,
	status_nav_complete TIMESTAMPTZ,
	status_gremlins_launched TIMESTAMPTZ,
    status_ended TIMESTAMPTZ NOT NULL
);

-- Record of each `frames` document
CREATE TABLE IF NOT EXISTS frames (
    id SERIAL PRIMARY KEY,
    mongo_oid BYTEA UNIQUE NOT NULL,
    imported TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    page_id INT REFERENCES pages(id),           -- Page visit on which this frame was encountered
    parent_id INT REFERENCES frames(id),        -- [Optional] parent frame (based on last-attached parent UUID)
    token TEXT UNIQUE NOT NULL,                 -- UUID-ish identifier for this frame
    is_main BOOLEAN NOT NULL DEFAULT FALSE      -- Was this the main frame?
);

-- Import schema for temp working table
CREATE TABLE IF NOT EXISTS frames_import_schema (
    mongo_oid BYTEA PRIMARY KEY,
    page_mongo_oid BYTEA NOT NULL,
    token TEXT NOT NULL,
    parent_token TEXT NOT NULL,
    is_main BOOLEAN NOT NULL
);

-- Record of each "requestWillBeSent" page `event`
CREATE TABLE IF NOT EXISTS request_inits (
    id SERIAL PRIMARY KEY,
    mongo_oid BYTEA UNIQUE NOT NULL,
    imported TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    page_id INT REFERENCES pages(id),       -- Page visit [technically redundant w.r.t. frame_id, but handy]
    frame_id INT REFERENCES frames(id),     -- Frame that issued this request (if known)
    
    document_url_id INT REFERENCES urls(id) NOT NULL,             -- Document URL loaded into source frame at time of request (i.e., SOP)
    http_method TEXT NOT NULL,              -- HTTP method invoked (GET, etc.)
    request_url_id INT REFERENCES urls(id) NOT NULL,              -- URL requested
    resource_type TEXT NOT NULL,            -- CDT-specific classification of what kind of resource is being loaded (document, script, etc.)
    initiator_type TEXT NOT NULL,           -- CDT-specific classification of what kind of agent triggered the request
    logged_when TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS request_inits_import_schema (
    mongo_oid BYTEA PRIMARY KEY,
    page_mongo_oid BYTEA NOT NULL,
    frame_token TEXT NOT NULL,
    document_url_sha256 BYTEA NOT NULL,
    http_method TEXT NOT NULL,
    request_url_sha256 BYTEA NOT NULL,
    resource_type TEXT NOT NULL,
    initiator_type TEXT NOT NULL,
    logged_when TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS request_responses (
    id SERIAL PRIMARY KEY,
    mongo_oid BYTEA UNIQUE NOT NULL,
    imported TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    page_id INT
        REFERENCES pages(id),               -- Page visit FK
    resource_type TEXT,                     -- Chrome's classification of resource-type (e.g., image)
    request_url_id INT
        REFERENCES urls(id) NOT NULL,       -- URL requested
    request_method TEXT NOT NULL,           -- HTTP method (GET, etc.)
    request_headers JSONB,                  -- HTTP request headers collection (JSON object)
    redirect_chain JSONB,                   -- optional array of URLs/statuses we redirected through to get here
    
    response_from_cache BOOLEAN NOT NULL,   -- Was the response from local cache?
    response_status INT NOT NULL,           -- HTTP response code (200, etc.)
    response_headers JSONB,                 -- HTTP response headers collection (JSON object)
    response_body_sha256 BYTEA,             -- SHA256 hash of response body (if available)
    
    server_ip INET,                         -- Remote server IP
    server_port INT,                        -- Remote server port
    protocol TEXT,                          -- HTTP protocol/version
    security_details JSONB,                 -- TLS details, if available

    logged_when TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS request_responses_import_schema (
    mongo_oid BYTEA PRIMARY KEY,
    page_mongo_oid BYTEA NOT NULL,
    resource_type TEXT,
    request_url_sha256 BYTEA NOT NULL,
    request_method TEXT NOT NULL,
    request_headers TEXT,
    redirect_chain TEXT,
    response_from_cache BOOLEAN NOT NULL,
    response_status INT NOT NULL,
    response_headers TEXT,
    response_body_sha256 BYTEA,
    server_ip INET,
    server_port INT,
    protocol TEXT,
    security_details TEXT,
    logged_when TIMESTAMPTZ NOT NULL
);

-- Record of each "requestFailure" page `event`
CREATE TABLE IF NOT EXISTS request_failures (
    id SERIAL PRIMARY KEY,
    mongo_oid BYTEA UNIQUE NOT NULL,
    imported TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    page_id INT
        REFERENCES pages(id),               -- Page visit FK
    resource_type TEXT,                     -- Chrome's classification of resource-type (e.g., image)
    request_url_id INT
        REFERENCES urls(id) NOT NULL,       -- URL requested
    request_method TEXT NOT NULL,           -- HTTP method (GET, etc.)
    request_headers JSONB,                  -- HTTP request headers collection (JSON object)
    failure TEXT NOT NULL,                  -- Why did this fail (according to Chrome)?

    logged_when TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS request_failures_import_schema (
    mongo_oid BYTEA PRIMARY KEY,
    page_mongo_oid BYTEA NOT NULL,
    resource_type TEXT,
    request_url_sha256 BYTEA NOT NULL,
    request_method TEXT NOT NULL,
    request_headers TEXT,
    failure TEXT NOT NULL,
    logged_when TIMESTAMPTZ NOT NULL
);

-- Record of each "scriptParsed" page `event`
CREATE TABLE IF NOT EXISTS parsed_scripts (
    id SERIAL PRIMARY KEY,
    mongo_oid BYTEA UNIQUE NOT NULL,
    imported TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    page_id INT
        REFERENCES pages(id),               -- Page visit FK
    script_url_id INT
        REFERENCES urls(id),                -- Source of script (if available)
    script_hash BYTEA,                      -- SHA256 of sript body (if available)
    logged_when TIMESTAMPTZ NOT NULL        -- Event's "date"
);
CREATE TABLE IF NOT EXISTS parsed_scripts_import_schema (
    mongo_oid BYTEA PRIMARY KEY,
    page_mongo_oid BYTEA NOT NULL,
    script_url_sha256 BYTEA,
    script_hash BYTEA,
    logged_when TIMESTAMPTZ NOT NULL
);

-- Record of each "targetSquashed" page `event`
CREATE TABLE IF NOT EXISTS squashed_targets (
    id SERIAL PRIMARY KEY,
    mongo_oid BYTEA UNIQUE NOT NULL,
    imported TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    page_id INT
        REFERENCES pages(id),           -- Page visit FK
    target_url_id INT
        REFERENCES urls(id),            -- Target dest/doc
    logged_when TIMESTAMPTZ NOT NULL    -- Event's "date"
);
CREATE TABLE IF NOT EXISTS squashed_targets_import_schema (
    mongo_oid BYTEA PRIMARY KEY,
    page_mongo_oid BYTEA NOT NULL,
    target_url_sha256 BYTEA,
    logged_when TIMESTAMPTZ NOT NULL
);

-- Record of aggregate-console-error counts per page
CREATE TABLE IF NOT EXISTS console_errors (
    id SERIAL PRIMARY KEY,
    imported TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    page_id INT UNIQUE REFERENCES pages(id),-- Page visit FK (unique!!)
    total INT NOT NULL,                     -- Sum of all console categories
    categories JSONB,                       -- Per-category counts in JSON form
    last_when TIMESTAMPTZ NOT NULL          -- Latest timestamp of any of the counted console messages
);

CREATE TABLE IF NOT EXISTS console_errors_import_schema (
    page_mongo_oid BYTEA PRIMARY KEY,
    total INT NOT NULL,
    categories TEXT NOT NULL,
    last_when TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS visit_chains (
    id SERIAL PRIMARY KEY,
    page_id INT REFERENCES pages(id),                           -- Which page record from the original this maps to
    imported TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,    -- When we imported this record
    visit_links TEXT[],                                         -- URLs in the chain (from first request [1] to final visit [n])
    logged_when TIMESTAMPTZ NOT NULL                            -- Timestamp of the last link in the visit chain
);

CREATE TABLE IF NOT EXISTS visit_chains_import_schema (
    page_mongo_oid BYTEA UNIQUE NOT NULL,                       -- Which page record from the original this maps to
    visit_links TEXT[],                                         -- URLs in the chain (from first request [1] to final visit [n])
    logged_when TIMESTAMPTZ NOT NULL                            -- Timestamp of the last link in the visit chain
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