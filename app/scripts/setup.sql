-- ============================================================================
-- Anakin API for Snowflake — setup.sql
--
-- Runs once when the consumer CREATEs (or upgrades) the application, per
-- docs.snowflake.com/en/developer-guide/native-apps/native-apps-about
-- ("Snowflake creates the application and runs the setup script to create
-- the required objects within the application").
--
-- Wraps 18 of Anakin's 21 REST API operations (base URL
-- https://api.anakin.io/v1, auth via X-API-Key). Ground truth for the
-- original 3 (url-scraper, search, agentic-search) is anakin-py's
-- src/anakin/client.py / _http.py, the official Python SDK built earlier
-- this session. Ground truth for the 15 added afterwards — map, crawl, the
-- 5 Wire operations, the 2 AI Visibility operations, the 2 browser-session
-- operations, and the 4 website-monitoring operations — is anakin-mcp's own
-- source, src/client.ts + src/tools/*.ts, Anakin's own MCP server:
--   - POST /url-scraper (submit) + GET /url-scraper/:jobId (poll)
--   - POST /search (synchronous)
--   - POST /agentic-search (submit) + GET /agentic-search/:jobId (poll)
--   - POST /map (submit) + GET /map/:jobId (poll)
--   - POST /crawl (submit) + GET /crawl/:jobId (poll)
--   - GET /wire/resolve (synchronous)
--   - GET /wire/catalog[/:slug] (synchronous)
--   - POST /wire/task (submit, read or write action) + GET /wire/jobs/:jobId (poll)
--   - GET /wire/identities (synchronous)
--   - POST /ai-visibility/search (submit) + GET /ai-visibility/search/:search_id (poll)
--   - GET /ai-visibility/sources (synchronous)
--   - GET /sessions (synchronous)
--   - DELETE /sessions/:id (synchronous)
--   - POST /monitors (synchronous)
--   - GET /monitors[/:id] (synchronous)
--   - GET /monitors/:id/changes (synchronous)
--   - POST /monitors/:id/pause|resume|run, DELETE /monitors/:id (synchronous)
-- Not wrapped here: ai/evaluate (browser_task), wire/login, wire/build-request
-- — scoped out of this pass, see SUBMIT.md.
--
-- Async design: submit_* / check_* procedure PAIRS are the primary,
-- recommended pattern (not a single blocking poll-loop procedure) for every
-- endpoint that is async on Anakin's side. Reason, stated plainly: a Python
-- stored procedure CAN sleep/loop across a poll (Snowflake's default
-- STATEMENT_TIMEOUT_IN_SECONDS is 172800s / 2 days, so nothing stops it),
-- but the warehouse it runs on is billed per second for every second it's
-- blocked in that loop — turning a free client-side wait (which is what
-- anakin-py's HttpClient.poll() and anakin-mcp's client.ts pollJob()/
-- pollWireJob() do today) into paid Snowflake compute for no benefit. A
-- bounded, opt-in `*_sync` convenience procedure is included below for
-- short jobs, for every async operation, with this cost trade-off
-- documented inline — but submit_*/check_* is the procedure pair to reach
-- for by default, especially from a Snowflake Task on a schedule, which is
-- the idiomatic Snowflake-native way to poll an async job without holding a
-- warehouse open the whole time. Endpoints that are already synchronous on
-- Anakin's side (search, wire_discover, wire_catalog, wire_identities,
-- ai_visibility_sources, session_list/delete, monitor_create/list/changes/
-- control) get a single procedure each — no job/poll cycle exists to wrap.
-- ============================================================================

CREATE APPLICATION ROLE IF NOT EXISTS app_public;

CREATE OR ALTER VERSIONED SCHEMA config;
GRANT USAGE ON SCHEMA config TO APPLICATION ROLE app_public;

CREATE OR ALTER VERSIONED SCHEMA core;
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE app_public;

-- ============================================================================
-- Reference callbacks
--
-- Required by any manifest.yml reference of type EXTERNAL ACCESS INTEGRATION
-- or SECRET. Pattern confirmed against
-- docs.snowflake.com/en/developer-guide/native-apps/requesting-refs and
-- .../requesting-example-oauth (register_callback body is close to
-- verbatim from those pages; get_config_for_ref's payload shape for an
-- EXTERNAL ACCESS INTEGRATION reference — host_ports / allowed_secrets —
-- matches the worked example independently corroborated via
-- hakkoda.io/resources/using-requests-to-access-external-endpoints).
-- ============================================================================

CREATE OR REPLACE PROCEDURE config.register_single_reference(
  ref_name STRING, operation STRING, ref_or_alias STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  CASE (operation)
    WHEN 'ADD' THEN
      SELECT SYSTEM$SET_REFERENCE(:ref_name, :ref_or_alias);
    WHEN 'REMOVE' THEN
      SELECT SYSTEM$REMOVE_REFERENCE(:ref_name, :ref_or_alias);
    WHEN 'CLEAR' THEN
      SELECT SYSTEM$REMOVE_ALL_REFERENCES(:ref_name);
    ELSE
      RETURN 'unknown operation: ' || operation;
  END CASE;
  RETURN NULL;
END;
$$;

GRANT USAGE ON PROCEDURE config.register_single_reference(STRING, STRING, STRING)
  TO APPLICATION ROLE app_public;

CREATE OR REPLACE PROCEDURE config.get_config_for_ref(ref_name STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'get_config'
AS
$$
import json

def get_config(ref_name):
    if ref_name == 'anakin_api_access':
        return json.dumps({
            "type": "CONFIGURATION",
            "payload": {
                "host_ports": ["api.anakin.io"],
                "allowed_secrets": "ALL",
            },
        })
    if ref_name == 'anakin_api_key_secret':
        return json.dumps({
            "type": "CONFIGURATION",
            "payload": {"type": "GENERIC_STRING"},
        })
    return json.dumps({
        "type": "ERROR",
        "payload": {"message": f"no configuration for reference '{ref_name}'"},
    })
$$;

GRANT USAGE ON PROCEDURE config.get_config_for_ref(STRING)
  TO APPLICATION ROLE app_public;

-- ============================================================================
-- core.* — the original 3 Anakin API operations (scrape, search, agentic
-- search). The 15 operations added afterwards follow in their own section
-- below, past the agentic-search procedures.
--
-- Each procedure's Python body is a copy of the matching function in
-- ../python/anakin_procs.py (kept in sync by hand — see that file's
-- docstring for why IMPORTS-based staging wasn't used here instead of
-- inline AS $$ ... $$ blocks). Every procedure returns the raw Anakin JSON
-- response as VARIANT, unmodified — this is a thin wrapper, not a
-- reinterpretation of Anakin's response shape, so consumers query it with
-- ordinary Snowflake `:` VARIANT field access (e.g. `result:jobId::string`).
-- ============================================================================

-- ─── url-scraper ────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE core.submit_url_scraper(
  url STRING,
  formats ARRAY DEFAULT NULL,
  country STRING DEFAULT 'us',
  use_browser BOOLEAN DEFAULT FALSE,
  generate_json BOOLEAN DEFAULT FALSE,
  force_fresh BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Submits POST /url-scraper. Returns the raw job-submission JSON (includes jobId). Poll with core.check_url_scraper.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(url, formats=None, country="us", use_browser=False,
        generate_json=False, force_fresh=False):
    api_key = _snowflake.get_generic_secret_string("api_key")
    body = {
        "url": url,
        "country": country,
        "formats": list(formats) if formats else ["markdown"],
        "useBrowser": use_browser,
        "generateJson": generate_json,
        "forceFresh": force_fresh,
    }
    resp = requests.post(
        f"{BASE_URL}/url-scraper",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        json=body,
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin POST /url-scraper failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.submit_url_scraper(STRING, ARRAY, STRING, BOOLEAN, BOOLEAN, BOOLEAN)
  TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE core.check_url_scraper(job_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Single GET /url-scraper/:jobId poll. status is one of pending/processing/completed/failed.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(job_id):
    api_key = _snowflake.get_generic_secret_string("api_key")
    resp = requests.get(
        f"{BASE_URL}/url-scraper/{job_id}",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin GET /url-scraper/{job_id} failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.check_url_scraper(STRING)
  TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE core.scrape_url_sync(
  url STRING,
  formats ARRAY DEFAULT NULL,
  country STRING DEFAULT 'us',
  use_browser BOOLEAN DEFAULT FALSE,
  generate_json BOOLEAN DEFAULT FALSE,
  force_fresh BOOLEAN DEFAULT FALSE,
  max_wait_seconds FLOAT DEFAULT 60
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Convenience: submits + polls in one call, capped at max_wait_seconds. Blocks the calling warehouse the whole time it waits — prefer submit_url_scraper/check_url_scraper for anything that might run long.'
AS
$$
import time
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"


def _headers():
    return {"X-API-Key": _snowflake.get_generic_secret_string("api_key"), "Accept": "application/json"}


def _get(path):
    resp = requests.get(f"{BASE_URL}{path}", headers=_headers(), timeout=30)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Anakin GET {path} failed: HTTP {resp.status_code} - {resp.text[:2000]}")
    return resp.json()


def run(url, formats=None, country="us", use_browser=False, generate_json=False,
        force_fresh=False, max_wait_seconds=60):
    body = {
        "url": url,
        "country": country,
        "formats": list(formats) if formats else ["markdown"],
        "useBrowser": use_browser,
        "generateJson": generate_json,
        "forceFresh": force_fresh,
    }
    resp = requests.post(f"{BASE_URL}/url-scraper", headers=_headers(), json=body, timeout=30)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Anakin POST /url-scraper failed: HTTP {resp.status_code} - {resp.text[:2000]}")
    submitted = resp.json()
    job_id = submitted.get("jobId") or submitted.get("job_id") or submitted.get("id")
    if not job_id:
        return submitted

    deadline = time.monotonic() + max_wait_seconds
    wait = 1.0
    path = f"/url-scraper/{job_id}"
    while True:
        body = _get(path)
        if isinstance(body, dict) and body.get("status") in ("completed", "failed"):
            return body
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            body["_snowflake_note"] = (
                f"Polling stopped after {max_wait_seconds}s without a terminal status; "
                f"the job may still be running. Call core.check_url_scraper('{job_id}') later."
            )
            return body
        time.sleep(min(wait, remaining))
        wait = min(wait * 1.5, 10.0)
$$;

GRANT USAGE ON PROCEDURE core.scrape_url_sync(STRING, ARRAY, STRING, BOOLEAN, BOOLEAN, BOOLEAN, FLOAT)
  TO APPLICATION ROLE app_public;


-- ─── search (synchronous — no job/poll cycle) ───────────────────────────────

CREATE OR REPLACE PROCEDURE core.anakin_search(
  prompt STRING,
  result_limit FLOAT DEFAULT 5
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'POST /search. Synchronous — returns directly, no polling. Costs 3 Anakin credits per call.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(prompt, result_limit=5):
    api_key = _snowflake.get_generic_secret_string("api_key")
    body = {"prompt": prompt, "limit": result_limit}
    resp = requests.post(
        f"{BASE_URL}/search",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        json=body,
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin POST /search failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.anakin_search(STRING, FLOAT)
  TO APPLICATION ROLE app_public;


-- ─── agentic-search ──────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE core.submit_agentic_search(
  prompt STRING,
  use_browser BOOLEAN DEFAULT TRUE,
  schema VARIANT DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Submits POST /agentic-search. Returns the raw job-submission JSON (includes jobId). Poll with core.check_agentic_search. Costs 10 Anakin credits.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(prompt, use_browser=True, schema=None):
    api_key = _snowflake.get_generic_secret_string("api_key")
    body = {"prompt": prompt, "useBrowser": use_browser}
    if schema is not None:
        body["schema"] = schema
    resp = requests.post(
        f"{BASE_URL}/agentic-search",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        json=body,
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin POST /agentic-search failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.submit_agentic_search(STRING, BOOLEAN, VARIANT)
  TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE core.check_agentic_search(job_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Single GET /agentic-search/:jobId poll.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(job_id):
    api_key = _snowflake.get_generic_secret_string("api_key")
    resp = requests.get(
        f"{BASE_URL}/agentic-search/{job_id}",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin GET /agentic-search/{job_id} failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.check_agentic_search(STRING)
  TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE core.agentic_search_sync(
  prompt STRING,
  use_browser BOOLEAN DEFAULT TRUE,
  schema VARIANT DEFAULT NULL,
  max_wait_seconds FLOAT DEFAULT 90
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Convenience: submits + polls in one call, capped at max_wait_seconds. Agentic-search is a multi-stage pipeline and the slowest of the three operations wrapped here — prefer submit_agentic_search/check_agentic_search (e.g. from a scheduled Task) over raising this cap.'
AS
$$
import time
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"


def _headers():
    return {"X-API-Key": _snowflake.get_generic_secret_string("api_key"), "Accept": "application/json"}


def _get(path):
    resp = requests.get(f"{BASE_URL}{path}", headers=_headers(), timeout=30)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Anakin GET {path} failed: HTTP {resp.status_code} - {resp.text[:2000]}")
    return resp.json()


def run(prompt, use_browser=True, schema=None, max_wait_seconds=90):
    body = {"prompt": prompt, "useBrowser": use_browser}
    if schema is not None:
        body["schema"] = schema
    resp = requests.post(f"{BASE_URL}/agentic-search", headers=_headers(), json=body, timeout=30)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Anakin POST /agentic-search failed: HTTP {resp.status_code} - {resp.text[:2000]}")
    submitted = resp.json()
    job_id = submitted.get("jobId") or submitted.get("job_id") or submitted.get("id")
    if not job_id:
        return submitted

    deadline = time.monotonic() + max_wait_seconds
    wait = 1.0
    path = f"/agentic-search/{job_id}"
    while True:
        body = _get(path)
        if isinstance(body, dict) and body.get("status") in ("completed", "failed"):
            return body
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            body["_snowflake_note"] = (
                f"Polling stopped after {max_wait_seconds}s without a terminal status; "
                f"the job may still be running. Call core.check_agentic_search('{job_id}') later."
            )
            return body
        time.sleep(min(wait, remaining))
        wait = min(wait * 1.5, 10.0)
$$;

GRANT USAGE ON PROCEDURE core.agentic_search_sync(STRING, BOOLEAN, VARIANT, FLOAT)
  TO APPLICATION ROLE app_public;


-- ============================================================================
-- Endpoints added after the initial 3-op release. Ground truth for every
-- request body / query param / poll path below: anakin-mcp's own source —
-- src/client.ts (AnakinClient — exact wire format) and src/tools/*.ts
-- (parameter names/defaults as exposed to callers), Anakin's own MCP
-- server, not this submission's guesses. Same pattern as above: async ops
-- get a submit_*/check_* pair plus a bounded *_sync convenience; endpoints
-- that are already synchronous on Anakin's side (no jobId) get one
-- procedure. Every procedure returns the raw Anakin JSON response as
-- VARIANT, unmodified.
-- ============================================================================

-- ─── map: POST /map (submit) + GET /map/:jobId (poll) ──────────────────────
-- Body fields/defaults match AnakinClient.map() in client.ts exactly. The
-- overall-count parameter is named url_limit here (not `limit`) to avoid
-- colliding with the SQL reserved word LIMIT in the procedure signature.

CREATE OR REPLACE PROCEDURE core.submit_map(
  url STRING,
  url_limit FLOAT DEFAULT 100,
  depth FLOAT DEFAULT 2,
  limit_per_level FLOAT DEFAULT 100,
  include_subdomains BOOLEAN DEFAULT FALSE,
  include_external_links BOOLEAN DEFAULT FALSE,
  use_browser BOOLEAN DEFAULT FALSE,
  search STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Submits POST /map (site URL discovery). Returns the raw job-submission JSON (includes jobId). Poll with core.check_map.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(url, url_limit=100, depth=2, limit_per_level=100, include_subdomains=False,
        include_external_links=False, use_browser=False, search=None):
    api_key = _snowflake.get_generic_secret_string("api_key")
    body = {
        "url": url,
        "limit": url_limit,
        "depth": depth,
        "limitPerLevel": limit_per_level,
        "includeSubdomains": include_subdomains,
        "includeExternalLinks": include_external_links,
        "useBrowser": use_browser,
    }
    if search is not None:
        body["search"] = search
    resp = requests.post(
        f"{BASE_URL}/map",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        json=body,
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin POST /map failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.submit_map(STRING, FLOAT, FLOAT, FLOAT, BOOLEAN, BOOLEAN, BOOLEAN, STRING)
  TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE core.check_map(job_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Single GET /map/:jobId poll.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(job_id):
    api_key = _snowflake.get_generic_secret_string("api_key")
    resp = requests.get(
        f"{BASE_URL}/map/{job_id}",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin GET /map/{job_id} failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.check_map(STRING)
  TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE core.map_sync(
  url STRING,
  url_limit FLOAT DEFAULT 100,
  depth FLOAT DEFAULT 2,
  limit_per_level FLOAT DEFAULT 100,
  include_subdomains BOOLEAN DEFAULT FALSE,
  include_external_links BOOLEAN DEFAULT FALSE,
  use_browser BOOLEAN DEFAULT FALSE,
  search STRING DEFAULT NULL,
  max_wait_seconds FLOAT DEFAULT 90
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Convenience: submits + polls in one call, capped at max_wait_seconds. Prefer submit_map/check_map for anything that might run long.'
AS
$$
import time
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"


def _headers():
    return {"X-API-Key": _snowflake.get_generic_secret_string("api_key"), "Accept": "application/json"}


def _get(path):
    resp = requests.get(f"{BASE_URL}{path}", headers=_headers(), timeout=30)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Anakin GET {path} failed: HTTP {resp.status_code} - {resp.text[:2000]}")
    return resp.json()


def run(url, url_limit=100, depth=2, limit_per_level=100, include_subdomains=False,
        include_external_links=False, use_browser=False, search=None, max_wait_seconds=90):
    body = {
        "url": url,
        "limit": url_limit,
        "depth": depth,
        "limitPerLevel": limit_per_level,
        "includeSubdomains": include_subdomains,
        "includeExternalLinks": include_external_links,
        "useBrowser": use_browser,
    }
    if search is not None:
        body["search"] = search
    resp = requests.post(f"{BASE_URL}/map", headers=_headers(), json=body, timeout=30)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Anakin POST /map failed: HTTP {resp.status_code} - {resp.text[:2000]}")
    submitted = resp.json()
    job_id = submitted.get("jobId") or submitted.get("job_id") or submitted.get("id")
    if not job_id:
        return submitted

    deadline = time.monotonic() + max_wait_seconds
    wait = 1.0
    path = f"/map/{job_id}"
    while True:
        body = _get(path)
        if isinstance(body, dict) and body.get("status") in ("completed", "failed"):
            return body
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            body["_snowflake_note"] = (
                f"Polling stopped after {max_wait_seconds}s without a terminal status; "
                f"the job may still be running. Call core.check_map('{job_id}') later."
            )
            return body
        time.sleep(min(wait, remaining))
        wait = min(wait * 1.5, 10.0)
$$;

GRANT USAGE ON PROCEDURE core.map_sync(STRING, FLOAT, FLOAT, FLOAT, BOOLEAN, BOOLEAN, BOOLEAN, STRING, FLOAT)
  TO APPLICATION ROLE app_public;


-- ─── crawl: POST /crawl (submit) + GET /crawl/:jobId (poll) ────────────────
-- Body fields/defaults match AnakinClient.crawl() in client.ts exactly.

CREATE OR REPLACE PROCEDURE core.submit_crawl(
  url STRING,
  max_pages FLOAT DEFAULT 10,
  depth FLOAT DEFAULT 1,
  country STRING DEFAULT 'us',
  use_browser BOOLEAN DEFAULT FALSE,
  include_patterns ARRAY DEFAULT NULL,
  exclude_patterns ARRAY DEFAULT NULL,
  session_id STRING DEFAULT NULL,
  session_name STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Submits POST /crawl (bulk multi-page fetch). Returns the raw job-submission JSON (includes jobId). Poll with core.check_crawl.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(url, max_pages=10, depth=1, country="us", use_browser=False,
        include_patterns=None, exclude_patterns=None, session_id=None, session_name=None):
    api_key = _snowflake.get_generic_secret_string("api_key")
    body = {
        "url": url,
        "maxPages": max_pages,
        "depth": depth,
        "country": country,
        "useBrowser": use_browser,
    }
    if include_patterns:
        body["includePatterns"] = list(include_patterns)
    if exclude_patterns:
        body["excludePatterns"] = list(exclude_patterns)
    if session_id is not None:
        body["sessionId"] = session_id
    if session_name is not None:
        body["sessionName"] = session_name
    resp = requests.post(
        f"{BASE_URL}/crawl",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        json=body,
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin POST /crawl failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.submit_crawl(STRING, FLOAT, FLOAT, STRING, BOOLEAN, ARRAY, ARRAY, STRING, STRING)
  TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE core.check_crawl(job_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Single GET /crawl/:jobId poll.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(job_id):
    api_key = _snowflake.get_generic_secret_string("api_key")
    resp = requests.get(
        f"{BASE_URL}/crawl/{job_id}",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin GET /crawl/{job_id} failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.check_crawl(STRING)
  TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE core.crawl_sync(
  url STRING,
  max_pages FLOAT DEFAULT 10,
  depth FLOAT DEFAULT 1,
  country STRING DEFAULT 'us',
  use_browser BOOLEAN DEFAULT FALSE,
  include_patterns ARRAY DEFAULT NULL,
  exclude_patterns ARRAY DEFAULT NULL,
  session_id STRING DEFAULT NULL,
  session_name STRING DEFAULT NULL,
  max_wait_seconds FLOAT DEFAULT 120
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Convenience: submits + polls in one call, capped at max_wait_seconds. Crawls can cover many pages — prefer submit_crawl/check_crawl (e.g. from a scheduled Task) for anything beyond a quick worksheet check.'
AS
$$
import time
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"


def _headers():
    return {"X-API-Key": _snowflake.get_generic_secret_string("api_key"), "Accept": "application/json"}


def _get(path):
    resp = requests.get(f"{BASE_URL}{path}", headers=_headers(), timeout=30)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Anakin GET {path} failed: HTTP {resp.status_code} - {resp.text[:2000]}")
    return resp.json()


def run(url, max_pages=10, depth=1, country="us", use_browser=False, include_patterns=None,
        exclude_patterns=None, session_id=None, session_name=None, max_wait_seconds=120):
    body = {
        "url": url,
        "maxPages": max_pages,
        "depth": depth,
        "country": country,
        "useBrowser": use_browser,
    }
    if include_patterns:
        body["includePatterns"] = list(include_patterns)
    if exclude_patterns:
        body["excludePatterns"] = list(exclude_patterns)
    if session_id is not None:
        body["sessionId"] = session_id
    if session_name is not None:
        body["sessionName"] = session_name
    resp = requests.post(f"{BASE_URL}/crawl", headers=_headers(), json=body, timeout=30)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Anakin POST /crawl failed: HTTP {resp.status_code} - {resp.text[:2000]}")
    submitted = resp.json()
    job_id = submitted.get("jobId") or submitted.get("job_id") or submitted.get("id")
    if not job_id:
        return submitted

    deadline = time.monotonic() + max_wait_seconds
    wait = 1.0
    path = f"/crawl/{job_id}"
    while True:
        body = _get(path)
        if isinstance(body, dict) and body.get("status") in ("completed", "failed"):
            return body
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            body["_snowflake_note"] = (
                f"Polling stopped after {max_wait_seconds}s without a terminal status; "
                f"the job may still be running. Call core.check_crawl('{job_id}') later."
            )
            return body
        time.sleep(min(wait, remaining))
        wait = min(wait * 1.5, 10.0)
$$;

GRANT USAGE ON PROCEDURE core.crawl_sync(STRING, FLOAT, FLOAT, STRING, BOOLEAN, ARRAY, ARRAY, STRING, STRING, FLOAT)
  TO APPLICATION ROLE app_public;


-- ─── wire_discover: GET /wire/resolve?q=&limit= (synchronous) ──────────────

CREATE OR REPLACE PROCEDURE core.wire_discover(
  q STRING,
  result_limit FLOAT DEFAULT 5
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'GET /wire/resolve. Natural-language intent to ranked candidate Wire actions (action_id, type read/write, params, credit cost, auth requirement). Synchronous.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(q, result_limit=5):
    api_key = _snowflake.get_generic_secret_string("api_key")
    params = {"q": q}
    if result_limit is not None:
        params["limit"] = result_limit
    resp = requests.get(
        f"{BASE_URL}/wire/resolve",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        params=params,
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin GET /wire/resolve failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.wire_discover(STRING, FLOAT)
  TO APPLICATION ROLE app_public;


-- ─── wire_catalog: GET /wire/catalog or GET /wire/catalog/:slug (sync) ─────

CREATE OR REPLACE PROCEDURE core.wire_catalog(slug STRING DEFAULT NULL)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'GET /wire/catalog (all sites) or GET /wire/catalog/:slug (one site''s full action list with parameter schemas). Synchronous.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(slug=None):
    api_key = _snowflake.get_generic_secret_string("api_key")
    path = f"/wire/catalog/{slug}" if slug else "/wire/catalog"
    resp = requests.get(
        f"{BASE_URL}{path}",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin GET {path} failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.wire_catalog(STRING)
  TO APPLICATION ROLE app_public;


-- ─── wire_read_action / wire_write_action: POST /wire/task (submit) +
-- GET /wire/jobs/:jobId (poll). Both share the same request/poll shape in
-- the real API — anakin-mcp's read/write split (wire_read_action vs.
-- wire_write_action) is a client-side safety convention (separate tools
-- carrying honest readOnlyHint/destructiveHint annotations), not two
-- different endpoints. Mirrored here the same way: two submit procedures
-- sharing one check_wire_job procedure. The POST response may already
-- carry terminal data inline with no job_id (a synchronous action) — both
-- *_sync variants handle that by returning it directly. `action_params` is
-- named that (not `params`) to sidestep any ambiguity with the SQL PARAMS
-- keyword in the procedure signature. ───────────────────────────────────────

CREATE OR REPLACE PROCEDURE core.submit_wire_read_action(
  action_id STRING,
  action_params VARIANT DEFAULT NULL,
  credential_id STRING DEFAULT NULL,
  identity_id STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Submits POST /wire/task for a READ (data-extraction, read-only) Wire action. Returns the raw response — either terminal data inline (sync action) or {job_id} to poll with core.check_wire_job. Confirm the action''s type is "read" via core.wire_discover/wire_catalog first; use core.submit_wire_write_action for "write" actions.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(action_id, action_params=None, credential_id=None, identity_id=None):
    api_key = _snowflake.get_generic_secret_string("api_key")
    body = {"action_id": action_id}
    if action_params:
        body["params"] = action_params
    if credential_id is not None:
        body["credential_id"] = credential_id
    if identity_id is not None:
        body["identity_id"] = identity_id
    resp = requests.post(
        f"{BASE_URL}/wire/task",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        json=body,
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin POST /wire/task failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.submit_wire_read_action(STRING, VARIANT, STRING, STRING)
  TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE core.submit_wire_write_action(
  action_id STRING,
  action_params VARIANT DEFAULT NULL,
  credential_id STRING DEFAULT NULL,
  identity_id STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Submits POST /wire/task for a WRITE (state-changing — submit a form, add to cart, etc.) Wire action. Returns the raw response — either terminal data inline (sync action) or {job_id} to poll with core.check_wire_job. Confirm the action''s type is "write" via core.wire_discover/wire_catalog first; use core.submit_wire_read_action for "read" actions. Does not execute payments or fund transfers on its own — that restriction, if desired, is left to the caller/catalog since this is a thin API passthrough.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(action_id, action_params=None, credential_id=None, identity_id=None):
    api_key = _snowflake.get_generic_secret_string("api_key")
    body = {"action_id": action_id}
    if action_params:
        body["params"] = action_params
    if credential_id is not None:
        body["credential_id"] = credential_id
    if identity_id is not None:
        body["identity_id"] = identity_id
    resp = requests.post(
        f"{BASE_URL}/wire/task",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        json=body,
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin POST /wire/task failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.submit_wire_write_action(STRING, VARIANT, STRING, STRING)
  TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE core.check_wire_job(job_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Single GET /wire/jobs/:jobId poll, for a job_id returned by core.submit_wire_read_action or core.submit_wire_write_action. status is one of processing/completed/failed.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(job_id):
    api_key = _snowflake.get_generic_secret_string("api_key")
    resp = requests.get(
        f"{BASE_URL}/wire/jobs/{job_id}",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin GET /wire/jobs/{job_id} failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.check_wire_job(STRING)
  TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE core.wire_read_action_sync(
  action_id STRING,
  action_params VARIANT DEFAULT NULL,
  credential_id STRING DEFAULT NULL,
  identity_id STRING DEFAULT NULL,
  max_wait_seconds FLOAT DEFAULT 60
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Convenience: submits a READ Wire action + polls in one call, capped at max_wait_seconds. Honors the job''s server-suggested retry_after_ms pacing hint. Prefer submit_wire_read_action/check_wire_job for anything that might run long.'
AS
$$
import time
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"


def _headers():
    return {"X-API-Key": _snowflake.get_generic_secret_string("api_key"), "Accept": "application/json"}


def _get(path):
    resp = requests.get(f"{BASE_URL}{path}", headers=_headers(), timeout=30)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Anakin GET {path} failed: HTTP {resp.status_code} - {resp.text[:2000]}")
    return resp.json()


def run(action_id, action_params=None, credential_id=None, identity_id=None, max_wait_seconds=60):
    body = {"action_id": action_id}
    if action_params:
        body["params"] = action_params
    if credential_id is not None:
        body["credential_id"] = credential_id
    if identity_id is not None:
        body["identity_id"] = identity_id
    resp = requests.post(f"{BASE_URL}/wire/task", headers=_headers(), json=body, timeout=30)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Anakin POST /wire/task failed: HTTP {resp.status_code} - {resp.text[:2000]}")
    submitted = resp.json()
    job_id = submitted.get("job_id") if isinstance(submitted, dict) else None
    if not job_id:
        return submitted  # sync action: terminal data came back inline

    deadline = time.monotonic() + max_wait_seconds
    path = f"/wire/jobs/{job_id}"
    while True:
        body = _get(path)
        status = body.get("status") if isinstance(body, dict) else None
        if status in ("completed", "failed"):
            return body
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            body["_snowflake_note"] = (
                f"Polling stopped after {max_wait_seconds}s without a terminal status; "
                f"the job may still be running. Call core.check_wire_job('{job_id}') later."
            )
            return body
        retry_after_ms = body.get("retry_after_ms") if isinstance(body, dict) else None
        wait_s = (retry_after_ms / 1000.0) if isinstance(retry_after_ms, (int, float)) else 3.0
        wait_s = min(max(wait_s, 0.5), 10.0)
        time.sleep(min(wait_s, remaining))
$$;

GRANT USAGE ON PROCEDURE core.wire_read_action_sync(STRING, VARIANT, STRING, STRING, FLOAT)
  TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE core.wire_write_action_sync(
  action_id STRING,
  action_params VARIANT DEFAULT NULL,
  credential_id STRING DEFAULT NULL,
  identity_id STRING DEFAULT NULL,
  max_wait_seconds FLOAT DEFAULT 60
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Convenience: submits a WRITE Wire action + polls in one call, capped at max_wait_seconds. Honors the job''s server-suggested retry_after_ms pacing hint. Prefer submit_wire_write_action/check_wire_job for anything that might run long.'
AS
$$
import time
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"


def _headers():
    return {"X-API-Key": _snowflake.get_generic_secret_string("api_key"), "Accept": "application/json"}


def _get(path):
    resp = requests.get(f"{BASE_URL}{path}", headers=_headers(), timeout=30)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Anakin GET {path} failed: HTTP {resp.status_code} - {resp.text[:2000]}")
    return resp.json()


def run(action_id, action_params=None, credential_id=None, identity_id=None, max_wait_seconds=60):
    body = {"action_id": action_id}
    if action_params:
        body["params"] = action_params
    if credential_id is not None:
        body["credential_id"] = credential_id
    if identity_id is not None:
        body["identity_id"] = identity_id
    resp = requests.post(f"{BASE_URL}/wire/task", headers=_headers(), json=body, timeout=30)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Anakin POST /wire/task failed: HTTP {resp.status_code} - {resp.text[:2000]}")
    submitted = resp.json()
    job_id = submitted.get("job_id") if isinstance(submitted, dict) else None
    if not job_id:
        return submitted

    deadline = time.monotonic() + max_wait_seconds
    path = f"/wire/jobs/{job_id}"
    while True:
        body = _get(path)
        status = body.get("status") if isinstance(body, dict) else None
        if status in ("completed", "failed"):
            return body
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            body["_snowflake_note"] = (
                f"Polling stopped after {max_wait_seconds}s without a terminal status; "
                f"the job may still be running. Call core.check_wire_job('{job_id}') later."
            )
            return body
        retry_after_ms = body.get("retry_after_ms") if isinstance(body, dict) else None
        wait_s = (retry_after_ms / 1000.0) if isinstance(retry_after_ms, (int, float)) else 3.0
        wait_s = min(max(wait_s, 0.5), 10.0)
        time.sleep(min(wait_s, remaining))
$$;

GRANT USAGE ON PROCEDURE core.wire_write_action_sync(STRING, VARIANT, STRING, STRING, FLOAT)
  TO APPLICATION ROLE app_public;


-- ─── wire_identities: GET /wire/identities?catalog_id= (synchronous) ───────

CREATE OR REPLACE PROCEDURE core.wire_identities(catalog_id STRING DEFAULT NULL)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'GET /wire/identities. Lists saved Wire identities and their credentials (credential_id values for auth-required actions), optionally filtered by catalog_id.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(catalog_id=None):
    api_key = _snowflake.get_generic_secret_string("api_key")
    params = {"catalog_id": catalog_id} if catalog_id is not None else None
    resp = requests.get(
        f"{BASE_URL}/wire/identities",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        params=params,
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin GET /wire/identities failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.wire_identities(STRING)
  TO APPLICATION ROLE app_public;


-- ─── ai_visibility_search: POST /ai-visibility/search (submit) +
-- GET /ai-visibility/search/:search_id (poll) ───────────────────────────────
-- Terminal condition differs from every other poller in this file:
-- client.ts's aiVisibilitySearch() stops as soon as status != "running"
-- (returning a "failed" result rather than throwing — the payload still
-- carries per-source results/errors), not on a named completed/failed pair.

CREATE OR REPLACE PROCEDURE core.submit_ai_visibility_search(
  query STRING,
  sources ARRAY DEFAULT NULL,
  country STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Submits POST /ai-visibility/search. Returns the raw job-submission JSON (includes search_id). Poll with core.check_ai_visibility_search. Billed per source queried; failed sources are free.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(query, sources=None, country=None):
    api_key = _snowflake.get_generic_secret_string("api_key")
    body = {"query": query}
    if sources:
        body["sources"] = list(sources)
    if country is not None:
        body["country"] = country
    resp = requests.post(
        f"{BASE_URL}/ai-visibility/search",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        json=body,
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin POST /ai-visibility/search failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.submit_ai_visibility_search(STRING, ARRAY, STRING)
  TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE core.check_ai_visibility_search(search_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Single GET /ai-visibility/search/:search_id poll. status "running" means not yet terminal; any other status (e.g. "completed", "failed") is terminal.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(search_id):
    api_key = _snowflake.get_generic_secret_string("api_key")
    resp = requests.get(
        f"{BASE_URL}/ai-visibility/search/{search_id}",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin GET /ai-visibility/search/{search_id} failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.check_ai_visibility_search(STRING)
  TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE core.ai_visibility_search_sync(
  query STRING,
  sources ARRAY DEFAULT NULL,
  country STRING DEFAULT NULL,
  max_wait_seconds FLOAT DEFAULT 120
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Convenience: submits + polls in one call, capped at max_wait_seconds. Terminal as soon as status != "running" (matches anakin-mcp''s client.ts). Prefer submit_ai_visibility_search/check_ai_visibility_search for anything that might run long.'
AS
$$
import time
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"


def _headers():
    return {"X-API-Key": _snowflake.get_generic_secret_string("api_key"), "Accept": "application/json"}


def _get(path):
    resp = requests.get(f"{BASE_URL}{path}", headers=_headers(), timeout=30)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Anakin GET {path} failed: HTTP {resp.status_code} - {resp.text[:2000]}")
    return resp.json()


def run(query, sources=None, country=None, max_wait_seconds=120):
    body = {"query": query}
    if sources:
        body["sources"] = list(sources)
    if country is not None:
        body["country"] = country
    resp = requests.post(f"{BASE_URL}/ai-visibility/search", headers=_headers(), json=body, timeout=30)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Anakin POST /ai-visibility/search failed: HTTP {resp.status_code} - {resp.text[:2000]}")
    submitted = resp.json()
    search_id = submitted.get("search_id") if isinstance(submitted, dict) else None
    if not search_id:
        return submitted

    deadline = time.monotonic() + max_wait_seconds
    path = f"/ai-visibility/search/{search_id}"
    while True:
        body = _get(path)
        status = body.get("status") if isinstance(body, dict) else None
        if status != "running":
            return body
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            body["_snowflake_note"] = (
                f"Polling stopped after {max_wait_seconds}s while still 'running'; "
                f"call core.check_ai_visibility_search('{search_id}') later."
            )
            return body
        time.sleep(min(3.0, remaining))
$$;

GRANT USAGE ON PROCEDURE core.ai_visibility_search_sync(STRING, ARRAY, STRING, FLOAT)
  TO APPLICATION ROLE app_public;


-- ─── ai_visibility_sources: GET /ai-visibility/sources (synchronous) ───────

CREATE OR REPLACE PROCEDURE core.ai_visibility_sources()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'GET /ai-visibility/sources. Lists the AI answer engines available to core.ai_visibility_search (slug + display label).'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run():
    api_key = _snowflake.get_generic_secret_string("api_key")
    resp = requests.get(
        f"{BASE_URL}/ai-visibility/sources",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin GET /ai-visibility/sources failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.ai_visibility_sources()
  TO APPLICATION ROLE app_public;


-- ─── session_list: GET /sessions?domain= (synchronous) ─────────────────────

CREATE OR REPLACE PROCEDURE core.session_list(domain STRING DEFAULT NULL)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'GET /sessions. Lists saved browser sessions (encrypted login states); each id is usable as sessionId in scrape/crawl/map/monitor_create. Optionally filter by domain.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(domain=None):
    api_key = _snowflake.get_generic_secret_string("api_key")
    params = {"domain": domain} if domain is not None else None
    resp = requests.get(
        f"{BASE_URL}/sessions",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        params=params,
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin GET /sessions failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.session_list(STRING)
  TO APPLICATION ROLE app_public;


-- ─── session_delete: DELETE /sessions/:id (synchronous) ────────────────────

CREATE OR REPLACE PROCEDURE core.session_delete(session_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'DELETE /sessions/:id. Permanently deletes a saved browser session. Irreversible.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(session_id):
    api_key = _snowflake.get_generic_secret_string("api_key")
    resp = requests.delete(
        f"{BASE_URL}/sessions/{session_id}",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin DELETE /sessions/{session_id} failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    if not resp.content:
        return {}
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.session_delete(STRING)
  TO APPLICATION ROLE app_public;


-- ─── monitor_create: POST /monitors (synchronous — returns the created
-- monitor directly, not a job to poll). `options` is a VARIANT object
-- mirroring MonitorCreateOptions in client.ts (scope, watchMode,
-- watchFormat, outputSchema, aiMode, aiGoal, useBrowser, country,
-- sessionId, isActive, expiresAt, alertWebhookUrl, alertEmails, maxPages,
-- maxDepth, includePatterns, excludePatterns, wireActionId,
-- wireCatalogSlug, wireCredentialId, wireParams, wireWatchPaths) — pass the
-- exact camelCase JSON keys the API expects; omitted keys default
-- server-side. A VARIANT bag (not ~20 individual SQL parameters) matches
-- how `schema` is already passed to submit_agentic_search. Note: unlike
-- anakin-mcp (which redacts each monitor's alertWebhookSecret before it
-- reaches a model), this procedure returns Anakin's response unmodified,
-- consistent with every other procedure in this file being a thin,
-- unmodified passthrough — treat any alertWebhookSecret in the result as
-- sensitive. ──────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE core.monitor_create(
  url STRING,
  interval_minutes FLOAT,
  options VARIANT DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'POST /monitors. Creates a scheduled website monitor (min intervalMinutes 15). Synchronous — returns the created monitor directly. Returns Anakin''s response unmodified, including any alertWebhookSecret field — treat it as sensitive.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(url, interval_minutes, options=None):
    api_key = _snowflake.get_generic_secret_string("api_key")
    body = {"url": url, "intervalMinutes": interval_minutes}
    if options:
        body.update(options)
    resp = requests.post(
        f"{BASE_URL}/monitors",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        json=body,
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin POST /monitors failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.monitor_create(STRING, FLOAT, VARIANT)
  TO APPLICATION ROLE app_public;


-- ─── monitor_list: GET /monitors, or GET /monitors/:id when monitor_id is
-- given (synchronous) ───────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE core.monitor_list(monitor_id STRING DEFAULT NULL)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'GET /monitors (list all) or GET /monitors/:id (one monitor''s full config/status) when monitor_id is passed.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(monitor_id=None):
    api_key = _snowflake.get_generic_secret_string("api_key")
    path = f"/monitors/{monitor_id}" if monitor_id else "/monitors"
    resp = requests.get(
        f"{BASE_URL}{path}",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin GET {path} failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.monitor_list(STRING)
  TO APPLICATION ROLE app_public;


-- ─── monitor_changes: GET /monitors/:id/changes (synchronous) ──────────────

CREATE OR REPLACE PROCEDURE core.monitor_changes(monitor_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'GET /monitors/:id/changes. Detected changes for a monitor, each with when the content differed from the previous check.'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(monitor_id):
    api_key = _snowflake.get_generic_secret_string("api_key")
    resp = requests.get(
        f"{BASE_URL}/monitors/{monitor_id}/changes",
        headers={"X-API-Key": api_key, "Accept": "application/json"},
        timeout=30,
    )
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin GET /monitors/{monitor_id}/changes failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.monitor_changes(STRING)
  TO APPLICATION ROLE app_public;


-- ─── monitor_control: POST /monitors/:id/pause|resume|run, or
-- DELETE /monitors/:id for "delete". action "run_now" maps to POST
-- .../run (not .../run_now) — matches client.ts's monitorControl() switch
-- exactly. ────────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE core.monitor_control(
  monitor_id STRING,
  action STRING
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (reference('anakin_api_access'))
SECRETS = ('api_key' = reference('anakin_api_key_secret'))
HANDLER = 'run'
COMMENT = 'Controls a monitor: action "pause" (POST .../pause), "resume" (POST .../resume), "run_now" (POST .../run — an immediate, billed out-of-schedule check), or "delete" (DELETE .../:id — permanent).'
AS
$$
import _snowflake
import requests

BASE_URL = "https://api.anakin.io/v1"

def run(monitor_id, action):
    api_key = _snowflake.get_generic_secret_string("api_key")
    headers = {"X-API-Key": api_key, "Accept": "application/json"}
    base = f"{BASE_URL}/monitors/{monitor_id}"
    if action == "pause":
        resp = requests.post(f"{base}/pause", headers=headers, timeout=30)
        path = f"/monitors/{monitor_id}/pause"
    elif action == "resume":
        resp = requests.post(f"{base}/resume", headers=headers, timeout=30)
        path = f"/monitors/{monitor_id}/resume"
    elif action == "run_now":
        resp = requests.post(f"{base}/run", headers=headers, timeout=30)
        path = f"/monitors/{monitor_id}/run"
    elif action == "delete":
        resp = requests.delete(base, headers=headers, timeout=30)
        path = f"/monitors/{monitor_id}"
    else:
        raise ValueError(f'Unknown monitor action "{action}" - use pause, resume, run_now, or delete.')

    if not (200 <= resp.status_code < 300):
        raise RuntimeError(
            f"Anakin request to {path} failed: HTTP {resp.status_code} - {resp.text[:2000]}"
        )
    if not resp.content:
        return {}
    return resp.json()
$$;

GRANT USAGE ON PROCEDURE core.monitor_control(STRING, STRING)
  TO APPLICATION ROLE app_public;
