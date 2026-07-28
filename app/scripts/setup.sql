-- ============================================================================
-- Anakin API for Snowflake — setup.sql
--
-- Runs once when the consumer CREATEs (or upgrades) the application, per
-- docs.snowflake.com/en/developer-guide/native-apps/native-apps-about
-- ("Snowflake creates the application and runs the setup script to create
-- the required objects within the application").
--
-- Wraps three Anakin API operations (base URL https://api.anakin.io/v1,
-- auth via X-API-Key — ground truth read from anakin-py's src/anakin/
-- client.py / _http.py, the official SDK built earlier this session):
--   - POST /url-scraper (submit) + GET /url-scraper/:jobId (poll)
--   - POST /search (synchronous)
--   - POST /agentic-search (submit) + GET /agentic-search/:jobId (poll)
--
-- Async design: submit_* / check_* procedure PAIRS are the primary,
-- recommended pattern (not a single blocking poll-loop procedure). Reason,
-- stated plainly: a Python stored procedure CAN sleep/loop across a poll
-- (Snowflake's default STATEMENT_TIMEOUT_IN_SECONDS is 172800s / 2 days,
-- so nothing stops it), but the warehouse it runs on is billed per second
-- for every second it's blocked in that loop — turning a free client-side
-- wait (which is what anakin-py's HttpClient.poll() does today) into paid
-- Snowflake compute for no benefit. A bounded, opt-in `*_sync` convenience
-- procedure is included below for short jobs (default caps: 60s for
-- url-scraper, 90s for agentic-search) with this cost trade-off documented
-- inline — but submit_*/check_* is the procedure pair to reach for by
-- default, especially from a Snowflake Task on a schedule, which is the
-- idiomatic Snowflake-native way to poll an async job without holding a
-- warehouse open the whole time.
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
-- core.* — the 3 Anakin API operations
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
