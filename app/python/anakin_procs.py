"""
Reference implementation of the Python handler logic used by the Anakin
Native App's Snowpark stored procedures (see ../scripts/setup.sql).

Ground truth for the original three wrapped endpoints — POST /url-scraper +
GET /url-scraper/:jobId, POST /search, POST /agentic-search + GET
/agentic-search/:jobId — is anakin-py's src/anakin/client.py, models.py,
and _http.py (the official Python SDK for the same API, built earlier this
session). Base URL https://api.anakin.io/v1, auth via the X-API-Key header,
confirmed directly from that source, not guessed.

Ground truth for the 15 endpoints added afterwards (map, crawl, the 5 Wire
operations, the 2 AI Visibility operations, the 2 browser-session
operations, and the 4 website-monitoring operations) is anakin-mcp's own
source — `anakin-mcp/src/client.ts` (the AnakinClient class: exact request
bodies, query params, and poll-terminal-status logic) and the matching files
under `anakin-mcp/src/tools/*.ts` (parameter names/defaults as exposed to
callers) — Anakin's own MCP server, not this submission's guesses. Every
request body field, query param, and poll path below is copied from those
two sources.

This module does NOT import or depend on the `anakin` SDK package itself:
Snowflake's Python UDF/procedure sandbox only allows packages from the
Snowflake Anaconda channel (or files you stage yourself via IMPORTS) — an
unpublished/private PyPI package like `anakin-py` isn't installable inside
a stored procedure's PACKAGES clause. So each endpoint is called directly
with `requests` (which *is* on the Snowflake Anaconda channel) instead of
reusing the SDK's HttpClient.

Relationship to scripts/setup.sql
----------------------------------
Snowflake supports two ways to supply a Python stored procedure's code:
inline in a `CREATE PROCEDURE ... AS $$ ... $$` block, or staged via
`IMPORTS = ('@stage/file.py')` with `HANDLER = 'file.function_name'` and
(per some, but not all, of Snowflake's own documentation pages fetched
during this build) no `AS` clause at all. Because official pages disagreed
on whether the `AS` clause is required alongside `IMPORTS` for procedures,
and every fully-verbatim, unambiguous example found in this research was
the inline `AS $$ ... $$` form, `scripts/setup.sql` uses inline code for
every procedure rather than `IMPORTS`-ing this file — to avoid shipping SQL
built on a syntax detail that could not be confirmed with certainty.

This file therefore is NOT executed by setup.sql. It exists so the same
handler logic can be read, linted, and `py_compile`-verified independently
of the (larger, duplicated-per-procedure) inline copies in setup.sql. Keep
the two in sync by hand when editing either one — see SUBMIT.md for this
tracked as a real maintainability trade-off, not hidden.

Not reproduced here: the SDK's retry-with-jitter on 429/5xx/network errors
(anakin-py's _http.py). Each call below is a single attempt with a bounded
per-request timeout and a plain exception on failure. Real, scoped-out
follow-on work — see SUBMIT.md "Not done".
"""

import time

import requests

try:
    import _snowflake  # only importable inside a running Snowflake procedure
except ImportError:  # pragma: no cover - allows py_compile / local linting outside Snowflake
    _snowflake = None

ANAKIN_BASE_URL = "https://api.anakin.io/v1"
REQUEST_TIMEOUT_SECONDS = 30


def _api_key():
    """Read the consumer's Anakin API key from the bound SECRET reference.

    The secret is declared in app/manifest.yml as the `anakin_api_key_secret`
    reference and passed to each procedure via `SECRETS = ('api_key' =
    reference('anakin_api_key_secret'))`; `_snowflake.get_generic_secret_string`
    is the accessor Snowflake provides for GENERIC_STRING secrets bound this way.
    """
    if _snowflake is None:
        raise RuntimeError("_snowflake module is only available inside Snowflake.")
    return _snowflake.get_generic_secret_string("api_key")


def _headers():
    return {
        "X-API-Key": _api_key(),
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "anakin-snowflake-native-app/0.1",
    }


def _request(method, path, json_body=None, params=None):
    url = f"{ANAKIN_BASE_URL}{path}"
    response = requests.request(
        method,
        url,
        headers=_headers(),
        json=json_body,
        params=params,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if not (200 <= response.status_code < 300):
        raise RuntimeError(
            f"Anakin API {method} {path} failed: HTTP {response.status_code} - "
            f"{response.text[:2000]}"
        )
    if not response.content:
        return {}
    return response.json()


def _job_id(submitted):
    return submitted.get("jobId") or submitted.get("job_id") or submitted.get("id")


def _is_terminal(body):
    return isinstance(body, dict) and body.get("status") in ("completed", "failed")


def _poll(path, max_wait_seconds):
    """Bounded poll loop: 1s initial delay, x1.5 backoff, capped at 10s,
    mirroring anakin-py's HttpClient.poll() defaults. Runs inside the
    calling procedure's own execution, so every second here bills the
    consumer's warehouse — see SUBMIT.md for why submit_*/check_* is the
    recommended primary pattern and this is offered only as a bounded
    convenience for short jobs.
    """
    deadline = time.monotonic() + max_wait_seconds
    wait = 1.0
    while True:
        body = _request("GET", path)
        if _is_terminal(body):
            return body
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            body["_snowflake_note"] = (
                f"Polling stopped after {max_wait_seconds}s without reaching a "
                f"terminal status; the job may still be running. Call the "
                f"matching check_* procedure again later with this jobId."
            )
            return body
        time.sleep(min(wait, remaining))
        wait = min(wait * 1.5, 10.0)


def _wire_job_id(accepted):
    """POST /wire/task's accepted-job field is `job_id` (snake_case) per
    client.ts's WireTaskAccepted type — unlike the other endpoints' jobId/
    job_id/id fallback chain, this one has exactly one documented shape.
    """
    return accepted.get("job_id") if isinstance(accepted, dict) else None


def _poll_wire_job(job_id, max_wait_seconds):
    """Poll GET /wire/jobs/:jobId to a terminal status. Mirrors client.ts's
    pollWireJob(): honors the server's `retry_after_ms` pacing hint when
    present, clamped to [500ms, 10s] — the one poll loop in this file that
    isn't a fixed 1.5x backoff, because Wire's job endpoint is the only one
    of the wrapped APIs that returns a server-suggested delay.
    """
    deadline = time.monotonic() + max_wait_seconds
    path = f"/wire/jobs/{job_id}"
    while True:
        body = _request("GET", path)
        status = body.get("status") if isinstance(body, dict) else None
        if status == "completed":
            return body
        if status == "failed":
            return body
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            body["_snowflake_note"] = (
                f"Polling stopped after {max_wait_seconds}s without reaching a "
                f"terminal status; the job may still be running. Call "
                f"core.check_wire_job('{job_id}') again later."
            )
            return body
        retry_after_ms = body.get("retry_after_ms") if isinstance(body, dict) else None
        wait_s = (retry_after_ms / 1000.0) if isinstance(retry_after_ms, (int, float)) else 3.0
        wait_s = min(max(wait_s, 0.5), 10.0)
        time.sleep(min(wait_s, remaining))


def _poll_ai_visibility(search_id, max_wait_seconds):
    """Poll GET /ai-visibility/search/:search_id. Terminal condition differs
    from every other poller here: client.ts's aiVisibilitySearch() returns
    as soon as `status !== 'running'` (so a `failed` result is returned, not
    thrown — the payload still carries per-source results/errors the caller
    needs), rather than checking for one of two named terminal values.
    """
    deadline = time.monotonic() + max_wait_seconds
    path = f"/ai-visibility/search/{search_id}"
    wait = 3.0  # matches client.ts's POLL_INTERVAL_MS = 3000
    while True:
        body = _request("GET", path)
        status = body.get("status") if isinstance(body, dict) else None
        if status != "running":
            return body
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            body["_snowflake_note"] = (
                f"Polling stopped after {max_wait_seconds}s while still 'running'; "
                f"call core.check_ai_visibility_search('{search_id}') again later."
            )
            return body
        time.sleep(min(wait, remaining))


# ─── url-scraper: POST /url-scraper (submit) + GET /url-scraper/:jobId (poll) ──


def submit_url_scraper(url, formats=None, country="us", use_browser=False,
                        generate_json=False, force_fresh=False):
    body = {
        "url": url,
        "country": country,
        "formats": list(formats) if formats else ["markdown"],
        "useBrowser": use_browser,
        "generateJson": generate_json,
        "forceFresh": force_fresh,
    }
    return _request("POST", "/url-scraper", json_body=body)


def check_url_scraper(job_id):
    return _request("GET", f"/url-scraper/{job_id}")


def scrape_url_sync(url, formats=None, country="us", use_browser=False,
                     generate_json=False, force_fresh=False, max_wait_seconds=60):
    submitted = submit_url_scraper(url, formats, country, use_browser,
                                    generate_json, force_fresh)
    job_id = _job_id(submitted)
    if not job_id:
        return submitted
    return _poll(f"/url-scraper/{job_id}", max_wait_seconds)


# ─── search: POST /search (synchronous, no polling) ─────────────────────────


def anakin_search(prompt, result_limit=5):
    body = {"prompt": prompt, "limit": result_limit}
    return _request("POST", "/search", json_body=body)


# ─── agentic-search: POST /agentic-search (submit) + GET .../:jobId (poll) ──


def submit_agentic_search(prompt, use_browser=True, schema=None):
    body = {"prompt": prompt, "useBrowser": use_browser}
    if schema is not None:
        body["schema"] = schema
    return _request("POST", "/agentic-search", json_body=body)


def check_agentic_search(job_id):
    return _request("GET", f"/agentic-search/{job_id}")


def agentic_search_sync(prompt, use_browser=True, schema=None, max_wait_seconds=90):
    submitted = submit_agentic_search(prompt, use_browser, schema)
    job_id = _job_id(submitted)
    if not job_id:
        return submitted
    return _poll(f"/agentic-search/{job_id}", max_wait_seconds)


# ─── map: POST /map (submit) + GET /map/:jobId (poll) ───────────────────────
# Body fields and defaults match AnakinClient.map() in client.ts exactly.


def submit_map(url, limit=100, depth=2, limit_per_level=100,
                include_subdomains=False, include_external_links=False,
                use_browser=False, search=None):
    body = {
        "url": url,
        "limit": limit,
        "depth": depth,
        "limitPerLevel": limit_per_level,
        "includeSubdomains": include_subdomains,
        "includeExternalLinks": include_external_links,
        "useBrowser": use_browser,
    }
    if search is not None:
        body["search"] = search
    return _request("POST", "/map", json_body=body)


def check_map(job_id):
    return _request("GET", f"/map/{job_id}")


def map_sync(url, limit=100, depth=2, limit_per_level=100,
             include_subdomains=False, include_external_links=False,
             use_browser=False, search=None, max_wait_seconds=90):
    submitted = submit_map(url, limit, depth, limit_per_level,
                            include_subdomains, include_external_links,
                            use_browser, search)
    job_id = _job_id(submitted)
    if not job_id:
        return submitted
    return _poll(f"/map/{job_id}", max_wait_seconds)


# ─── crawl: POST /crawl (submit) + GET /crawl/:jobId (poll) ─────────────────
# Body fields and defaults match AnakinClient.crawl() in client.ts exactly.


def submit_crawl(url, max_pages=10, depth=1, country="us", use_browser=False,
                  include_patterns=None, exclude_patterns=None,
                  session_id=None, session_name=None):
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
    return _request("POST", "/crawl", json_body=body)


def check_crawl(job_id):
    return _request("GET", f"/crawl/{job_id}")


def crawl_sync(url, max_pages=10, depth=1, country="us", use_browser=False,
                include_patterns=None, exclude_patterns=None,
                session_id=None, session_name=None, max_wait_seconds=120):
    submitted = submit_crawl(url, max_pages, depth, country, use_browser,
                              include_patterns, exclude_patterns,
                              session_id, session_name)
    job_id = _job_id(submitted)
    if not job_id:
        return submitted
    return _poll(f"/crawl/{job_id}", max_wait_seconds)


# ─── wire_discover: GET /wire/resolve?q=&limit= (synchronous) ───────────────


def wire_discover(q, result_limit=5):
    params = {"q": q}
    if result_limit is not None:
        params["limit"] = result_limit
    return _request("GET", "/wire/resolve", params=params)


# ─── wire_catalog: GET /wire/catalog or GET /wire/catalog/:slug (sync) ──────


def wire_catalog(slug=None):
    path = f"/wire/catalog/{slug}" if slug else "/wire/catalog"
    return _request("GET", path)


# ─── wire_read_action / wire_write_action: POST /wire/task (submit) +
# GET /wire/jobs/:jobId (poll). Both actions share the same request/poll
# shape in the real API — the read/write split is a client-side safety
# convention (separate tools with honest readOnlyHint/destructiveHint
# annotations in anakin-mcp), not two different endpoints. Mirrored here as
# two submit procedures sharing one check procedure. `params` may come back
# with the result inline (no job_id) for sync actions — both submit
# procedures return that raw response either way; call check_wire_job only
# if a job_id is present. ─────────────────────────────────────────────────


def _submit_wire_task(action_id, params=None, credential_id=None, identity_id=None):
    body = {"action_id": action_id}
    if params:
        body["params"] = params
    if credential_id is not None:
        body["credential_id"] = credential_id
    if identity_id is not None:
        body["identity_id"] = identity_id
    return _request("POST", "/wire/task", json_body=body)


def submit_wire_read_action(action_id, params=None, credential_id=None, identity_id=None):
    return _submit_wire_task(action_id, params, credential_id, identity_id)


def submit_wire_write_action(action_id, params=None, credential_id=None, identity_id=None):
    return _submit_wire_task(action_id, params, credential_id, identity_id)


def check_wire_job(job_id):
    return _request("GET", f"/wire/jobs/{job_id}")


def wire_read_action_sync(action_id, params=None, credential_id=None, identity_id=None,
                           max_wait_seconds=60):
    submitted = submit_wire_read_action(action_id, params, credential_id, identity_id)
    job_id = _wire_job_id(submitted)
    if not job_id:
        return submitted  # sync action: terminal data came back inline
    return _poll_wire_job(job_id, max_wait_seconds)


def wire_write_action_sync(action_id, params=None, credential_id=None, identity_id=None,
                            max_wait_seconds=60):
    submitted = submit_wire_write_action(action_id, params, credential_id, identity_id)
    job_id = _wire_job_id(submitted)
    if not job_id:
        return submitted
    return _poll_wire_job(job_id, max_wait_seconds)


# ─── wire_identities: GET /wire/identities?catalog_id= (synchronous) ────────


def wire_identities(catalog_id=None):
    params = {"catalog_id": catalog_id} if catalog_id is not None else None
    return _request("GET", "/wire/identities", params=params)


# ─── ai_visibility_search: POST /ai-visibility/search (submit) +
# GET /ai-visibility/search/:search_id (poll) ────────────────────────────────


def submit_ai_visibility_search(query, sources=None, country=None):
    body = {"query": query}
    if sources:
        body["sources"] = list(sources)
    if country is not None:
        body["country"] = country
    return _request("POST", "/ai-visibility/search", json_body=body)


def check_ai_visibility_search(search_id):
    return _request("GET", f"/ai-visibility/search/{search_id}")


def ai_visibility_search_sync(query, sources=None, country=None, max_wait_seconds=120):
    submitted = submit_ai_visibility_search(query, sources, country)
    search_id = submitted.get("search_id") if isinstance(submitted, dict) else None
    if not search_id:
        return submitted
    return _poll_ai_visibility(search_id, max_wait_seconds)


# ─── ai_visibility_sources: GET /ai-visibility/sources (synchronous) ────────


def ai_visibility_sources():
    return _request("GET", "/ai-visibility/sources")


# ─── session_list: GET /sessions?domain= (synchronous) ──────────────────────


def session_list(domain=None):
    params = {"domain": domain} if domain is not None else None
    return _request("GET", "/sessions", params=params)


# ─── session_delete: DELETE /sessions/:id (synchronous) ─────────────────────


def session_delete(session_id):
    return _request("DELETE", f"/sessions/{session_id}")


# ─── monitor_create: POST /monitors (synchronous — returns the created
# monitor directly, not a job to poll). `options` mirrors MonitorCreateOptions
# in client.ts (scope, watchMode, watchFormat, outputSchema, aiMode, aiGoal,
# useBrowser, country, sessionId, isActive, expiresAt, alertWebhookUrl,
# alertEmails, maxPages, maxDepth, includePatterns, excludePatterns,
# wireActionId, wireCatalogSlug, wireCredentialId, wireParams,
# wireWatchPaths) — passed through key-for-key, undefined/omitted keys left
# to the API's own defaults, exactly like monitorCreate()'s passthrough loop. ─


def monitor_create(url, interval_minutes, options=None):
    body = {"url": url, "intervalMinutes": interval_minutes}
    if options:
        body.update(options)
    return _request("POST", "/monitors", json_body=body)


# ─── monitor_list: GET /monitors, or GET /monitors/:id when id is given ─────


def monitor_list(monitor_id=None):
    if monitor_id:
        return _request("GET", f"/monitors/{monitor_id}")
    return _request("GET", "/monitors")


# ─── monitor_changes: GET /monitors/:id/changes (synchronous) ───────────────


def monitor_changes(monitor_id):
    return _request("GET", f"/monitors/{monitor_id}/changes")


# ─── monitor_control: POST /monitors/:id/pause|resume|run, or
# DELETE /monitors/:id for "delete". Note action "run_now" maps to POST
# .../run (not .../run_now) — matches client.ts's monitorControl() switch. ──


def monitor_control(monitor_id, action):
    base = f"/monitors/{monitor_id}"
    if action == "pause":
        return _request("POST", f"{base}/pause")
    if action == "resume":
        return _request("POST", f"{base}/resume")
    if action == "run_now":
        return _request("POST", f"{base}/run")
    if action == "delete":
        return _request("DELETE", base)
    raise ValueError(f'Unknown monitor action "{action}" — use pause, resume, run_now, or delete.')
