"""
Reference implementation of the Python handler logic used by the Anakin
Native App's Snowpark stored procedures (see ../scripts/setup.sql).

Ground truth for the three wrapped endpoints — POST /url-scraper + GET
/url-scraper/:jobId, POST /search, POST /agentic-search + GET
/agentic-search/:jobId — is anakin-py's src/anakin/client.py, models.py,
and _http.py (the official Python SDK for the same API, built earlier this
session). Base URL https://api.anakin.io/v1, auth via the X-API-Key header,
confirmed directly from that source, not guessed.

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
