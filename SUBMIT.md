# Snowflake Native App — submission instructions

This row was previously flagged and deliberately deferred as "large —
Snowflake Native Apps need Snowpark app packaging, a different framework
than any integration built this session." That's accurate: this is a real
and materially different framework from every other submission in this
batch (no `pip install`, no `gh api` template pull — a manifest + SQL
setup-script + a consumer-granted network permission model, packaged for
Snowflake's own CLI and reviewed for Marketplace listing through Provider
Studio, a different product surface than a code-hosting registry). Built
now, on request, with the scope boundary stated precisely below rather than
glossed over.

## What's here

```
snowflake.yml              Snowflake CLI project definition (snow app run / snow app deploy)
app/manifest.yml            Native App manifest — artifacts + references (EAI + secret)
app/scripts/setup.sql        Setup script: 2 reference callbacks + 31 Snowpark Python procedures
app/python/anakin_procs.py    Same handler logic, standalone + py_compile-checked
app/README.md                 In-app README shown to consumers in Snowsight
README.md                    This submission's top-level overview (dagster-anakin's convention)
SUBMIT.md                    This file
```

**Coverage update (this pass):** the initial build wrapped 3 of Anakin's 21
REST API operations (url-scraper, search, agentic-search). This pass adds
15 more — map, crawl, the 5 Wire operations (wire_discover, wire_catalog,
wire_read_action, wire_write_action, wire_identities), the 2 AI Visibility
operations, the 2 browser-session operations, and the 4 website-monitoring
operations — bringing coverage to 18 of 21. Ground truth for these 15 is
Anakin's own MCP server source, `anakin-mcp/src/client.ts` (exact request
bodies, query params, poll-terminal-status logic) and the matching files
under `anakin-mcp/src/tools/*.ts` (parameter names/defaults/behavior notes
as exposed to callers) — read directly, not guessed. Still not wrapped:
`ai/evaluate` (browser_task — an AI-driven browser automation endpoint with
its own longer poll window and a different accepted-job shape,
`workflow_id` not `jobId`/`job_id`), `wire/login` (interactive credential
sign-in), and `wire/build-request` (generates a new catalog action) — all
three are real, documented endpoints in client.ts, scoped out of this pass
by the task's own capability list, not overlooked. `app/scripts/setup.sql`
and `app/python/anakin_procs.py` both flag this explicitly at the top.

## Real Native App structure confirmed — not assumed

All fetched live from `docs.snowflake.com` (or, where a fetch returned a
summarized/partial answer rather than verbatim text, corroborated by a
second independent source, noted explicitly below) during this session,
not recalled from training data:

- **Core structure** —
  `docs.snowflake.com/en/developer-guide/native-apps/native-apps-about`.
  Confirms an Application Package "encapsulates the data content,
  application logic, metadata, and setup script"; the setup script "Contains
  SQL statements that are run when the consumer installs or upgrades an
  application"; on install "Snowflake creates the application and runs the
  setup script to create the required objects."
- **manifest.yml required fields** —
  `docs.snowflake.com/en/developer-guide/native-apps/manifest-reference`
  plus corroborating search results. Confirms `manifest_version`,
  `artifacts` (block), and `artifacts.setup_script` are required; `artifacts
  .readme` is the consumer-facing readme path. `app/manifest.yml` sets all
  three plus the `references` block (below).
- **snowflake.yml (Snowflake CLI project definition)** —
  `docs.snowflake.com/en/developer-guide/snowflake-cli/native-apps/project-definitions`.
  Confirms `definition_version: 2`, an `entities` map with `type:
  "application package"` / `type: "application"`, and `artifacts: [{src,
  dest}]` mapping — `snowflake.yml` in this submission mirrors the fetched
  minimal example's shape field-for-field.
- **External Access Integration + Network Rule mechanics (the
  general/account-level version)** —
  `docs.snowflake.com/en/developer-guide/external-network-access/creating-using-external-network-access`
  and `docs.snowflake.com/en/sql-reference/sql/create-external-access-integration`,
  corroborated by a targeted web search returning the same syntax from two
  independent sources. Confirms `CREATE NETWORK RULE ... MODE = EGRESS TYPE
  = HOST_PORT VALUE_LIST = (...)`, `CREATE EXTERNAL ACCESS INTEGRATION ...
  ALLOWED_NETWORK_RULES = (...) ALLOWED_AUTHENTICATION_SECRETS = (...)
  ENABLED = true`, and `CREATE FUNCTION/PROCEDURE ... EXTERNAL_ACCESS_
  INTEGRATIONS = (integration_name) PACKAGES = ('requests') SECRETS =
  ('cred' = secret_name)` — the exact Google-Translate worked example
  quoted verbatim by the fetch was used to confirm every clause name.
- **The Native-App-specific mechanism (references)** — this is the part
  that's genuinely different from a plain per-account external access
  integration, and where the most care went in. Confirmed across three
  sources that corroborate each other's SQL:
  - `docs.snowflake.com/en/developer-guide/native-apps/requesting-refs` —
    verbatim `register_single_reference` callback procedure using
    `SYSTEM$SET_REFERENCE` / `SYSTEM$REMOVE_REFERENCE` /
    `SYSTEM$REMOVE_ALL_REFERENCES`, and the consumer-side invocation shape
    `CALL app.config.register_single_reference('ref_name', 'ADD',
    SYSTEM$REFERENCE(...))`.
  - `docs.snowflake.com/en/developer-guide/native-apps/requesting-example-oauth`
    — verbatim `references:` YAML block showing `object_type: EXTERNAL
    ACCESS INTEGRATION` (space-separated, not `EXTERNAL_ACCESS_INTEGRATION`
    — this detail mattered and is used correctly in `app/manifest.yml`),
    plus `object_type: SECRET`, `register_callback`, `configuration_callback`,
    `required_at_setup`.
  - `hakkoda.io/resources/using-requests-to-access-external-endpoints` (a
    third-party but technically detailed walkthrough) — independently
    corroborates the same references block, the `config.get_config_for_ref`
    Python callback returning `{"type": "CONFIGURATION", "payload":
    {"host_ports": [...], "allowed_secrets": "ALL"}}` for an EAI reference
    and `{"type": "CONFIGURATION", "payload": {"type": "GENERIC_STRING"}}`
    for a secret reference, and the `EXTERNAL_ACCESS_INTEGRATIONS =
    (reference('name'))` / `SECRETS = ('x' = reference('name'))` syntax for
    consuming a bound reference inside a procedure definition. All three
    sources agree; `app/scripts/setup.sql`'s `config.register_single_reference`
    and `config.get_config_for_ref` procedures follow this pattern closely.
- **A second, newer Snowflake-documented pattern exists and was
  deliberately not used** — `manifest_version: 2` plus a top-level
  `privileges:` block (e.g. `CREATE EXTERNAL ACCESS INTEGRATION`) combined
  with "app specifications" that the consumer approves, per
  `docs.snowflake.com/en/developer-guide/native-apps/requesting-app-specs-eai`
  and `.../requesting-auto-privs`. Both pages, when fetched, explicitly
  state this is what Snowflake now recommends over "the manual method"
  (references) — but neither fetch surfaced the exact consumer-side SQL for
  approving an app specification (the pages describe the concept and show
  the provider-side `ALTER APPLICATION ... SET SPECIFICATION` syntax, but
  not a concrete, verbatim consumer-approval command). Given that gap, this
  submission uses the older but *fully and consistently* verbatim-confirmed
  references pattern instead of building on a mechanism whose consumer-side
  half couldn't be pinned down with the same confidence. This is a
  documented trade-off, not an oversight — flagged explicitly as something
  worth revisiting if Snowflake's newer pattern's consumer SQL can be
  confirmed directly (e.g. against a real trial account, or Snowflake's own
  quickstart repos) before this is actually submitted.

## Async execution design — and why

**Decision: `submit_*` / `check_*` procedure pairs are the primary,
documented pattern for the two async Anakin endpoints (`url-scraper`,
`agentic-search`); a bounded `*_sync` convenience procedure is offered
alongside each, capped by a `max_wait_seconds` argument, not as the
default recommendation.**

The task asked specifically whether a Snowpark Python procedure can
actually run anakin-py's own poll-loop pattern (`HttpClient.poll()`:
sleep, GET, repeat until terminal, up to a timeout) inside one call, or
whether a submit/check split is more realistic. Both are technically
possible — the deciding factor turned out to be cost, not a hard platform
ceiling:

- **A Python stored procedure genuinely can sleep and loop.**
  `time.sleep()` inside a Python UDF/procedure handler works normally, and
  Snowflake's own default `STATEMENT_TIMEOUT_IN_SECONDS` is **172800
  seconds (2 days)** — confirmed via a targeted search of Snowflake's
  parameter documentation and independently corroborated community
  sources, not asserted from memory. So nothing in the platform itself
  would kill a procedure that polls Anakin for, say, 5 minutes.
- **But the warehouse running that procedure is billed per second for
  every second it's blocked.** anakin-py's own `HttpClient.poll()` costs
  the *caller's* CPU nothing while it sleeps — it's a local Python process.
  A Snowpark procedure doing the identical sleep loop instead holds a
  Snowflake virtual warehouse "active" (and billed) for the entire wait,
  turning a free client-side pattern into paid compute with no added
  value. For a fast job this is negligible; for `agentic-search`
  specifically — described in anakin-py's own docstring as a "multi-stage
  AI research pipeline" costing 10 credits, clearly the heaviest of the
  three operations — an in-procedure poll loop is the wrong default.
- **Snowflake's own idiomatic answer to "run something async, then check
  on it later" is a scheduled Task** (`CREATE TASK ... SCHEDULE = ... AS
  CALL core.check_url_scraper(...)`), not a blocking loop inside one
  session. The submit/check split is what makes that pattern possible at
  all — a single fused procedure can't be polled by a Task without
  re-submitting the job every time.

So: `core.submit_url_scraper` / `core.check_url_scraper` and
`core.submit_agentic_search` / `core.check_agentic_search` are the
procedures documented as the recommended pattern in both `app/README.md`
and inline SQL comments. `core.scrape_url_sync` and
`core.agentic_search_sync` exist for convenience (genuinely useful for a
quick ad hoc `CALL` in a worksheet) with the cost trade-off stated directly
in their `COMMENT` clause and default caps kept small (60s / 90s) rather
than defaulting to anakin-py's much longer 300s `poll_timeout`.
`core.anakin_search` needed none of this — confirmed directly from
anakin-py's `client.py` docstring ("Synchronous — returns directly, no
polling") and implemented as a single `POST /search` call.

**The same reasoning was applied consistently to all 15 endpoints added
afterwards.** map, crawl, wire_read_action, wire_write_action, and
ai_visibility_search are async on Anakin's side (each returns a `jobId` /
`job_id` / `search_id` to poll), so each got a submit_*/check_* pair plus a
bounded *_sync convenience, exactly like url-scraper and agentic-search.
wire_discover, wire_catalog, wire_identities, ai_visibility_sources,
session_list, session_delete, monitor_create, monitor_list,
monitor_changes, and monitor_control are synchronous on Anakin's side (no
job/poll cycle exists in the real API for any of them, confirmed from
`anakin-mcp/src/client.ts` — e.g. `monitorCreate()` calls `request('POST',
'/monitors', body)` once and returns; there is no `/monitors/:id/status` to
poll), so each got exactly one procedure, matching `core.anakin_search`'s
precedent rather than inventing a poll cycle the API doesn't have.

Two behavioral details worth flagging explicitly because they differ from
the fixed-backoff poll loop used everywhere else in this file:
`core.wire_read_action_sync` / `core.wire_write_action_sync` honor the
Wire job's server-suggested `retry_after_ms` pacing hint (clamped to
[500ms, 10s]) instead of a fixed 1.5x backoff, matching client.ts's
`pollWireJob()` exactly; and `core.ai_visibility_search_sync` treats any
status other than `"running"` as terminal (so a `"failed"` result is
returned, not raised, since the payload still carries per-source
results/errors the caller needs) instead of checking for named
`completed`/`failed` values, matching client.ts's `aiVisibilitySearch()`
exactly.

## Ground truth for the Anakin API — read from source, not guessed

The original 3 wrapped operations and their exact request/response field
names (`jobId`, camelCase body fields like `useBrowser`/`generateJson`/
`forceFresh`, the `status` terminal values `completed`/`failed`) were read
directly from `anakin-py/src/anakin/client.py`, `models.py`, and `_http.py`
— the real SDK built earlier this session — not inferred from this
submission's own guesses:

- `POST /url-scraper` (submit) + `GET /url-scraper/:jobId` (poll) —
  matches `Anakin.scrape()` in `client.py` exactly, including the
  `_require_job_id` fallback chain (`jobId` / `job_id` / `id`) reused
  verbatim in `anakin_procs.py`'s `_job_id()`.
- `POST /search` — matches `Anakin.search()`: synchronous, `{prompt,
  limit}` body, no polling, "Costs 3 credits per call" per its docstring
  (quoted in `app/scripts/setup.sql`'s `COMMENT` clause).
- `POST /agentic-search` (submit) + `GET /agentic-search/:jobId` (poll) —
  matches `Anakin.agentic_search()`: `{prompt, useBrowser, schema?}` body,
  "Costs 10 credits" per its docstring.
- Base URL `https://api.anakin.io/v1` and the `X-API-Key` header — from
  `_http.py`'s `DEFAULT_BASE_URL` and `HttpClient.__init__`'s
  `headers={"X-API-Key": ...}`.
- **300 credits, no card required** on the free signup grant — using the
  corrected figure this session's `aws-marketplace-anakin/SUBMIT.md`
  established (checked directly against live product code, Prisma schema
  default, and signup-page copy), not the older "500 credits" figure
  `dagster-anakin/README.md` and several other earlier submissions this
  session used before that correction was found.

The 15 operations added afterwards were read directly from **anakin-mcp's
own source** — `anakin-mcp/src/client.ts` (the `AnakinClient` class: every
request body field, query param, and poll-terminal-status check) and the
matching files under `anakin-mcp/src/tools/*.ts` (parameter names/defaults/
behavior notes as exposed to MCP callers, useful for confirming intent
alongside the wire format) — Anakin's own MCP server, a second independent
real codebase, not this submission's guesses:

- `POST /map` (submit) + `GET /map/:jobId` (poll) — body fields and
  defaults (`limit: 100`, `depth: 2`, `limitPerLevel: 100`,
  `includeSubdomains: false`, `includeExternalLinks: false`, `useBrowser:
  false`, optional `search`) match `AnakinClient.map()` exactly.
- `POST /crawl` (submit) + `GET /crawl/:jobId` (poll) — body fields and
  defaults (`maxPages: 10`, `depth: 1`, `country: "us"`, `useBrowser:
  false`, optional `includePatterns`/`excludePatterns`/`sessionId`/
  `sessionName`) match `AnakinClient.crawl()` exactly.
- `GET /wire/resolve?q=&limit=` — matches `wireResolve()`.
- `GET /wire/catalog` or `GET /wire/catalog/:slug` — matches
  `wireCatalog()`.
- `POST /wire/task` (submit, `{action_id, params?, credential_id?,
  identity_id?}`) + `GET /wire/jobs/:jobId` (poll) — matches `wireRun()` /
  `pollWireJob()` exactly, including the `job_id` (not `jobId`) accepted-
  job field name per the `WireTaskAccepted` type, and the
  `retry_after_ms` pacing hint honored by `pollWireJob()`. Split into
  `core.submit_wire_read_action` / `core.submit_wire_write_action` (sharing
  one `core.check_wire_job`) to mirror anakin-mcp's own `wire_read_action`
  / `wire_write_action` tool split — the same underlying endpoint, split
  client-side as a safety convention (tools/wire.ts explicitly documents
  this: "This separation is mandatory for the Connectors Directory, which
  rejects a single catch-all tool that mixes safe (read) and unsafe
  (write) operations").
- `GET /wire/identities?catalog_id=` — matches `wireIdentities()`.
- `POST /ai-visibility/search` (submit) + `GET
  /ai-visibility/search/:search_id` (poll) — matches
  `aiVisibilitySearch()` exactly, including its non-standard terminal
  condition (`status !== 'running'`, not a named completed/failed pair).
- `GET /ai-visibility/sources` — matches `aiVisibilitySources()`.
- `GET /sessions?domain=` — matches `sessionsList()`.
- `DELETE /sessions/:id` — matches `sessionDelete()`.
- `POST /monitors`, `GET /monitors[/:id]`, `GET /monitors/:id/changes`,
  `POST /monitors/:id/pause|resume|run`, `DELETE /monitors/:id` — match
  `monitorCreate()`/`monitorList()`/`monitorGet()`/`monitorChanges()`/
  `monitorControl()` exactly, including the `run_now` action mapping to
  `POST .../run` (not `.../run_now` — a naming mismatch in the real API
  that would have been an easy, silent bug to introduce by guessing
  instead of reading `monitorControl()`'s switch statement directly).
- Not wrapped: `POST /ai/evaluate` + `GET /ai/jobs/:id` (`browser_task` —
  its own accepted-job field is `workflow_id`, not `jobId`/`job_id`, and
  its poll window is 6 minutes vs. everything else's 3), `POST /wire/login`
  (interactive credential sign-in), and `POST /wire/build-request` (builds
  a brand-new catalog action). All three are real, documented in
  `client.ts`, and excluded because they were outside the task's stated
  capability list for this pass, not because anything about them was
  unclear.

## Verified, not assumed — file mechanics

- `python3 -m py_compile app/python/anakin_procs.py` — passes (all 33
  module-level functions across the original 3 operations and the 15
  added afterwards).
- Every one of the **32 inline Python handler blocks embedded in
  `app/scripts/setup.sql`** (the `config.get_config_for_ref` callback plus
  all 31 `core.*` procedures) was individually extracted and run through
  `py_compile` in isolation — **all 32 pass**. This matters because
  `setup.sql`'s handlers are hand-copied from `anakin_procs.py` (see that
  file's docstring for why — real doc ambiguity on the `IMPORTS`+`AS`
  clause requirement, see below), so each copy needed its own independent
  syntax check rather than trusting they matched. Verified programmatically
  (extract every `AS\n$$\n...\n$$;` block via regex, `py_compile` each in
  isolation), not by eye.
- `app/manifest.yml` and `snowflake.yml` both parse cleanly with PyYAML
  (`yaml.safe_load`), confirmed structurally: `manifest.yml` has the 3
  required top-level keys (`manifest_version`, `artifacts`, `references`)
  and both `references` entries carry `object_type`, `register_callback`,
  `configuration_callback` as expected — unchanged by this pass, since
  every new procedure reuses the same two references (`anakin_api_access`,
  `anakin_api_key_secret`), no new EAI/secret needed; `snowflake.yml` has
  `definition_version: 2` and both `pkg`/`app` entities.
- `setup.sql`'s `$$ ... $$` handler-body delimiters are balanced: 33
  `CREATE PROCEDURE ... AS $$ ... $$;` bodies (the SQL `register_single_
  reference` callback + the Python `get_config_for_ref` callback + 31
  `core.*` procedures) = 66 delimiters, plus one `--` comment line that
  mentions `$$ ... $$` in prose (2 more, inert inside a SQL comment) = 68
  total, an even count — verified programmatically by counting, not by eye
  — and parenthesis depth across the whole file balances (691 open, 691
  close), i.e. no unclosed `CREATE PROCEDURE(...)` argument lists anywhere
  in the file, old or new.
- All 33 `COMMENT = '...'` string literals in `setup.sql` were checked for
  correctly-escaped internal apostrophes (`''`) by counting `COMMENT = '`
  occurrences against a regex that only matches a *properly terminated*
  quoted string — counts matched (31; two procedures have no `COMMENT`
  clause), so no unescaped apostrophe silently breaks a string literal
  into invalid SQL.
- No tabs in either YAML file (YAML requires spaces for indentation).

## Genuinely uncertain — flagged, not hidden

- **`SYSTEM$REFERENCE('EXTERNAL ACCESS INTEGRATION', 'anakin_api_eai',
  'PERSISTENT', 'USAGE')`'s exact argument list for the `EXTERNAL ACCESS
  INTEGRATION` and `SECRET` object types** (used in `app/README.md`'s
  step 4) is **inferred**, not verbatim-confirmed. The only fully verbatim
  `SYSTEM$REFERENCE` example found in this session's research was for a
  `TABLE` reference (`SYSTEM$REFERENCE('table', 'db1.schema1.table1',
  'PERSISTENT', 'SELECT', 'INSERT')`), which establishes the general shape
  `(object_type, object_name, persistence, ...privileges)` — this
  submission's EAI/SECRET calls follow that same shape with the privileges
  declared in `manifest.yml` (`USAGE` / `READ`), which is a reasonable and
  consistent inference, but it was not found written out verbatim for
  these two specific object types on any page fetched this session.
- **`CREATE SECRET ... TYPE = GENERIC_STRING SECRET_STRING = '...'`**
  syntax is used in `app/README.md`'s consumer setup steps with moderate,
  not high, confidence — it matches the well-known, widely-documented
  Snowflake secret-object pattern and is consistent with the
  `ALLOWED_AUTHENTICATION_SECRETS` / `SECRETS = ('cred' = secret_name)`
  usage confirmed verbatim elsewhere, but this exact `CREATE SECRET`
  statement wasn't itself pulled verbatim from a fetched page this
  session.
- **Whether `IMPORTS`-staged Python procedure handlers require an `AS $$
  $$` clause** — two official-looking fetches disagreed (one said the `AS`
  clause is always required, a search-result synthesis said it's omitted
  for staged handlers). Rather than guess, this submission sidesteps the
  question entirely: every procedure in `setup.sql` uses the inline `AS $$
  ... $$` form, which every fully verbatim example found this session used
  without exception. `app/python/anakin_procs.py` exists as an
  independently compilable copy of the same logic specifically because of
  this — see that file's docstring.
- **The newer `manifest_version: 2` / app-specification pattern's
  consumer-approval SQL** — see "A second, newer Snowflake-documented
  pattern" above. Not used, and not fully understood at the SQL-syntax
  level, by design (the references pattern used instead was the one that
  could be confirmed with confidence).

## Not done — and why, stated plainly

- **Nothing here was run against a real Snowflake account, warehouse, or
  the Snowflake CLI.** There is no `snow` CLI installed and no Snowflake
  account credentials available in this sandbox. `snow app run`, `snow app
  deploy`, `CREATE APPLICATION PACKAGE`, installing the app, granting the
  external access integration, and calling any of the 31 `core.*`
  procedures against the live Anakin API were **never attempted** — this
  is expected and stated directly, not a gap being minimized. Everything
  marked "Verified" above is syntax-level (Python compiles, YAML parses,
  delimiters balance) or research-level (matches Snowflake's own
  documentation and cross-corroborated third-party examples, or matches
  anakin-mcp's own source for the 15 newer procedures); nothing here
  constitutes proof the app actually installs or runs.
- **No financial-transaction guard on `core.submit_wire_write_action` /
  `core.wire_write_action_sync`.** anakin-mcp's own `tools/wire.ts` +
  `tools/policy.ts` refuse write actions that look like a payment/fund
  transfer (checkout, buy now, wire transfer, etc.) as a defense-in-depth
  measure required by Anthropic's Connectors Directory policy for MCP
  connectors. This Snowflake submission's write-action procedures are, by
  design, thin unmodified passthroughs to `POST /wire/task` — like every
  other procedure in this file — with no equivalent guard, and Snowflake
  Marketplace's own review criteria (a different product surface, not the
  Connectors Directory) were not researched to know whether an equivalent
  restriction is expected there. Flagged here rather than silently
  ported or silently omitted — a real decision for whoever reviews this
  before it's actually submitted.
- **Provider Studio listing itself was not attempted.** Publishing this
  as a Snowflake Marketplace listing needs a Snowflake **Provider**
  account (a real, separate onboarding step from a regular consumer
  account), an application package actually pushed to Snowflake, and then
  the Provider Studio listing flow (category, pricing — this would follow
  the same "free" reasoning `aws-marketplace-anakin/listing.md` already
  worked through for AWS, but Snowflake's own free/paid listing options
  weren't researched in this pass — support contact, security review).
  Snowflake's manual review for listings that use External Access
  Integration is documented to include specific scrutiny of what hosts the
  app can reach, which this submission's tightly-scoped single-host
  network rule (`api.anakin.io` only, nothing broader) is built to pass
  cleanly, but that review was never actually run against it.
- **No test suite.** Unlike `dagster-anakin` (which at least has
  `pytest`-shaped test files, even if never run), this submission has no
  automated tests — Snowflake Python procedure handlers are awkward to
  unit-test without either a live Snowflake connection or mocking
  `_snowflake`/`requests` by hand, and building that harness was judged
  lower-value than getting the manifest/setup.sql/references design itself
  right, given the size of this task already. A real follow-on: mock
  `requests.request` and `_snowflake.get_generic_secret_string` in
  `anakin_procs.py`'s functions and assert on request shape (URL, headers,
  JSON body) per operation — straightforward, just not done here.
- **Retry/backoff on 429/5xx/network errors is not implemented** — every
  HTTP call in `anakin_procs.py` / `setup.sql` is a single attempt with a
  30s timeout and a plain exception on any non-2xx response. anakin-py's
  own `_http.py` has jittered exponential backoff on retryable statuses;
  reproducing that here was scoped out and flagged in both files' comments
  rather than silently dropped.
- **No Provider Studio listing copy** (title, category, pricing dimension,
  support URL) was drafted — out of scope for this pass, which the task
  named as manifest/setup.sql/python/README/SUBMIT specifically. Worth a
  follow-on pass modeled on `aws-marketplace-anakin/listing.md`'s
  structure if this actually gets submitted.

## Steps (needs the account owner)

1. Get a Snowflake account with the Native Apps Framework enabled and (for
   eventual listing) Provider Studio access — a business decision/account
   this sandbox has no path to.
2. Install the Snowflake CLI, run `snow app run` from this directory to
   create a dev application package + application, and actually exercise
   all 31 `core.*` procedures against live Anakin credentials — the one
   verification step this session structurally could not do.
3. Resolve the two "genuinely uncertain" `SYSTEM$REFERENCE`/`CREATE SECRET`
   syntax points above against real Snowflake CLI error messages (or
   Snowflake's own quickstart/sample-app GitHub repos, not fetched this
   session) before treating `app/README.md`'s setup SQL as final.
4. Submit through Provider Studio once the app is confirmed working live.
