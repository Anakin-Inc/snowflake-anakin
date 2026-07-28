# Anakin API for Snowflake

SQL-native access to [Anakin](https://anakin.io)'s web scraping, site
mapping/crawling, AI search, agentic research, Wire automation catalog, AI
visibility tracking, browser sessions, and website monitoring — call it
with `CALL`, get back `VARIANT`, join it with the rest of your data. No
separate service to run, no API client library to install.

## What this wraps

18 of Anakin's REST API operations, called directly over HTTPS
(`https://api.anakin.io/v1`, `X-API-Key` auth):

| Anakin endpoint | Procedure(s) |
|---|---|
| `POST /url-scraper` + `GET /url-scraper/:jobId` | `core.submit_url_scraper`, `core.check_url_scraper`, `core.scrape_url_sync` |
| `POST /search` (synchronous) | `core.anakin_search` |
| `POST /agentic-search` + `GET /agentic-search/:jobId` | `core.submit_agentic_search`, `core.check_agentic_search`, `core.agentic_search_sync` |
| `POST /map` + `GET /map/:jobId` | `core.submit_map`, `core.check_map`, `core.map_sync` |
| `POST /crawl` + `GET /crawl/:jobId` | `core.submit_crawl`, `core.check_crawl`, `core.crawl_sync` |
| `GET /wire/resolve` (synchronous) | `core.wire_discover` |
| `GET /wire/catalog[/:slug]` (synchronous) | `core.wire_catalog` |
| `POST /wire/task` (read action) + `GET /wire/jobs/:jobId` | `core.submit_wire_read_action`, `core.check_wire_job`, `core.wire_read_action_sync` |
| `POST /wire/task` (write action) + `GET /wire/jobs/:jobId` | `core.submit_wire_write_action`, `core.check_wire_job`, `core.wire_write_action_sync` |
| `GET /wire/identities` (synchronous) | `core.wire_identities` |
| `POST /ai-visibility/search` + `GET /ai-visibility/search/:search_id` | `core.submit_ai_visibility_search`, `core.check_ai_visibility_search`, `core.ai_visibility_search_sync` |
| `GET /ai-visibility/sources` (synchronous) | `core.ai_visibility_sources` |
| `GET /sessions` (synchronous) | `core.session_list` |
| `DELETE /sessions/:id` (synchronous) | `core.session_delete` |
| `POST /monitors` (synchronous) | `core.monitor_create` |
| `GET /monitors[/:id]` (synchronous) | `core.monitor_list` |
| `GET /monitors/:id/changes` (synchronous) | `core.monitor_changes` |
| `POST /monitors/:id/pause\|resume\|run`, `DELETE /monitors/:id` | `core.monitor_control` |

Not wrapped: `POST /ai/evaluate` (`browser_task`, AI-driven browser
automation), `POST /wire/login` (interactive credential sign-in), `POST
/wire/build-request` (generates a new Wire catalog action) — out of scope
for this pass, see `../SUBMIT.md`.

`url-scraper`, `agentic-search`, `map`, `crawl`, `wire/task` (both read and
write actions), and `ai-visibility/search` are async jobs on Anakin's side,
so each has a **submit** procedure (returns immediately with a job/search
id) and a **check** procedure (one poll, call it again whenever you want a
status update). A bounded `*_sync` convenience procedure is also provided
for each — it submits and polls in a loop for you, up to `max_wait_seconds`
— but it blocks (and bills) the calling warehouse for as long as it waits,
so prefer submit/check pairs, especially from a scheduled Task, for
anything that might run longer than a few seconds. `search`, `wire_discover`,
`wire_catalog`, `wire_identities`, `ai_visibility_sources`, `session_list`,
`session_delete`, and the four `monitor_*` operations are synchronous on
Anakin's side already, so there's only one procedure for each.

Every procedure returns Anakin's raw JSON response as `VARIANT`, unchanged
— query it with ordinary Snowflake `:` field access, e.g.
`result:jobId::string` or `result:generatedJson:summary::string`.
`core.monitor_create`'s response may include an `alertWebhookSecret` field
— it is returned unmodified like everything else here (this app does not
redact it the way Anakin's own MCP server does before content reaches a
model); treat it as sensitive.

## Setup

This app needs outbound network access to `api.anakin.io` and your Anakin
API key, neither of which the app can provision for itself — Snowflake
requires you, the consumer, to explicitly grant both after installing.

1. Get a free Anakin API key (300 credits, no card required) at
   [anakin.io/dashboard](https://anakin.io/dashboard).

2. Run the following as `ACCOUNTADMIN` (or a role with
   `CREATE NETWORK RULE` / `CREATE SECRET` / `CREATE EXTERNAL ACCESS
   INTEGRATION` privileges), replacing `<your ANAKIN_API_KEY>` and
   `ANAKIN_API` (this application's name) as needed:

   ```sql
   -- 1. Allow outbound HTTPS to Anakin's API
   CREATE OR REPLACE NETWORK RULE anakin_api_network_rule
     MODE = EGRESS
     TYPE = HOST_PORT
     VALUE_LIST = ('api.anakin.io');

   -- 2. Store your Anakin API key as a Snowflake secret
   CREATE OR REPLACE SECRET anakin_api_key_secret
     TYPE = GENERIC_STRING
     SECRET_STRING = '<your ANAKIN_API_KEY>';

   -- 3. Bundle both into an External Access Integration
   CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION anakin_api_eai
     ALLOWED_NETWORK_RULES = (anakin_api_network_rule)
     ALLOWED_AUTHENTICATION_SECRETS = (anakin_api_key_secret)
     ENABLED = TRUE;

   -- 4. Bind both references the app declared in its manifest
   CALL anakin_api.config.register_single_reference(
     'anakin_api_access', 'ADD',
     SYSTEM$REFERENCE('EXTERNAL ACCESS INTEGRATION', 'anakin_api_eai', 'PERSISTENT', 'USAGE')
   );
   CALL anakin_api.config.register_single_reference(
     'anakin_api_key_secret', 'ADD',
     SYSTEM$REFERENCE('SECRET', 'anakin_api_key_secret', 'PERSISTENT', 'READ')
   );

   -- 5. Let a working role call the app's procedures
   GRANT APPLICATION ROLE anakin_api.app_public TO ROLE <your_role>;
   ```

## Usage

```sql
-- Scrape a page, wait up to 60s for the result
CALL anakin_api.core.scrape_url_sync('https://example.com');

-- Or submit + check separately (recommended for anything non-trivial)
SET job = (CALL anakin_api.core.submit_url_scraper('https://example.com'));
SELECT $job:jobId::string;
CALL anakin_api.core.check_url_scraper('<jobId from above>');

-- AI web search (synchronous, 3 credits/call)
CALL anakin_api.core.anakin_search('latest Snowflake Native App pricing', 5);

-- Agentic research (10 credits/call) — submit/check is the recommended
-- pattern here; this is the slowest of the three original operations
CALL anakin_api.core.submit_agentic_search('Summarize Snowflake''s 2026 Native App external-access changes');
CALL anakin_api.core.check_agentic_search('<jobId>');

-- Map a site's URLs before deciding what to crawl
CALL anakin_api.core.map_sync('https://example.com', 100, 2);

-- Bulk-fetch markdown across a site (submit/check recommended — can cover many pages)
SET crawl_job = (CALL anakin_api.core.submit_crawl('https://example.com', 25));
CALL anakin_api.core.check_crawl('<jobId from above>');

-- Wire: discover an action, then run it (read actions extract data, write actions change state)
CALL anakin_api.core.wire_discover('top phones on walmart', 5);
CALL anakin_api.core.wire_catalog('walmart');
CALL anakin_api.core.wire_read_action_sync('walmart.search_products', PARSE_JSON('{"query": "phones"}'));
-- Write actions typically need a credential_id — find one first:
CALL anakin_api.core.wire_identities();

-- Ask multiple AI answer engines the same question and compare (AI visibility / AI SEO)
CALL anakin_api.core.ai_visibility_sources();
CALL anakin_api.core.ai_visibility_search_sync('what do AI engines say about Snowflake Native Apps?');

-- Saved browser sessions (for login-protected scrape/crawl/map/monitor)
CALL anakin_api.core.session_list();
CALL anakin_api.core.session_delete('<session id>');

-- Website monitoring: watch a page every 60 minutes, get alerted on change
CALL anakin_api.core.monitor_create('https://example.com/pricing', 60);
CALL anakin_api.core.monitor_list();
CALL anakin_api.core.monitor_changes('<monitor id>');
CALL anakin_api.core.monitor_control('<monitor id>', 'pause');
```

## Support

`support@anakin.io` · [anakin.io](https://anakin.io) ·
[api.anakin.io](https://api.anakin.io) docs.
