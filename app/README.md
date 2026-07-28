# Anakin API for Snowflake

SQL-native access to [Anakin](https://anakin.io)'s web scraping, AI search,
and multi-stage agentic research API — call it with `CALL`, get back
`VARIANT`, join it with the rest of your data. No separate service to run,
no API client library to install.

## What this wraps

Three Anakin API operations, called directly over HTTPS
(`https://api.anakin.io/v1`, `X-API-Key` auth):

| Anakin endpoint | Procedure(s) |
|---|---|
| `POST /url-scraper` + `GET /url-scraper/:jobId` | `core.submit_url_scraper`, `core.check_url_scraper`, `core.scrape_url_sync` |
| `POST /search` (synchronous) | `core.anakin_search` |
| `POST /agentic-search` + `GET /agentic-search/:jobId` | `core.submit_agentic_search`, `core.check_agentic_search`, `core.agentic_search_sync` |

`url-scraper` and `agentic-search` are async jobs on Anakin's side, so each
has a **submit** procedure (returns immediately with a `jobId`) and a
**check** procedure (one poll, call it again whenever you want a status
update). A bounded `*_sync` convenience procedure is also provided for each
— it submits and polls in a loop for you, up to `max_wait_seconds` — but it
blocks (and bills) the calling warehouse for as long as it waits, so prefer
submit/check pairs, especially from a scheduled Task, for anything that
might run longer than a few seconds. `search` is synchronous on Anakin's
side already, so there's only one procedure for it.

Every procedure returns Anakin's raw JSON response as `VARIANT`, unchanged
— query it with ordinary Snowflake `:` field access, e.g.
`result:jobId::string` or `result:generatedJson:summary::string`.

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
-- pattern here; this is the slowest of the three operations
CALL anakin_api.core.submit_agentic_search('Summarize Snowflake''s 2026 Native App external-access changes');
CALL anakin_api.core.check_agentic_search('<jobId>');
```

## Support

`support@anakin.io` · [anakin.io](https://anakin.io) ·
[api.anakin.io](https://api.anakin.io) docs.
