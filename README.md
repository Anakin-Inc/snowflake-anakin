# snowflake-anakin

A Snowflake Native App wrapping [Anakin](https://anakin.io)'s API — web
scraping, AI search, and multi-stage agentic research — callable via plain
SQL `CALL` statements, for listing on Snowflake Marketplace via Provider
Studio.

## What's here

```
snowflake.yml           Snowflake CLI project definition (snow app run / snow app deploy)
app/manifest.yml         Native App manifest — artifacts + references (EAI + secret)
app/scripts/setup.sql     Setup script: reference callbacks + 7 Snowpark Python procedures
app/python/anakin_procs.py  Same handler logic, standalone + py_compile-checked
app/README.md             In-app README shown to consumers in Snowsight (setup + usage)
SUBMIT.md                 Submission notes: what's verified vs. uncertain vs. not done
```

## Install (provider side — needs a Snowflake account + Snowflake CLI)

```sh
snow app run       # creates/upgrades the application package + app in a dev account
```

## Usage (consumer side, after granting the External Access Integration — see app/README.md)

```sql
CALL anakin_api.core.scrape_url_sync('https://example.com');
CALL anakin_api.core.anakin_search('latest Snowflake Native App pricing', 5);
CALL anakin_api.core.submit_agentic_search('Summarize Snowflake''s 2026 Native App external-access changes');
```

Get a free Anakin API key at [anakin.io/dashboard](https://anakin.io/dashboard) —
300 credits, no card required.

## Verify (syntax-level only — see SUBMIT.md)

```sh
python3 -m py_compile app/python/anakin_procs.py
python3 -c "import yaml, sys; yaml.safe_load(open('app/manifest.yml')); yaml.safe_load(open('snowflake.yml')); print('yaml ok')"
```

No Snowflake CLI, warehouse, or account was available in this sandbox — the
SQL was never run against a live Snowflake instance. See SUBMIT.md for the
precise boundary between what was verified and what wasn't.
