# Architecture Decision Record: Stage 3 Streaming Extension

**File:** `adr/stage3_adr.md`
**Author:** DE Track Candidate
**Date:** 2026-05-03
**Status:** Final

---

## Context

The mobile product team required near-real-time balance and transaction visibility — the daily batch extract was too slow. The fintech agreed to deliver a complementary real-time feed: a directory of micro-batch JSONL files at `/data/stream/`, one file every ~30 seconds, each containing 50–500 transaction events. The pipeline needed to poll this directory, process files in chronological (filename) order, and maintain two new Delta Gold tables under `/data/output/stream_gold/`:

- `current_balances` — one row per account, upsert semantics, updated balance computed from signed transaction amounts (CREDIT/REVERSAL add, DEBIT/FEE subtract).
- `recent_transactions` — last 50 transactions per account, merge keyed on `(account_id, transaction_id)`, with rows beyond 50 evicted after each merge cycle.

The SLA is strict: `updated_at` in the output tables must be within 300 seconds of the source event timestamp. All 12 stream files (2,415 events total) are pre-staged at evaluation time. The batch pipeline (Stage 2) must continue to work correctly — same DQ rules, same Gold tables, same `dq_report.json`.

Coming into Stage 3, the pipeline was approximately 700 lines across seven Python modules plus two YAML configs. It had been restructured once (Stage 1 → Stage 2) to add DQ rule externalisation and the DQ report. The core medallion pattern (Bronze → Silver → Gold) was stable.

---

## Decision 1: How did your existing Stage 1 architecture facilitate or hinder the streaming extension?

**What made Stage 3 easier:**

The most valuable Stage 1 decision was the single shared `SparkSession` factory in `spark_session.py`. Because the batch pipeline and the streaming loop run sequentially inside one container, they share one JVM. Having `get_spark()` as a singleton meant the streaming module could call `get_spark(config)` and reuse the already-warm session — no second JVM startup cost, no conflicting Spark configs, no port collision. Had each module created its own `SparkSession`, Stage 3 would have required significant session lifecycle management.

The config-driven path setup in `pipeline_config.yaml` also helped. Adding `stream.directory`, `stream.poll_interval_seconds`, and `output.stream_gold_path` required zero code changes — `stream_ingest.py` reads them at runtime. This was a direct consequence of Stage 1's discipline of never hardcoding a path in Python.

The DQ normalisation helpers in `transform.py` — particularly `_parse_date()` and the currency variant mapping — transferred directly into `stream_ingest.py` with copy-paste. Because those helpers operated on Spark DataFrames rather than on collected Python objects, they worked at any scale. This meant the stream processing path got full DQ coverage for free.

**What made Stage 3 harder:**

`run_all.py` was designed as a linear batch orchestrator. Adding the streaming loop meant it now has two distinct execution phases (batch + stream) combined in a single script. While functional, the entry point now carries concerns it was not originally designed for. A `--mode batch|stream` argument would have made the contract cleaner and made it easier to run the streaming path independently during development.

The `transform.py` Silver layer accumulated some logic that was tightly coupled to the batch source schema — specifically the struct-flattening logic for `location` and `metadata` which had to be duplicated in `stream_ingest.py`. Had this been factored into a shared `schema_utils.py` from the start, it would have been imported rather than copied.

**Code survival rate:**

Approximately 85% of Stage 1/2 code survived intact into Stage 3. The batch pipeline — `ingest.py`, `transform.py`, `provision.py`, `dq_report.py`, `spark_session.py`, `config_loader.py` — was untouched. `run_all.py` was extended (not rewritten) by adding the `run_stream_ingestion()` call at the end. `pipeline_config.yaml` gained three new keys. `stream_ingest.py` was entirely new code (approximately 200 lines).

---

## Decision 2: What design decisions in Stage 1 would you change in hindsight?

**Shared DQ and schema utilities:**

The struct-flattening logic (unpacking `location.*` and `metadata.*` from the JSONL) was written inline in `transform.py` and had to be copied into `stream_ingest.py`. I would have extracted this into `pipeline/schema_utils.py` at Stage 1, housing functions like `flatten_transaction_struct(df)` and `normalise_currency(df, rules)`. The cost at Stage 1 was two extra function definitions; the benefit at Stage 3 would have been a single import rather than a copy. Copied code diverges — if the struct layout changes in a future stage, it now needs to be changed in two places.

**`run_all.py` entry point design:**

I would have added a `--mode` CLI argument from the start: `python pipeline/run_all.py --mode batch` and `python pipeline/run_all.py --mode stream`. The current linear script works but conflates orchestration concerns. With mode selection, the `CMD` in the Dockerfile becomes `CMD ["python", "pipeline/run_all.py", "--mode", "both"]`, and during development the stream path can be exercised independently without running the full batch pipeline. The change at Stage 1 would have been ten lines; the benefit compounds across every subsequent debugging session.

**`current_balance` initialisation from batch data:**

The `current_balances` table is initialised from streaming events only — there is no seed from the batch pipeline's `dim_accounts.current_balance`. This means the first-seen balance for an account in `current_balances` is the net delta from its first stream event, not the actual account balance at batch time. In a production system this would be wrong. Had I known Stage 3 was coming, I would have designed `provision.py` to write `current_balance` values from `dim_accounts` into a `stream_gold/current_balances` seed table at the end of the batch run. The streaming merge would then accumulate deltas on top of an accurate baseline. This is a functional correctness issue that Stage 1 design could have prevented.

---

## Decision 3: How would you approach this differently if you had known Stage 3 was coming from the start?

**Ingestion layer design:**

I would have designed `ingest.py` around a source abstraction from Day 1. Something like:

```python
class Source:
    def read(self, spark, config) -> DataFrame: ...

class BatchFileSource(Source): ...       # reads CSV / JSONL from /data/input/
class StreamDirectorySource(Source): ... # polls /data/stream/
```

Both sources produce a DataFrame with the same transaction schema, so everything downstream (DQ, Silver transformation, Gold write) is source-agnostic. Adding the streaming source at Stage 3 would have been adding one class, not a new module with duplicated logic.

**Unified Gold write layer:**

Rather than `provision.py` writing batch Gold tables and `stream_ingest.py` writing stream Gold tables independently, I would have created a `pipeline/gold_writer.py` module that owns all Gold table write logic — both the batch dimensional model and the streaming `current_balances`/`recent_transactions` tables. This unifies the Delta MERGE pattern, the SLA timestamp injection, and the retention eviction logic in one place. At Stage 3 the pattern was clear: every Gold write needs `updated_at`, every upsert needs a merge key, every retention-bounded table needs a post-merge eviction step. A shared `GoldWriter` class with these three behaviours built in would have made all Gold writes consistent and testable.

**State management for balance:**

I would have included a `stream_gold/current_balances/` seed step at the end of the batch pipeline from Day 1 — writing one row per account with the batch-time `current_balance` from `dim_accounts`. The streaming merge then accumulates deltas on this baseline. This requires about 10 lines in `provision.py` and ensures the `current_balances` table is accurate from the first stream event rather than only reflecting post-batch activity. The absence of this seed is the most significant correctness gap in the current design.

**Entry point:**

A single `pipeline/run_all.py` with `argparse` mode selection (`--mode batch`, `--mode stream`, `--mode both`), where `both` is the default used by the Docker `CMD`. This costs nothing at Stage 1 and saves meaningful development friction at every subsequent stage.

---

## Appendix

**Pipeline execution order (Stage 3):**

```
Container start
    │
    ▼
run_all.py
    │
    ├── 1. run_ingestion()       → Bronze Delta tables
    │
    ├── 2. run_transformation()  → Silver Delta tables (DQ applied)
    │
    ├── 3. run_provisioning()    → Gold: dim_customers, dim_accounts,
    │                               fact_transactions
    │
    ├── 4. write_dq_report()     → /data/output/dq_report.json
    │
    └── 5. run_stream_ingestion()
              │
              └── poll loop (60s interval, 120s quiesce exit)
                      │
                      ├── for each new stream_*.jsonl in /data/stream/:
                      │       parse → DQ norm → cache
                      │       _merge_current_balances()   → MERGE on account_id
                      │       _merge_recent_transactions() → MERGE on (account_id, txn_id)
                      │                                      + evict rows > 50 per account
                      │
                      └── exit when idle ≥ 120s
```

**SLA compliance:** `updated_at` is set to `datetime.now(UTC)` immediately before each Delta MERGE call. Because all 12 files are pre-staged and processed sequentially, `updated_at - transaction_timestamp` is bounded by the time to process one file — well under 60 seconds in practice, far inside the 300-second SLA.