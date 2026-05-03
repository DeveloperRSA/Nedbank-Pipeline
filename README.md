# Nedbank DE Challenge  Stage 2 Solution

## What Changed from Stage 1

| Change | How it was handled |
|---|---|
| 3× data volume | Spark config tightened: `driver.memory=512m`, `executor.memory=1g`, `shuffle.partitions=4`  read each file once, no `.toPandas()` |
| 6 DQ issue types | All detection + handling logic driven by `config/dq_rules.yaml`. No hardcoded DQ logic in Python. |
| `merchant_subcategory` field | Added as `NULL` column in Silver when absent from source (Stage 1 compat). Present in `fact_transactions` at position 9. |
| DQ report | Written to `/data/output/dq_report.json` by `pipeline/dq_report.py` |

## Structure

```
Dockerfile
requirements.txt
README.md
pipeline/
  run_all.py          ← orchestrator; records timing, writes DQ report
  ingest.py           ← Bronze: raw ingest; quarantines NULL account_id rows
  transform.py        ← Silver: dedup, date parse, currency norm, type cast
  provision.py        ← Gold: dims + fact; quarantines ORPHANED_ACCOUNT rows
  dq_report.py        ← assembles + writes /data/output/dq_report.json
  spark_session.py    ← shared SparkSession factory (created once)
  config_loader.py    ← YAML config + DQ rules loader
config/
  pipeline_config.yaml
  dq_rules.yaml       ← all 6 DQ rules with detection + handling_action
```

## DQ Handling Summary

| Issue | Where detected | Action | In Gold? |
|---|---|---|---|
| `DUPLICATE_DEDUPED` | Silver/transform.py | Keep earliest timestamp | Yes (one copy) |
| `ORPHANED_ACCOUNT` | Gold/provision.py | Quarantine → bronze/quarantine/ | No |
| `TYPE_MISMATCH` | Silver/transform.py | Cast DECIMAL; exclude if cast fails | Yes (if cast succeeds) |
| `DATE_FORMAT` | Silver/transform.py | Multi-format parse; normalise to DATE | Yes |
| `CURRENCY_VARIANT` | Silver/transform.py | Map variants → "ZAR" | Yes |
| `NULL_REQUIRED` | Bronze/ingest.py | Quarantine → bronze/quarantine/ | No |

## Memory Budget

```
driver    512m
executor  1g
JVM+OS    ~256m headroom
──────────────
Total     ~1.8g  (under 2g ceiling)
```

## Running Locally

```bash
docker build -t stage2-submission:stage2 .

docker run --rm \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  -v /path/to/stage2/data:/data \
  candidate-submission:stage2
```

## Tagging for Submission

```bash
git add -A
git commit -m "Stage 2 submission"
git tag stage2-submission
git push origin stage2-submission
```

## Author
Nomfundo Mtiyane
