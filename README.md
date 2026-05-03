# Nedbank Data Engineering Challenge/Stage 1 Solution

## Architecture

Medallion pipeline: **Bronze → Silver → Gold** using PySpark 3.5 + Delta Lake 3.1.

```
Bronze          Silver              Gold
Raw ingest  →   Standardised    →   Dimensional model
(as-arrived)    (typed, linked,     (fact_transactions,
                deduplicated)       dim_accounts,
                                    dim_customers)
```

## Project Structure

```
Dockerfile                   # Extends nedbank-de-challenge/base:1.0
requirements.txt             # Additional deps (none beyond base image)
pipeline/
  __init__.py
  run_all.py                 # Entry point orchestrates all three stages
  ingest.py                  # Bronze layer
  transform.py               # Silver layer
  provision.py               # Gold layer
  spark_session.py           # Shared SparkSession factory
  config_loader.py           # YAML config + DQ rules loader
config/
  pipeline_config.yaml       # Paths, Spark settings, DQ rules path
  dq_rules.yaml              # DQ rules (null checks, domain checks, etc.)
```

## Design Decisions

### Surrogate Keys
Uses `sha2(natural_key, 256)` → first 15 hex chars → `conv(..., 16, 10).cast(BIGINT)`.
This is deterministic across re-runs on the same input, satisfying the stability requirement.

### Spark Configuration
- `local[2]` master- matches the 2-vCPU evaluation constraint
- `spark.sql.shuffle.partitions=8`- avoids 200-partition default on small data
- Driver/executor memory set to 1800m within the 2 GB container limit
- Temp files directed to `/tmp` (512 MB tmpfs provided by the evaluation system)

### No `.collect()` at Scale
All transformations use native Spark DataFrame operations.
`.count()` is called only for logging after writes (not in the critical path).
Joins use broadcast-eligible patterns where possible.

### DQ Flagging
Rules are read from `dq_rules.yaml` at runtime- no hardcoded DQ logic.
Priority: `NULL_REQUIRED` > `CURRENCY_VARIANT` > `ORPHANED_ACCOUNT` (resolved in Gold).
Clean records receive `dq_flag = NULL`.

### Configuration
All paths are read from `pipeline_config.yaml`. The evaluation system may inject
this file at `/data/config/pipeline_config.yaml`- the config loader checks that
path first, then falls back to `/app/config/pipeline_config.yaml`.

## Running Locally

```bash
# Build the image
docker build -t stage1-submission:latest .

# Run (mount your local data directory to /data)
docker run \
  --rm \
  -v /path/to/data:/data \
  -m 2g --cpus="2" \
  stage1-submission:latest
```

The `/path/to/data` directory must have the layout:
```
data/
  input/
    accounts.csv
    customers.csv
    transactions.jsonl
  config/
    pipeline_config.yaml
```

Output will be written to `data/output/bronze/`, `data/output/silver/`, `data/output/gold/`.

## Author
Nomfundo Mtiyane
