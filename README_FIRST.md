# Nedbank Data Engineering Challenge — Start Here

## Quick Start

1. Read `challenge_brief.md` — understand what you're building
2. Read `data_dictionary.md` — understand the data
3. Read `output_schema_spec.md` — understand what to produce
4. Copy `starter_kit/` as your project base
5. Build and test: `./infrastructure/run_tests.sh --stage 1`
6. Submit your repo URL on the Nedbank Innovate Challenge platform

---

## Pack Contents

| File | Description |
|---|---|
| `README_FIRST.md` | This file — start here |
| `challenge_brief.md` | Full challenge description, objectives, and evaluation criteria |
| `data_dictionary.md` | Field-by-field reference for all source data files |
| `output_schema_spec.md` | Exact schema and format requirements for all pipeline outputs |
| `docker_interface_contract.md` | How your container will be invoked during automated evaluation |
| `README_DOCKER.md` | Step-by-step guide to building and running your Docker container |
| `submission_guide.md` | How to tag and submit your solution at each stage |
| `validation_queries.sql` | SQL queries used to validate your output tables |
| `resource_constraints.md` | CPU, memory, and time limits applied during evaluation |
| `Dockerfile.base` | Base image definition — extend or use as-is in your submission |
| `dq_report_template.json` | Template for the data quality report your pipeline must produce |
| `starter_kit/` | Project scaffold — copy this as your starting point |
| `starter_kit/README.md` | Starter kit overview and usage instructions |
| `starter_kit/Dockerfile` | Submission Dockerfile (extend `Dockerfile.base`) |
| `starter_kit/requirements.txt` | Python dependencies for the starter kit |
| `starter_kit/.gitignore` | Recommended git ignore rules |
| `starter_kit/pipeline/__init__.py` | Pipeline package init |
| `starter_kit/pipeline/ingest.py` | Batch ingestion stub |
| `starter_kit/pipeline/provision.py` | Database provisioning stub |
| `starter_kit/pipeline/run_all.py` | Top-level pipeline runner |
| `starter_kit/pipeline/transform.py` | Transformation logic stub |
| `starter_kit/config/pipeline_config.yaml` | Pipeline configuration file |
| `starter_kit/config/dq_rules.yaml` | Data quality rules configuration |
| `infrastructure/run_tests.sh` | Test harness — run locally before submitting |

---

## Key Documents

| Purpose | File |
|---|---|
| Challenge Brief | `challenge_brief.md` |
| Data Dictionary | `data_dictionary.md` |
| Output Schema | `output_schema_spec.md` |
| Docker Setup | `docker_interface_contract.md` + `README_DOCKER.md` |
| Submission Guide | `submission_guide.md` |
| Validation Queries | `validation_queries.sql` |
| Testing Harness | `infrastructure/run_tests.sh` |

---

## Data Files

Data files are bundled in this pack — see the `data/` directory.

| Stage | Directory | Contents | Approx. Size |
|---|---|---|---|
| Stage 1 | `data/` | `customers.csv`, `accounts.csv`, `transactions.jsonl` | ~473 MB |

When the test harness runs (`./infrastructure/run_tests.sh --stage 1`), it expects data to be mounted at `/data/input/` inside the container. See `docker_interface_contract.md` for the full mount specification.

---

## Stages

You will be notified when each stage opens.

## Support

For technical questions, clarifications, and issue reports, contact the challenge support channel provided in your participant onboarding email. Do not share your solution code in the support channel.
