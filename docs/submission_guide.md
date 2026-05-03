# Submission Guide

**Document:** `submission_guide.md`
**Version:** 1.0
**Status:** Final

---

## 1. Required Repository Structure

Your submission repository must contain the following layout at the time you create your stage tag. Files outside this structure are ignored by the evaluation system.

```
your-submission/
├── Dockerfile                      # Required. Must extend nedbank-de-challenge/base:1.0
├── docker-compose.yml              # Optional. For local development only — not used by scorer
├── pipeline/
│   ├── ingest.py                   # Bronze layer ingestion
│   ├── transform.py                # Bronze → Silver transformation
│   ├── provision.py                # Silver → Gold provisioning
│   └── run_all.py                  # Entry point; calls ingest → transform → provision in sequence
├── config/
│   ├── pipeline_config.yaml        # Required. Pipeline configuration (paths, settings)
│   └── dq_rules.yaml               # Data quality rules configuration
├── requirements.txt                # Python dependencies beyond the base image (may be empty)
└── README.md                       # Required. Must explain how to run the pipeline locally
```

**Notes on specific paths:**
- `output/` must **not** be committed. Add it to `.gitignore`. The evaluation system provides the output directory via volume mount — pre-existing output in the image will not be used.
- Additional modules under `pipeline/` are permitted. The naming above is a minimum; you may add `utils.py`, `models.py`, etc.
- `docker-compose.yml` is for your local development workflow only. The evaluation system does not use it.

---

## 2. Git Tagging Protocol

Each stage submission is identified by a specific Git tag on your repository. The evaluation system checks out the exact tagged commit — nothing else is visible.

The Stage 1 submission tag is `stage1-submission`. You will be notified when each stage opens.

**How to create and push a tag:**

```bash
# Ensure all your changes are committed
git add -A
git commit -m "Stage 1 final submission"

# Create an annotated tag
git tag -a stage1-submission -m "Stage 1 submission"

# Push both the commit and the tag
git push origin main
git push origin stage1-submission
```

**Important:**
- The tag must be present in the remote repository before the stage closes. A local-only tag is not visible to the scorer.
- Commits pushed after the stage closes are not visible to the evaluation system. The scorer checks out the tagged commit, not `HEAD`.
- You can overwrite an existing tag before the stage closes if you need to resubmit: `git tag -fa stage1-submission -m "Stage 1 resubmission"` followed by `git push origin stage1-submission --force`. Do not do this after the stage closes.

---

## 3. How to Submit

1. Push your tagged commit to your remote repository (the platform will provide a repository URL when you register, or you may use your own public repository with access granted to the evaluation system).
2. On the challenge platform, navigate to the submission form for the relevant stage.
3. Paste your repository URL into the submission field and confirm.
4. The evaluation system will clone your repository, check out the tagged commit, build your Docker image, and execute the pipeline.
5. You will receive a confirmation email when scoring begins. Score results are published to the leaderboard after the stage closes.

Only the most recent submitted URL per stage is used. If you update your repository URL after submission, resubmit on the platform.

---

## 4. What Happens After You Submit

The evaluation system assesses your submission.

---

## 5. Verifying Your Submission Locally Before Submitting

Run the following before pushing your stage tag. These steps mirror what the evaluation system does.

**Step 1: Verify the Docker build is clean**

```bash
# Build from a clean context — ensure no stale layers
docker build --no-cache -t my-submission:test .
```

**Step 2: Run with the same constraints as the evaluation system**

```bash
# Create a local data directory with your test data
mkdir -p /tmp/test-data/input /tmp/test-data/output /tmp/test-data/config

# Copy your input files
cp accounts.csv /tmp/test-data/input/
cp transactions.jsonl /tmp/test-data/input/
cp customers.csv /tmp/test-data/input/
cp config/pipeline_config.yaml /tmp/test-data/config/

# Run with scoring-equivalent constraints
docker run --rm \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  -v /tmp/test-data:/data \
  my-submission:test

echo "Exit code: $?"
```

**Step 3: Check outputs exist**

```bash
ls /tmp/test-data/output/bronze/
ls /tmp/test-data/output/silver/
ls /tmp/test-data/output/gold/
```

**Step 4: Run the provided local test harness**

```bash
bash infrastructure/run_tests.sh --stage 1 --data-dir /tmp/test-data --image my-submission:test
```

The harness checks: Docker build success, container exits 0, output directories created, Gold layer readable by DuckDB, validation queries execute without error.

**Step 5: Verify your tag is pushed**

```bash
git ls-remote origin refs/tags/stage1-submission
```

If this returns a hash, the tag is visible to the evaluation system. If it returns nothing, your tag has not been pushed.

---

## 6. Common Mistakes to Avoid

**Tag name errors** — The evaluation system requires the exact tag name `stage1-submission`. Tags named `stage-1`, `stage_1`, `v1`, or `submission-stage1` will not be found.

**Committing the output directory** — Output files are large and will slow your repository. More importantly, the evaluation system mounts its own output directory — any output baked into your image is unreachable. Add `output/` to `.gitignore` on day one.

**Dependencies not in the image** — The container has no network access during execution. Any package not installed in your `Dockerfile` will cause an import error at runtime. If you use `pip install` anywhere in your pipeline code, it will fail. Install everything at build time.

**Hardcoded paths** — The evaluation system mounts data at `/data/input/`. Do not hardcode paths relative to your local development environment. Use `pipeline_config.yaml` for all paths.

**Interactive input** — The container is not attached to a terminal. Any `input()` call or interactive prompt will hang until the timeout kills the container.

