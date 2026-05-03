#!/usr/bin/env bash
# =============================================================================
# run_tests.sh — Nedbank DE Challenge: Local Testing Harness
#
# Runs 5 checks against your pipeline submission before you push a stage tag.
# These checks mirror what the automated evaluation system does. Fix any FAILs
# before submitting.
#
# Usage:
#   bash run_tests.sh [options]
#
# Options:
#   --data-dir PATH     Host directory to mount as /data (default: ./sample)
#   --image NAME        Docker image to test (default: my-submission:test)
#   --build             Build the image before testing (runs docker build .)
#   --timeout N         Per-container timeout in seconds (default: 1800 = 30 min)
#   --help              Show this help message
#
# Examples:
#   bash run_tests.sh
#   bash run_tests.sh --data-dir /tmp/test-data --image my-sub:latest
#   bash run_tests.sh --build
#
# Exit codes:
#   0   All checks passed
#   1   One or more checks failed
# =============================================================================

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────────────
DATA_DIR="./data"
IMAGE="my-submission:test"
DO_BUILD=false
TIMEOUT_SECS=1800   # 30 minutes (matches the evaluation system limit)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Colour helpers ────────────────────────────────────────────────────────────
# Disable colour if output is not a terminal (e.g. CI logs)
if [ -t 1 ]; then
    GREEN="\033[0;32m"
    RED="\033[0;31m"
    YELLOW="\033[0;33m"
    BOLD="\033[1m"
    RESET="\033[0m"
else
    GREEN="" RED="" YELLOW="" BOLD="" RESET=""
fi

pass() { echo -e "  ${GREEN}[PASS]${RESET} $1"; }
fail() { echo -e "  ${RED}[FAIL]${RESET} $1"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "  ${YELLOW}[INFO]${RESET} $1"; }
header() { echo -e "\n${BOLD}$1${RESET}"; }

FAILURES=0

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --data-dir)    DATA_DIR="$2";    shift 2 ;;
        --image)       IMAGE="$2";       shift 2 ;;
        --build)       DO_BUILD=true;    shift   ;;
        --timeout)     TIMEOUT_SECS="$2"; shift 2 ;;
        --help)
            sed -n '/^# Usage/,/^[^#]/p' "$0" | grep '^#' | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "Unknown option: $1 (run with --help for usage)"
            exit 1
            ;;
    esac
done

# ── Resolve data directory ────────────────────────────────────────────────────
# If --data-dir was not explicitly set and the default ./sample doesn't exist,
# fall back gracefully but warn the user.
if [[ "$DATA_DIR" == "./data" ]] && [[ ! -d "$DATA_DIR" ]]; then
    info "No --data-dir supplied and ./data not found."
    info "Create a data directory with accounts.csv, transactions.jsonl,"
    info "customers.csv, and config/pipeline_config.yaml, then re-run with:"
    info "  bash run_tests.sh --data-dir /path/to/your/data"
    DATA_DIR=""
fi

# Convert to absolute path if provided
if [[ -n "$DATA_DIR" ]]; then
    DATA_DIR="$(cd "$DATA_DIR" && pwd)"
fi

# ── Summary banner ────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}=== Nedbank DE Challenge — Local Test Harness ===${RESET}"
echo "  Image:      $IMAGE"
echo "  Data dir:   ${DATA_DIR:-'(not set)'}"
echo "  Timeout:    ${TIMEOUT_SECS}s"
echo ""

# =============================================================================
# CHECK 1: Docker image builds successfully
# =============================================================================
header "Check 1: Docker image builds"

if $DO_BUILD; then
    info "Building image from current directory..."
    if docker build -t "$IMAGE" . > /tmp/docker_build.log 2>&1; then
        pass "docker build succeeded"
    else
        fail "docker build failed — see /tmp/docker_build.log for details"
        echo ""
        echo "  Last 20 lines of build log:"
        tail -20 /tmp/docker_build.log | sed 's/^/    /'
        echo ""
        echo -e "${RED}Build failed. Fix the Dockerfile before running other checks.${RESET}"
        exit 1
    fi
else
    # Check that the image exists locally
    if docker image inspect "$IMAGE" > /dev/null 2>&1; then
        pass "Image '$IMAGE' found locally (use --build to rebuild)"
    else
        fail "Image '$IMAGE' not found. Build it first or pass --build"
        echo "  Hint: docker build -t $IMAGE ."
        FAILURES=$((FAILURES + 1))
        # Cannot proceed without an image
        echo ""
        echo -e "${RED}Cannot run remaining checks without a valid image.${RESET}"
        exit 1
    fi
fi

# =============================================================================
# CHECK 2: Container starts and exits with code 0
# =============================================================================
header "Check 2: Container runs and exits 0"

if [[ -z "$DATA_DIR" ]]; then
    fail "No data directory available — skipping container run"
    info "Provide sample data with --data-dir to enable this check"
else
    # Prepare a fresh, empty output directory so we test against a clean run
    OUTPUT_DIR="$(mktemp -d /tmp/de_test_output.XXXXXX)"
    mkdir -p "$OUTPUT_DIR"

    # Build the docker run command using the same security flags as the scorer
    DOCKER_RUN_CMD=(
        docker run --rm
        --network=none
        --memory=2g --memory-swap=2g
        --cpus=2
        --pids-limit=512
        --read-only
        --tmpfs /tmp:rw,size=512m
        --cap-drop=ALL
        --security-opt no-new-privileges
        -e PYTHONDONTWRITEBYTECODE=1
        -v "${DATA_DIR}/input:/data/input:ro"
        -v "${DATA_DIR}/config:/data/config:ro"
        -v "${OUTPUT_DIR}:/data/output:rw"
        "$IMAGE"
    )

    info "Running container (timeout: ${TIMEOUT_SECS}s)..."

    # Use 'timeout' if available, otherwise fall back to plain docker run
    TIMED_OUT=false
    if command -v timeout > /dev/null 2>&1; then
        if timeout "$TIMEOUT_SECS" "${DOCKER_RUN_CMD[@]}" > /tmp/container_run.log 2>&1; then
            EXIT_CODE=0
        else
            EXIT_CODE=$?
            if [[ $EXIT_CODE -eq 124 ]]; then
                TIMED_OUT=true
            fi
        fi
    else
        if "${DOCKER_RUN_CMD[@]}" > /tmp/container_run.log 2>&1; then
            EXIT_CODE=0
        else
            EXIT_CODE=$?
        fi
    fi

    if $TIMED_OUT; then
        fail "Container timed out after ${TIMEOUT_SECS}s (exit code 124)"
        info "Optimise your pipeline to complete within the timeout."
    elif [[ $EXIT_CODE -eq 0 ]]; then
        pass "Container exited with code 0"
    elif [[ $EXIT_CODE -eq 137 ]]; then
        fail "Container killed by OOM (exit 137). Reduce memory usage."
        info "Hard limit is 2 GB. Use local[2] Spark, avoid .toPandas() on large frames."
    else
        fail "Container exited with code $EXIT_CODE (expected 0)"
        info "Check /tmp/container_run.log for the Python traceback"
        echo ""
        echo "  Last 30 lines of container output:"
        tail -30 /tmp/container_run.log | sed 's/^/    /'
    fi

    # ==========================================================================
    # CHECK 3: Output directory structure exists
    # ==========================================================================
    header "Check 3: Output directory structure"

    EXPECTED_DIRS=(
        "bronze"
        "silver"
        "gold"
    )

    ALL_DIRS_OK=true
    for dir in "${EXPECTED_DIRS[@]}"; do
        if [[ -d "$OUTPUT_DIR/$dir" ]]; then
            pass "/data/output/$dir/ exists"
        else
            fail "/data/output/$dir/ not found — your pipeline must create this directory"
            ALL_DIRS_OK=false
        fi
    done

    # ==========================================================================
    # CHECK 4: Gold layer Delta tables are readable by DuckDB
    # ==========================================================================
    header "Check 4: Gold layer Delta tables readable by DuckDB"

    # We run DuckDB inside a fresh container using the base image so the
    # participant doesn't need DuckDB installed on their host machine.
    # If the base image is not available, we try a direct host duckdb call.

    GOLD_TABLES=("fact_transactions" "dim_accounts" "dim_customers")
    DUCKDB_AVAILABLE=false

    # Try to locate duckdb on the host first (fast path)
    if command -v duckdb > /dev/null 2>&1; then
        DUCKDB_AVAILABLE=true
    fi

    check_gold_table_duckdb() {
        local table="$1"
        local table_path="$OUTPUT_DIR/gold/$table"

        if [[ ! -d "$table_path" ]]; then
            fail "gold/$table/ directory not found"
            return
        fi

        if $DUCKDB_AVAILABLE; then
            # Run DuckDB directly on the host
            local row_count
            row_count=$(duckdb -c "INSTALL delta; LOAD delta; SELECT COUNT(*) FROM delta_scan('${table_path}');" 2>/dev/null | grep -E '^[0-9]+$' | head -1 || echo "ERROR")

            if [[ "$row_count" == "ERROR" ]] || [[ -z "$row_count" ]]; then
                fail "gold/$table/ exists but DuckDB could not read it as a Delta table"
                info "Ensure your pipeline writes valid Delta Lake format (delta_log/ must be present)"
            elif [[ "$row_count" -eq 0 ]]; then
                fail "gold/$table/ is a valid Delta table but contains 0 rows"
            else
                pass "gold/$table/ readable by DuckDB ($row_count rows)"
            fi
        else
            # Fall back to checking for Delta log presence
            if [[ -d "$table_path/_delta_log" ]]; then
                pass "gold/$table/ contains _delta_log/ (Delta format assumed valid; install duckdb for full check)"
            else
                fail "gold/$table/ found but _delta_log/ missing — not a valid Delta table"
                info "Write using delta format in PySpark: df.write.format('delta').save(path)"
            fi
        fi
    }

    for table in "${GOLD_TABLES[@]}"; do
        check_gold_table_duckdb "$table"
    done

    # ==========================================================================
    # CHECK 5: Validation queries execute without error
    # ==========================================================================
    header "Check 5: Validation queries execute without error"

    # These are the three queries from validation_queries.sql.
    # We check structural correctness (query runs, returns expected row counts)
    # rather than exact values.

    if $DUCKDB_AVAILABLE; then
        # Build a DuckDB script that registers the Gold tables and runs all 3 queries
        DUCKDB_SCRIPT=$(cat <<DUCKDB_EOF
INSTALL delta;
LOAD delta;

-- Register Gold tables as views
CREATE VIEW fact_transactions AS SELECT * FROM delta_scan('${OUTPUT_DIR}/gold/fact_transactions');
CREATE VIEW dim_accounts      AS SELECT * FROM delta_scan('${OUTPUT_DIR}/gold/dim_accounts');
CREATE VIEW dim_customers     AS SELECT * FROM delta_scan('${OUTPUT_DIR}/gold/dim_customers');

-- Q1: Transaction volume by type (expected: 4 rows)
SELECT 'Q1' AS query, COUNT(*) AS result_rows FROM (
    SELECT transaction_type, COUNT(*) AS cnt, SUM(amount) AS total_amount
    FROM fact_transactions
    GROUP BY transaction_type
    ORDER BY transaction_type
);

-- Q2: Orphaned accounts (expected: 0)
SELECT 'Q2' AS query, COUNT(*) AS unlinked_accounts
FROM dim_accounts a
LEFT JOIN dim_customers c ON a.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Q3: Province distribution (expected: up to 9 rows)
SELECT 'Q3' AS query, COUNT(*) AS result_rows FROM (
    SELECT c.province, COUNT(DISTINCT a.account_id) AS account_count
    FROM dim_accounts a
    JOIN dim_customers c ON a.customer_id = c.customer_id
    GROUP BY c.province
    ORDER BY c.province
);
DUCKDB_EOF
)
        QUERY_OUTPUT=$(echo "$DUCKDB_SCRIPT" | duckdb 2>/tmp/duckdb_queries.log || true)

        if [[ $? -ne 0 ]] || grep -q "Error" /tmp/duckdb_queries.log 2>/dev/null; then
            fail "One or more validation queries failed to execute"
            info "See /tmp/duckdb_queries.log for the error"
            tail -10 /tmp/duckdb_queries.log | sed 's/^/    /'
        else
            # Q1: expect 4 rows (CREDIT, DEBIT, FEE, REVERSAL)
            Q1_ROWS=$(echo "$QUERY_OUTPUT" | grep "^Q1" | awk '{print $NF}' || echo "0")
            if [[ "$Q1_ROWS" == "4" ]]; then
                pass "Q1 (transaction_type distribution) returned 4 rows as expected"
            else
                fail "Q1 returned $Q1_ROWS rows (expected 4 — one per transaction type: CREDIT, DEBIT, FEE, REVERSAL)"
            fi

            # Q2: expect 0 orphaned accounts
            Q2_ORPHANS=$(echo "$QUERY_OUTPUT" | grep "^Q2" | awk '{print $NF}' || echo "-1")
            if [[ "$Q2_ORPHANS" == "0" ]]; then
                pass "Q2 (orphaned accounts) returned 0 — all accounts link to a customer"
            else
                fail "Q2 returned $Q2_ORPHANS orphaned accounts (expected 0) — check dim_accounts.customer_id join to dim_customers.customer_id"
            fi

            # Q3: expect up to 9 rows (9 SA provinces)
            Q3_ROWS=$(echo "$QUERY_OUTPUT" | grep "^Q3" | awk '{print $NF}' || echo "0")
            if [[ "$Q3_ROWS" -ge 1 ]] && [[ "$Q3_ROWS" -le 9 ]]; then
                pass "Q3 (province distribution) returned $Q3_ROWS province rows (expected ≤9)"
            elif [[ "$Q3_ROWS" -eq 0 ]]; then
                fail "Q3 returned 0 rows — province join produced no results"
            else
                fail "Q3 returned $Q3_ROWS rows (expected at most 9 — one per SA province)"
            fi
        fi
    else
        info "DuckDB not found on host — skipping query execution check"
        info "Install DuckDB (https://duckdb.org) to enable this check"
        info "The evaluation system always runs these queries; install duckdb before submitting"
    fi

    # Clean up temporary output directory
    rm -rf "$OUTPUT_DIR"
fi

# =============================================================================
# Final summary
# =============================================================================
echo ""
echo -e "${BOLD}=== Test Summary ===${RESET}"

if [[ $FAILURES -eq 0 ]]; then
    echo -e "  ${GREEN}All checks passed.${RESET} You are ready to push your stage1-submission tag."
    echo ""
    echo "  Next step:"
    echo "    git tag -a stage1-submission -m \"Stage 1 submission\""
    echo "    git push origin stage1-submission"
    exit 0
else
    echo -e "  ${RED}${FAILURES} check(s) failed.${RESET} Fix the issues above before submitting."
    echo ""
    echo "  The evaluation system will record zero correctness points for any"
    echo "  stage where the container exits non-zero or outputs are absent."
    exit 1
fi
