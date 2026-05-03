"""
Pipeline entry point — Stage 2.

Orchestrates all three medallion stages and writes the DQ report.
Exits 0 on success, 1 on any unhandled exception.

The evaluation system invokes:
    docker run ... python pipeline/run_all.py
"""

from __future__ import annotations
import logging
import sys
import os
import time
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config_loader import load_config, load_dq_rules
from pipeline.ingest      import run_ingestion
from pipeline.transform   import run_transformation
from pipeline.provision   import run_provisioning
from pipeline.dq_report   import write_dq_report


if __name__ == "__main__":
    try:
        pipeline_start   = time.time()
        run_timestamp    = datetime.now(timezone.utc).replace(
                               microsecond=0).isoformat()

        config = load_config()
        rules  = load_dq_rules(config)
        logger.info("Pipeline starting — stage %s", config.get("stage", "2"))

        # ── Stage 1/3: Bronze ingestion ────────────────────────────────────────
        logger.info("=== Stage 1/3: Bronze Ingestion ===")
        source_counts = run_ingestion(config)

        # ── Stage 2/3: Silver transformation ──────────────────────────────────
        logger.info("=== Stage 2/3: Silver Transformation ===")
        dq_transform_counts = run_transformation(config)

        # ── Stage 3/3: Gold provisioning ──────────────────────────────────────
        logger.info("=== Stage 3/3: Gold Provisioning ===")
        gold_counts = run_provisioning(config)

        # ── DQ Report ─────────────────────────────────────────────────────────
        duration = int(time.time() - pipeline_start)
        write_dq_report(
            config=config,
            rules=rules,
            run_timestamp=run_timestamp,
            source_counts=source_counts,
            dq_transform_counts=dq_transform_counts,
            gold_counts=gold_counts,
            execution_duration_seconds=duration,
        )

        logger.info("Pipeline complete in %ds — exiting 0.", duration)
        sys.exit(0)

    except Exception:
        logger.exception("Pipeline failed with unhandled exception.")
        sys.exit(1)