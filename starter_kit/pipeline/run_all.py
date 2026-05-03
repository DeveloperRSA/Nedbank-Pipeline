"""
Pipeline entry point — Stage 3.

Execution order:
  1. Batch pipeline (Stage 2 — Bronze → Silver → Gold + DQ report)
  2. Streaming loop (Stage 3 — polls /data/stream/, merges into stream_gold/)

Both must complete within the 30-minute container wall-clock limit.
Exits 0 on success, 1 on any unhandled exception.
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

from pipeline.config_loader  import load_config, load_dq_rules
from pipeline.ingest         import run_ingestion
from pipeline.transform      import run_transformation
from pipeline.provision      import run_provisioning
from pipeline.dq_report      import write_dq_report
from pipeline.stream_ingest  import run_stream_ingestion


if __name__ == "__main__":
    try:
        pipeline_start = time.time()
        run_timestamp  = datetime.now(timezone.utc).replace(
                             microsecond=0).isoformat()

        config = load_config()
        rules  = load_dq_rules(config)
        logger.info("Pipeline starting — stage %s", config.get("stage", "3"))

        # ── 1/4: Bronze ingestion ──────────────────────────────────────────
        logger.info("=== Stage 1/4: Bronze Ingestion ===")
        source_counts = run_ingestion(config)

        # ── 2/4: Silver transformation ─────────────────────────────────────
        logger.info("=== Stage 2/4: Silver Transformation ===")
        dq_transform_counts = run_transformation(config)

        # ── 3/4: Gold provisioning ─────────────────────────────────────────
        logger.info("=== Stage 3/4: Gold Provisioning ===")
        gold_counts = run_provisioning(config)

        # ── DQ report (written before stream loop) ─────────────────────────
        batch_duration = int(time.time() - pipeline_start)
        write_dq_report(
            config=config,
            rules=rules,
            run_timestamp=run_timestamp,
            source_counts=source_counts,
            dq_transform_counts=dq_transform_counts,
            gold_counts=gold_counts,
            execution_duration_seconds=batch_duration,
        )
        logger.info("Batch pipeline complete in %ds.", batch_duration)

        # ── 4/4: Stream ingestion ──────────────────────────────────────────
        logger.info("=== Stage 4/4: Stream Ingestion ===")
        run_stream_ingestion(config)

        total_duration = int(time.time() - pipeline_start)
        logger.info("Full pipeline complete in %ds — exiting 0.", total_duration)
        sys.exit(0)

    except Exception:
        logger.exception("Pipeline failed with unhandled exception.")
        sys.exit(1)