"""
Pipeline entry point.

Orchestrates the three medallion architecture stages in order:
  1. Ingest    — reads raw source files into Bronze layer Delta tables
  2. Transform — cleans and conforms Bronze into Silver layer Delta tables
  3. Provision — joins Silver tables into Gold dimensional model

The evaluation system invokes this file directly:
  docker run ... python pipeline/run_all.py

No interactive input, no argument parsing that blocks execution.
"""

import logging
import sys
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Add /app to sys.path so pipeline.* imports work whether run from /app or cwd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config_loader import load_config
from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation
from pipeline.provision import run_provisioning


if __name__ == "__main__":
    try:
        config = load_config()
        logger.info("Pipeline starting — config loaded.")

        logger.info("=== Stage 1/3: Bronze Ingestion ===")
        run_ingestion(config)

        logger.info("=== Stage 2/3: Silver Transformation ===")
        run_transformation(config)

        logger.info("=== Stage 3/3: Gold Provisioning ===")
        run_provisioning(config)

        logger.info("Pipeline complete — exiting 0.")
        sys.exit(0)

    except Exception:
        logger.exception("Pipeline failed with unhandled exception.")
        sys.exit(1)