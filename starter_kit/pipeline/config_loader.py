"""
Configuration loader.

Reads pipeline_config.yaml from /data/config/ (or the path set in the
PIPELINE_CONFIG environment variable).  Falls back to /app/config/ if
the runtime mount is absent — useful for local development.
"""

import os
import yaml


_DEFAULT_PATHS = [
    "/data/config/pipeline_config.yaml",
    "/app/config/pipeline_config.yaml",
]


def load_config(path: str | None = None) -> dict:
    """Load and return the pipeline configuration dictionary."""
    candidates = [path] if path else []
    candidates += [os.environ.get("PIPELINE_CONFIG", "")] + _DEFAULT_PATHS

    for candidate in candidates:
        if candidate and os.path.isfile(candidate):
            with open(candidate, "r") as fh:
                return yaml.safe_load(fh)

    raise FileNotFoundError(
        "pipeline_config.yaml not found. Searched: " + str(_DEFAULT_PATHS)
    )


def load_dq_rules(config: dict) -> dict:
    """Load DQ rules from the path specified in config."""
    candidates = [
        config.get("dq", {}).get("rules_path", ""),
        "/data/config/dq_rules.yaml",
        "/app/config/dq_rules.yaml",
    ]

    for candidate in candidates:
        if candidate and os.path.isfile(candidate):
            with open(candidate, "r") as fh:
                return yaml.safe_load(fh) or {}

    raise FileNotFoundError("dq_rules.yaml not found.")