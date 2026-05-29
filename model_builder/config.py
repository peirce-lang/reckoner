"""
model_builder/config.py — Configuration constants for Model Builder.

All tuneable values live here. Nothing else should define these.
"""

import os
import tempfile
from pathlib import Path

SESSION_TTL_SECONDS  = 60 * 60 * 2   # 2 hours
SAMPLE_ROWS          = 10             # rows shown as samples in column mapping
VARIANT_SCORE_THRESH = 0.5            # Tantivy score threshold for variant candidates
SINGLETON_MAX_COUNT  = 1              # values appearing <= this are "singletons"
NULL_PCT_WARN_THRESH = 0.20           # warn if > 20% of values are null

OUTPUT_DIR     = Path(tempfile.gettempdir()) / "model_builder_artifacts"
OUTPUT_DIR.mkdir(exist_ok=True)

SRF_IMPORTS_DIR = Path(os.environ.get("SNF_SRF_IMPORTS_DIR", "./substrates/srf_imports"))
