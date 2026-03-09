#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from etl.run import run_pipeline

if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception:
        sys.exit(1)
