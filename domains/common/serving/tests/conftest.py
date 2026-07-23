"""Make the dags root importable so ``import domains.common.serving`` resolves."""

from __future__ import annotations

import sys
from pathlib import Path

DAGS_ROOT = Path(__file__).resolve().parents[4]  # tests -> serving -> common -> domains -> dags root
if str(DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(DAGS_ROOT))
