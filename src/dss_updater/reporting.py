"""JSON reporting for reconciliation runs."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from .models import RowReport, SheetStats
from .safety import atomic_write


def serialize_report(sheet_stats: Sequence[SheetStats], row_reports: Sequence[RowReport], output_path: Path) -> None:
    payload = {
        "generated_at": datetime.now().isoformat(),
        "sheets": [asdict(stat) for stat in sheet_stats],
        "rows": [asdict(report) for report in row_reports],
    }
    content = json.dumps(payload, indent=2)
    atomic_write(output_path, lambda temporary: temporary.write_text(content, encoding="utf-8"))
