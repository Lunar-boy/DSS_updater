"""Data models shared by the reconciler modules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class ColumnIndices:
    software: int
    release: int
    status: int


@dataclass
class SoftwareEntry:
    easyconfigs: set[str]
    in_repo: bool
    installed: bool


@dataclass
class SheetStats:
    cluster: str
    file_path: str
    sheet_name: str
    release: str
    rows_scanned: int = 0
    matched_rows: int = 0
    unmatched_rows: int = 0
    ambiguous_rows: int = 0
    updated_rows: int = 0
    changed: bool = False
    skipped_reason: str = ""


@dataclass
class RowReport:
    cluster: str
    file_path: str
    sheet_name: str
    release: str
    software_name: str
    matched_easyconfigs: list[str]
    action: str
    reason: str
    source: str | None = None


@dataclass
class SheetUpdateResult:
    stats: SheetStats
    reports: list[RowReport]
    new_rows: list[list[str]]
    header_idx: Optional[int]
    cols: Optional[ColumnIndices]
