"""Domain reconciliation across ODS sheets and EasyBuild indexes."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from pathlib import Path

from .easyconfigs import (
    fuzzy_candidates,
    merge_filenames,
    normalize_name,
    normalize_text,
    read_merged_easyconfig_index,
    source_dirs,
)
from .models import ColumnIndices, RowReport, SheetStats, SheetUpdateResult
from .ods import apply_updates, load_workbook, save_workbook_safely
from .safety import fingerprint_file

SUPPORTED_CLUSTERS = ("alpha", "barnard", "capella", "julia", "romeo")
DEFAULT_ALIAS_MAP: dict[str, str] = {}
RELEASE_SHEET_PATTERN = re.compile(r"^r(?:\d{2}\.\d{2}|\d{4})$", re.IGNORECASE)


def infer_cluster_from_filename(file_path: Path) -> str | None:
    tokens = [token for token in re.split(r"[^a-z0-9]+", normalize_name(file_path.stem)) if token]
    return next((token for token in tokens if token in SUPPORTED_CLUSTERS), None)


def is_valid_release_sheet_name(sheet_name: str) -> bool:
    return bool(RELEASE_SHEET_PATTERN.match(normalize_text(sheet_name)))


def detect_header_row_and_columns(rows: Sequence[Sequence[str]]) -> tuple[int, ColumnIndices]:
    patterns = {
        "software": (r"\bsoftware\b", r"\bsoftwares\b"),
        "release": (r"\brelease\b", r"easyconfig"),
        "status": (r"\bstatus\b",),
    }
    for row_idx, row in enumerate(rows):
        normalized = [normalize_name(cell) for cell in row]
        found: dict[str, int] = {}
        for column, column_patterns in patterns.items():
            for cell_idx, cell in enumerate(normalized):
                if any(re.search(pattern, cell) for pattern in column_patterns):
                    found[column] = cell_idx
                    break
        if len(found) == 3:
            return row_idx, ColumnIndices(**found)
    raise ValueError("Could not detect header row and required columns (software/release/status)")


def reconcile_sheet(
    *,
    cluster: str,
    file_path: Path,
    sheet_name: str,
    release: str,
    rows: list[list[str]],
    alias_map: dict[str, str],
    repo_root: Path,
    dry_run: bool,
) -> SheetUpdateResult:
    stats = SheetStats(cluster, str(file_path), sheet_name, release)
    reports: list[RowReport] = []
    try:
        header_idx, cols = detect_header_row_and_columns(rows)
    except ValueError:
        stats.skipped_reason = "header_or_required_columns_not_found"
        reports.append(RowReport(cluster, str(file_path), sheet_name, release, "", [], "skipped", stats.skipped_reason))
        return SheetUpdateResult(stats, reports, rows, None, None)

    easyconfig_dirs = source_dirs(repo_root, cluster, release)
    if not any(path.is_dir() for path in easyconfig_dirs):
        stats.skipped_reason = "easyconfig_directory_missing"
        reason = f"{stats.skipped_reason}:{','.join(str(path) for path in easyconfig_dirs)}"
        reports.append(RowReport(cluster, str(file_path), sheet_name, release, "", [], "skipped", reason))
        return SheetUpdateResult(stats, reports, rows, header_idx, cols)

    index = read_merged_easyconfig_index(repo_root, cluster, release)
    for row in rows[header_idx + 1 :]:
        software_name = normalize_text(row[cols.software] if cols.software < len(row) else "")
        if not software_name:
            continue
        stats.rows_scanned += 1
        lookup_name = alias_map.get(normalize_name(software_name), normalize_name(software_name))
        filenames = index.get(lookup_name)
        if filenames:
            while len(row) <= max(cols.release, cols.status):
                row.append("")
            merged = merge_filenames(row[cols.release], filenames)
            changed = merged != row[cols.release] or row[cols.status] != "Done"
            row[cols.release], row[cols.status] = merged, "Done"
            stats.matched_rows += 1
            if changed:
                stats.updated_rows += 1
                stats.changed = True
            reports.append(RowReport(cluster, str(file_path), sheet_name, release, software_name, filenames, "updated" if changed else "unchanged", "exact_or_alias_match"))
            continue

        candidates = fuzzy_candidates(software_name, index)
        if len(candidates) > 1:
            stats.ambiguous_rows += 1
            reason = f"ambiguous_candidates:{','.join(sorted(candidates)[:5])}"
        else:
            stats.unmatched_rows += 1
            reason = f"single_fuzzy_candidate:{candidates[0]}" if candidates else "no_match"
        reports.append(RowReport(cluster, str(file_path), sheet_name, release, software_name, [], "skipped", reason))

    if dry_run:
        stats.changed = False
        stats.updated_rows = 0
    return SheetUpdateResult(stats, reports, rows, header_idx, cols)


def process_ods_file(
    *,
    file_path: Path,
    cluster: str,
    repo_root: Path,
    dry_run: bool,
    alias_map: dict[str, str],
    pre_replace_check: Callable[[], None] | None = None,
) -> tuple[list[SheetStats], list[RowReport], bool]:
    original_fingerprint = fingerprint_file(file_path)
    doc, workbook_tables = load_workbook(file_path)
    results = []
    all_stats: list[SheetStats] = []
    all_reports: list[RowReport] = []
    for sheet_name, row_elems, old_rows in workbook_tables:
        normalized_sheet = normalize_text(sheet_name)
        if not is_valid_release_sheet_name(normalized_sheet):
            stats = SheetStats(cluster, str(file_path), sheet_name, "", skipped_reason="invalid_release_sheet_name")
            report = RowReport(cluster, str(file_path), sheet_name, "", "", [], "skipped", f"invalid_release_sheet_name:{sheet_name}")
            all_stats.append(stats)
            all_reports.append(report)
            continue
        result = reconcile_sheet(
            cluster=cluster,
            file_path=file_path,
            sheet_name=sheet_name,
            release=normalized_sheet.casefold(),
            rows=[list(row) for row in old_rows],
            alias_map=alias_map,
            repo_root=repo_root,
            dry_run=dry_run,
        )
        all_stats.append(result.stats)
        all_reports.extend(result.reports)
        results.append((result, old_rows, row_elems))

    if dry_run or not any(result.stats.changed for result, _, _ in results):
        return all_stats, all_reports, False

    workbook_changed = False
    for result, old_rows, row_elems in results:
        if result.stats.changed and result.header_idx is not None and result.cols is not None:
            workbook_changed |= apply_updates(
                row_elems=row_elems,
                old_rows=old_rows,
                new_rows=result.new_rows,
                cols=result.cols,
                header_idx=result.header_idx,
            )
    if workbook_changed:
        save_workbook_safely(
            doc,
            file_path,
            original_fingerprint,
            pre_replace_check=pre_replace_check,
        )
    else:
        for result, _, _ in results:
            result.stats.changed = False
            result.stats.updated_rows = 0
    return all_stats, all_reports, workbook_changed
