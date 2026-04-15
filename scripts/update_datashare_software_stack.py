#!/usr/bin/env python3
"""Update Datashare software stack sheets from local barnard-ci easyconfigs.

ODS is the primary workflow: every sheet in a workbook is evaluated, with the
sheet name used as the release identifier (for example ``r25.06`` or ``r2026``).
CSV remains as a fallback for legacy usage.
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import json
import logging
import os
import re
import shutil
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

try:
    from odf import teletype
    from odf.element import CDATASection, Element, Node, Text
    from odf.opendocument import load as odf_load
    from odf.table import Table, TableCell, TableRow
    from odf.text import P
except ImportError:  # pragma: no cover
    teletype = None
    CDATASection = None
    Element = None
    Node = None
    Text = None
    odf_load = None
    Table = None
    TableCell = None
    TableRow = None
    P = None

SUPPORTED_CLUSTERS = ("alpha", "barnard", "capella", "julia", "romeo")
DEFAULT_ALIAS_MAP: Dict[str, str] = {
    # Add known one-off mismatches here as needed.
}
RELEASE_SHEET_PATTERN = re.compile(r"^r(?:\d{2}\.\d{2}|\d{4})$", re.IGNORECASE)


@dataclasses.dataclass
class ColumnIndices:
    software: int
    release: int
    status: int


@dataclasses.dataclass
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


@dataclasses.dataclass
class RowReport:
    cluster: str
    file_path: str
    sheet_name: str
    release: str
    software_name: str
    matched_easyconfigs: List[str]
    action: str
    reason: str


@dataclasses.dataclass
class SheetUpdateResult:
    stats: SheetStats
    reports: List[RowReport]
    new_rows: List[List[str]]
    header_idx: Optional[int]
    cols: Optional[ColumnIndices]


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def normalize_name(value: str) -> str:
    return normalize_text(value).casefold()


def parse_release_from_header(header_text: str) -> Optional[str]:
    text = normalize_text(header_text)
    match = re.search(r"(\d{2}\.\d{2})", text)
    if match:
        return f"r{match.group(1)}"
    return None


def parse_name_from_easyconfig_content(content: str) -> Optional[str]:
    match = re.search(r"^\s*name\s*=\s*(['\"])(.+?)\1\s*$", content, re.MULTILINE)
    if not match:
        return None
    return match.group(2).strip() or None


def fallback_name_from_filename(filename: str) -> str:
    stem = Path(filename).stem
    parts = stem.split("-")
    if len(parts) == 1:
        return stem

    for idx, token in enumerate(parts):
        if re.match(r"^v?\d", token):
            return stem if idx == 0 else "-".join(parts[:idx])

    return parts[0]


def is_valid_release_sheet_name(sheet_name: str) -> bool:
    return bool(RELEASE_SHEET_PATTERN.match(normalize_text(sheet_name)))


def normalized_release_from_sheet_name(sheet_name: str) -> str:
    return normalize_text(sheet_name).casefold()


def infer_cluster_from_filename(file_path: Path) -> Optional[str]:
    stem_norm = normalize_name(file_path.stem)
    tokens = [token for token in re.split(r"[^a-z0-9]+", stem_norm) if token]
    cluster_set = set(SUPPORTED_CLUSTERS)
    for token in tokens:
        if token in cluster_set:
            return token
    return None


def discover_ods_files(datashare_dir: Path) -> List[Path]:
    return sorted(
        [path for path in datashare_dir.iterdir() if path.is_file() and path.suffix.lower() == ".ods"]
    )


def discover_cluster_csv_file(datashare_dir: Path, cluster: str) -> Optional[Path]:
    candidates = [
        path
        for path in datashare_dir.iterdir()
        if path.is_file() and path.suffix.lower() == ".csv" and cluster in normalize_name(path.stem)
    ]
    return sorted(candidates)[0] if candidates else None


def _find_column_index(row_norm: Sequence[str], patterns: Sequence[str]) -> Optional[int]:
    for i, cell in enumerate(row_norm):
        for pattern in patterns:
            if re.search(pattern, cell):
                return i
    return None


def detect_header_row_and_columns(rows: Sequence[Sequence[str]]) -> Tuple[int, ColumnIndices]:
    for idx, row in enumerate(rows):
        row_norm = [normalize_name(cell) for cell in row]
        software_idx = _find_column_index(row_norm, [r"\bsoftware\b", r"\bsoftwares\b"])
        release_idx = _find_column_index(row_norm, [r"\brelease\b", r"easyconfig"])
        status_idx = _find_column_index(row_norm, [r"\bstatus\b"])

        if software_idx is not None and release_idx is not None and status_idx is not None:
            return idx, ColumnIndices(software=software_idx, release=release_idx, status=status_idx)

    raise ValueError("Could not detect header row and required columns (software/release/status).")


def read_easyconfig_index(repo_root: Path, cluster: str, release: str) -> Dict[str, List[str]]:
    target_dir = repo_root / "easyconfigs" / cluster / release
    if not target_dir.is_dir():
        return {}

    index: Dict[str, set] = defaultdict(set)
    for eb_path in sorted(target_dir.glob("*.eb")):
        try:
            content = eb_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            content = eb_path.read_text(encoding="latin-1")

        name = parse_name_from_easyconfig_content(content) or fallback_name_from_filename(eb_path.name)
        index[normalize_name(name)].add(eb_path.name)

    return {k: sorted(v) for k, v in index.items()}


def easyconfig_source_clusters(cluster: str) -> Tuple[str, ...]:
    if cluster == "alpha":
        return ("alpha", "romeo")
    return (cluster,)


def easyconfig_source_dirs(repo_root: Path, cluster: str, release: str) -> List[Path]:
    return [repo_root / "easyconfigs" / source_cluster / release for source_cluster in easyconfig_source_clusters(cluster)]


def read_merged_easyconfig_index(repo_root: Path, cluster: str, release: str) -> Dict[str, List[str]]:
    merged: Dict[str, set] = defaultdict(set)
    for source_cluster in easyconfig_source_clusters(cluster):
        source_index = read_easyconfig_index(repo_root=repo_root, cluster=source_cluster, release=release)
        for software_norm, filenames in source_index.items():
            merged[software_norm].update(filenames)
    return {software_norm: sorted(filenames) for software_norm, filenames in merged.items()}


def merge_filenames(existing_value: str, new_filenames: Sequence[str]) -> str:
    existing_parts = [normalize_text(part) for part in (existing_value or "").split(";") if normalize_text(part)]
    merged = sorted(set(existing_parts) | set(new_filenames))
    return "; ".join(merged)


def backup_file(path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = path.with_suffix(path.suffix + f".bak.{timestamp}")
    shutil.copy2(path, backup_path)
    return backup_path


def _find_fuzzy_candidates(software_norm: str, available_names: Iterable[str]) -> List[str]:
    return [candidate for candidate in available_names if software_norm in candidate or candidate in software_norm]


def apply_sheet_updates(
    *,
    cluster: str,
    file_path: Path,
    sheet_name: str,
    release: str,
    rows: List[List[str]],
    alias_map: Dict[str, str],
    repo_root: Path,
    dry_run: bool,
) -> SheetUpdateResult:
    stats = SheetStats(
        cluster=cluster,
        file_path=str(file_path),
        sheet_name=sheet_name,
        release=release,
    )
    reports: List[RowReport] = []

    try:
        header_idx, cols = detect_header_row_and_columns(rows)
    except ValueError:
        stats.skipped_reason = "header_or_required_columns_not_found"
        reports.append(
            RowReport(
                cluster=cluster,
                file_path=str(file_path),
                sheet_name=sheet_name,
                release=release,
                software_name="",
                matched_easyconfigs=[],
                action="skipped",
                reason=stats.skipped_reason,
            )
        )
        return SheetUpdateResult(stats=stats, reports=reports, new_rows=rows, header_idx=None, cols=None)

    source_dirs = easyconfig_source_dirs(repo_root=repo_root, cluster=cluster, release=release)
    existing_source_dirs = [path for path in source_dirs if path.is_dir()]
    if not existing_source_dirs:
        stats.skipped_reason = "easyconfig_directory_missing"
        reports.append(
            RowReport(
                cluster=cluster,
                file_path=str(file_path),
                sheet_name=sheet_name,
                release=release,
                software_name="",
                matched_easyconfigs=[],
                action="skipped",
                reason=f"{stats.skipped_reason}:{','.join(str(path) for path in source_dirs)}",
            )
        )
        return SheetUpdateResult(stats=stats, reports=reports, new_rows=rows, header_idx=header_idx, cols=cols)

    ec_index = read_merged_easyconfig_index(repo_root=repo_root, cluster=cluster, release=release)

    for r_idx in range(header_idx + 1, len(rows)):
        row = rows[r_idx]
        software_name = normalize_text(row[cols.software] if cols.software < len(row) else "")
        if not software_name:
            continue

        stats.rows_scanned += 1
        software_norm = normalize_name(software_name)
        lookup_norm = alias_map.get(software_norm, software_norm)
        filenames = ec_index.get(lookup_norm)

        if filenames:
            while len(row) <= max(cols.release, cols.status):
                row.append("")

            current_release_value = row[cols.release]
            merged_release_value = merge_filenames(current_release_value, filenames)

            changed = False
            if merged_release_value != current_release_value:
                row[cols.release] = merged_release_value
                changed = True

            if row[cols.status] != "Done":
                row[cols.status] = "Done"
                changed = True

            stats.matched_rows += 1
            if changed:
                stats.updated_rows += 1
                stats.changed = True

            reports.append(
                RowReport(
                    cluster=cluster,
                    file_path=str(file_path),
                    sheet_name=sheet_name,
                    release=release,
                    software_name=software_name,
                    matched_easyconfigs=filenames,
                    action="updated" if changed else "unchanged",
                    reason="exact_or_alias_match",
                )
            )
            continue

        fuzzy_candidates = _find_fuzzy_candidates(software_norm, ec_index.keys())
        if len(fuzzy_candidates) > 1:
            stats.ambiguous_rows += 1
            reason = f"ambiguous_candidates:{','.join(sorted(fuzzy_candidates)[:5])}"
        else:
            stats.unmatched_rows += 1
            reason = "single_fuzzy_candidate:" + fuzzy_candidates[0] if len(fuzzy_candidates) == 1 else "no_match"

        reports.append(
            RowReport(
                cluster=cluster,
                file_path=str(file_path),
                sheet_name=sheet_name,
                release=release,
                software_name=software_name,
                matched_easyconfigs=[],
                action="skipped",
                reason=reason,
            )
        )

    if dry_run:
        stats.changed = False
        stats.updated_rows = 0

    return SheetUpdateResult(stats=stats, reports=reports, new_rows=rows, header_idx=header_idx, cols=cols)


def _odf_cell_text(cell) -> str:
    if teletype is None:
        return ""
    return teletype.extractText(cell) or ""


def _iter_row_cells_with_expansion(row_elem) -> List[object]:
    expanded = []
    for child in row_elem.childNodes:
        if child.qname not in {
            ("urn:oasis:names:tc:opendocument:xmlns:table:1.0", "table-cell"),
            ("urn:oasis:names:tc:opendocument:xmlns:table:1.0", "covered-table-cell"),
        }:
            continue
        rep = int(child.getAttribute("numbercolumnsrepeated") or "1")
        expanded.extend([child] * rep)
    return expanded


def _clone_odf_node(node):
    node_type = getattr(node, "nodeType", None)
    owner_document = getattr(node, "ownerDocument", None)

    if node_type == Node.ELEMENT_NODE:
        clone = Element(qname=node.qname, check_grammar=False)
        clone.ownerDocument = owner_document
        for (namespace, localpart), value in node.attributes.items():
            clone.setAttrNS(namespace, localpart, value)
        for child in node.childNodes:
            clone.appendChild(_clone_odf_node(child))
        return clone

    if node_type == Node.TEXT_NODE:
        clone = Text(node.data)
        clone.ownerDocument = owner_document
        return clone

    if node_type == Node.CDATA_SECTION_NODE:
        clone = CDATASection(node.data)
        clone.ownerDocument = owner_document
        return clone

    raise TypeError(f"Unsupported ODF node type for clone: {node_type}")


def _ensure_single_cell_at_col(row_elem, col_idx: int):
    current_col = 0
    children = [
        cell
        for cell in row_elem.childNodes
        if cell.qname
        in {
            ("urn:oasis:names:tc:opendocument:xmlns:table:1.0", "table-cell"),
            ("urn:oasis:names:tc:opendocument:xmlns:table:1.0", "covered-table-cell"),
        }
    ]

    for cell in children:
        rep = int(cell.getAttribute("numbercolumnsrepeated") or "1")
        start = current_col
        end = current_col + rep - 1
        if start <= col_idx <= end:
            if rep == 1:
                return cell

            before_count = col_idx - start
            after_count = end - col_idx

            before = _clone_odf_node(cell) if before_count > 0 else None
            target = _clone_odf_node(cell)
            after = _clone_odf_node(cell) if after_count > 0 else None

            if before is not None:
                before.setAttribute("numbercolumnsrepeated", str(before_count))
            target.removeAttribute("numbercolumnsrepeated")
            if after is not None:
                after.setAttribute("numbercolumnsrepeated", str(after_count))

            if before is not None:
                row_elem.insertBefore(before, cell)
            row_elem.insertBefore(target, cell)
            if after is not None:
                row_elem.insertBefore(after, cell)
            row_elem.removeChild(cell)
            return target

        current_col = end + 1

    while current_col <= col_idx:
        new_cell = TableCell()
        row_elem.addElement(new_cell)
        if current_col == col_idx:
            return new_cell
        current_col += 1

    raise RuntimeError("Failed to ensure writable ODS cell.")


def _set_odf_cell_text(cell, value: str) -> None:
    for child in list(cell.childNodes):
        if child.qname == ("urn:oasis:names:tc:opendocument:xmlns:text:1.0", "p"):
            cell.removeChild(child)
    cell.setAttribute("valuetype", "string")
    cell.addElement(P(text=value))


def load_ods_workbook(path: Path):
    if odf_load is None:
        raise RuntimeError("odfpy is required to process ODS files. Install with: pip install odfpy")

    doc = odf_load(str(path))
    tables = doc.spreadsheet.getElementsByType(Table)
    if not tables:
        raise ValueError(f"No tables found in ODS: {path}")

    workbook_tables = []
    for table in tables:
        sheet_name = table.getAttribute("name") or ""
        row_elems = table.getElementsByType(TableRow)
        rows_text = []
        for row in row_elems:
            expanded_cells = _iter_row_cells_with_expansion(row)
            rows_text.append([_odf_cell_text(cell) for cell in expanded_cells])
        workbook_tables.append((sheet_name, row_elems, rows_text))

    return doc, workbook_tables


def save_ods_updates(
    *,
    row_elems,
    old_rows: Sequence[Sequence[str]],
    new_rows: Sequence[Sequence[str]],
    cols: ColumnIndices,
    header_idx: int,
) -> bool:
    changed = False
    for r_idx in range(header_idx + 1, min(len(row_elems), len(new_rows))):
        old_row = old_rows[r_idx] if r_idx < len(old_rows) else []
        new_row = new_rows[r_idx]

        for col_idx in (cols.release, cols.status):
            old_val = old_row[col_idx] if col_idx < len(old_row) else ""
            new_val = new_row[col_idx] if col_idx < len(new_row) else ""
            if old_val == new_val:
                continue
            target_cell = _ensure_single_cell_at_col(row_elems[r_idx], col_idx)
            _set_odf_cell_text(target_cell, new_val)
            changed = True

    return changed


def load_csv_rows(path: Path) -> List[List[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [list(row) for row in csv.reader(handle)]


def save_csv_rows(path: Path, rows: Sequence[Sequence[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)


def process_ods_file(
    *,
    file_path: Path,
    cluster: str,
    repo_root: Path,
    dry_run: bool,
    alias_map: Dict[str, str],
) -> Tuple[List[SheetStats], List[RowReport], bool]:
    doc, workbook_tables = load_ods_workbook(file_path)

    sheet_results: List[Tuple[SheetUpdateResult, Sequence[Sequence[str]], Sequence[object]]] = []
    all_stats: List[SheetStats] = []
    all_reports: List[RowReport] = []

    for sheet_name, row_elems, old_rows in workbook_tables:
        normalized_sheet = normalize_text(sheet_name)
        if not is_valid_release_sheet_name(normalized_sheet):
            stats = SheetStats(
                cluster=cluster,
                file_path=str(file_path),
                sheet_name=sheet_name,
                release="",
                skipped_reason="invalid_release_sheet_name",
            )
            report = RowReport(
                cluster=cluster,
                file_path=str(file_path),
                sheet_name=sheet_name,
                release="",
                software_name="",
                matched_easyconfigs=[],
                action="skipped",
                reason=f"invalid_release_sheet_name:{sheet_name}",
            )
            all_stats.append(stats)
            all_reports.append(report)
            continue

        release = normalized_release_from_sheet_name(normalized_sheet)
        result = apply_sheet_updates(
            cluster=cluster,
            file_path=file_path,
            sheet_name=sheet_name,
            release=release,
            rows=[list(row) for row in old_rows],
            alias_map=alias_map,
            repo_root=repo_root,
            dry_run=dry_run,
        )
        all_stats.append(result.stats)
        all_reports.extend(result.reports)
        sheet_results.append((result, old_rows, row_elems))

    if dry_run:
        return all_stats, all_reports, False

    should_write = any(result.stats.changed for result, _, _ in sheet_results)
    if not should_write:
        return all_stats, all_reports, False

    backup_file(file_path)
    workbook_changed = False
    for result, old_rows, row_elems in sheet_results:
        if not result.stats.changed or result.header_idx is None or result.cols is None:
            continue
        changed = save_ods_updates(
            row_elems=row_elems,
            old_rows=old_rows,
            new_rows=result.new_rows,
            cols=result.cols,
            header_idx=result.header_idx,
        )
        workbook_changed = workbook_changed or changed

    if workbook_changed:
        doc.save(str(file_path))

    if not workbook_changed:
        for result, _, _ in sheet_results:
            result.stats.changed = False
            result.stats.updated_rows = 0

    return all_stats, all_reports, workbook_changed


def process_csv_file(
    *,
    file_path: Path,
    cluster: str,
    repo_root: Path,
    dry_run: bool,
    alias_map: Dict[str, str],
) -> Tuple[List[SheetStats], List[RowReport], bool]:
    rows = load_csv_rows(file_path)
    header_idx, cols = detect_header_row_and_columns(rows)
    header_text = rows[header_idx][cols.release] if cols.release < len(rows[header_idx]) else ""
    release = parse_release_from_header(header_text) or ""
    if not release:
        stats = SheetStats(
            cluster=cluster,
            file_path=str(file_path),
            sheet_name="CSV",
            release="",
            skipped_reason="release_not_found_in_header",
        )
        report = RowReport(
            cluster=cluster,
            file_path=str(file_path),
            sheet_name="CSV",
            release="",
            software_name="",
            matched_easyconfigs=[],
            action="skipped",
            reason="release_not_found_in_header",
        )
        return [stats], [report], False

    result = apply_sheet_updates(
        cluster=cluster,
        file_path=file_path,
        sheet_name="CSV",
        release=release,
        rows=rows,
        alias_map=alias_map,
        repo_root=repo_root,
        dry_run=dry_run,
    )

    if result.stats.changed and not dry_run:
        backup_file(file_path)
        save_csv_rows(file_path, result.new_rows)

    return [result.stats], result.reports, result.stats.changed and not dry_run


def extract_share_token_from_url(share_url: str) -> str:
    parsed = urlparse((share_url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(
            "Malformed public-share URL: expected full URL like 'https://example.org/s/<share_token>'."
        )

    path = parsed.path.rstrip("/")
    match = re.search(r"(?:^|/)s/([^/]+)$", path)
    if not match or not match.group(1):
        raise ValueError(
            "Malformed public-share URL: could not extract share token from '/s/<share_token>'."
        )

    return match.group(1)


def build_public_share_dav_base(share_url: str) -> str:
    parsed = urlparse((share_url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("Malformed public-share URL: missing scheme or hostname.")
    token = extract_share_token_from_url(share_url)
    return f"{parsed.scheme}://{parsed.netloc}/public.php/dav/files/{token}"


def upload_files_via_authenticated_webdav(
    updated_files: Sequence[Path],
    *,
    webdav_url: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> None:
    base_url = webdav_url or os.environ.get("DATASHARE_WEBDAV_URL")
    user = username or os.environ.get("DATASHARE_WEBDAV_USERNAME")
    secret = password or os.environ.get("DATASHARE_WEBDAV_PASSWORD")

    if not (base_url and user and secret):
        raise RuntimeError(
            "Authenticated upload requested but DATASHARE_WEBDAV_URL, "
            "DATASHARE_WEBDAV_USERNAME, and DATASHARE_WEBDAV_PASSWORD are not fully configured."
        )
    if requests is None:
        raise RuntimeError("requests is required for upload. Install with: pip install requests")

    for path in updated_files:
        target_url = base_url.rstrip("/") + "/" + path.name
        with path.open("rb") as handle:
            response = requests.put(target_url, data=handle, auth=(user, secret), timeout=60)
        if not (200 <= response.status_code < 300):
            raise RuntimeError(f"Authenticated WebDAV upload failed with HTTP {response.status_code} for {target_url}.")
        logging.info("Uploaded via authenticated WebDAV: %s", target_url)


def upload_files_via_public_share_webdav(
    updated_files: Sequence[Path],
    *,
    share_url: Optional[str] = None,
    share_password: Optional[str] = None,
) -> None:
    if requests is None:
        raise RuntimeError("requests is required for upload. Install with: pip install requests")

    resolved_share_url = share_url or os.environ.get("DATASHARE_PUBLIC_SHARE_URL")
    resolved_password = share_password if share_password is not None else os.environ.get("DATASHARE_PUBLIC_SHARE_PASSWORD")

    if not resolved_share_url:
        raise RuntimeError("Public-share upload requested but DATASHARE_PUBLIC_SHARE_URL is missing.")

    try:
        dav_base = build_public_share_dav_base(resolved_share_url)
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc

    for path in updated_files:
        target_url = dav_base.rstrip("/") + "/" + path.name
        put_kwargs = {"timeout": 60}
        if resolved_password:
            put_kwargs["auth"] = ("anonymous", resolved_password)

        with path.open("rb") as handle:
            response = requests.put(target_url, data=handle, **put_kwargs)

        status = response.status_code
        if 200 <= status < 300:
            logging.info("Uploaded via public-share WebDAV: %s", target_url)
            continue

        base_message = f"Public-share WebDAV upload failed with HTTP {status} for {target_url}."
        if status == 401:
            raise RuntimeError(base_message + " Wrong share password or unauthorized public-share upload.")
        if status == 403:
            if resolved_password:
                raise RuntimeError(base_message + " Wrong password or this share does not permit uploads.")
            raise RuntimeError(base_message + " This share does not permit uploads (missing upload permissions).")
        if status in (404, 405):
            raise RuntimeError(base_message + " Public-share DAV endpoint is not supported or token/path is invalid.")

        raise RuntimeError(base_message + " Server returned non-2xx response.")


def serialize_report(sheet_stats: Sequence[SheetStats], row_reports: Sequence[RowReport], output_path: Path) -> None:
    payload = {
        "generated_at": datetime.now().isoformat(),
        "sheets": [
            {
                "cluster": stat.cluster,
                "file_path": stat.file_path,
                "sheet_name": stat.sheet_name,
                "release": stat.release,
                "rows_scanned": stat.rows_scanned,
                "matched_rows": stat.matched_rows,
                "unmatched_rows": stat.unmatched_rows,
                "ambiguous_rows": stat.ambiguous_rows,
                "updated_rows": stat.updated_rows,
                "changed": stat.changed,
                "skipped_reason": stat.skipped_reason,
            }
            for stat in sheet_stats
        ],
        "rows": [
            {
                "cluster": report.cluster,
                "file_path": report.file_path,
                "sheet_name": report.sheet_name,
                "release": report.release,
                "software_name": report.software_name,
                "matched_easyconfigs": report.matched_easyconfigs,
                "action": report.action,
                "reason": report.reason,
            }
            for report in row_reports
        ],
    }
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--datashare-dir", default="~/Desktop/Datashare", help="Path to Datashare folder")
    parser.add_argument("--repo", default="~/Desktop/barnard-ci", help="Path to barnard-ci repository")
    parser.add_argument("--cluster", choices=SUPPORTED_CLUSTERS, help="Process only files for one cluster")
    parser.add_argument("--dry-run", action="store_true", help="Compute changes without writing files")
    parser.add_argument("--report-out", default=None, help="Optional output path for JSON report")

    parser.add_argument("--authenticated-upload", action="store_true", help="Upload changed files via account WebDAV")
    parser.add_argument("--public-upload", action="store_true", help="Upload changed files via Nextcloud public-share WebDAV")

    parser.add_argument("--webdav-url", default=None, help="Authenticated WebDAV base URL (fallback: DATASHARE_WEBDAV_URL)")
    parser.add_argument("--webdav-username", default=None, help="Authenticated WebDAV username (fallback: DATASHARE_WEBDAV_USERNAME)")
    parser.add_argument("--webdav-password", default=None, help="Authenticated WebDAV password (fallback: DATASHARE_WEBDAV_PASSWORD)")

    parser.add_argument("--public-share-url", default=None, help="Nextcloud public-share URL, e.g. https://host/s/<token>")
    parser.add_argument("--public-share-password", default=None, help="Optional password for a password-protected public share")

    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    if args.authenticated_upload and args.public_upload:
        raise SystemExit("Choose only one upload mode: --authenticated-upload or --public-upload")

    datashare_dir = Path(args.datashare_dir).expanduser().resolve()
    repo_root = Path(args.repo).expanduser().resolve()

    if not datashare_dir.is_dir():
        raise SystemExit(f"Datashare directory not found: {datashare_dir}")
    if not repo_root.is_dir():
        raise SystemExit(f"Repository directory not found: {repo_root}")

    alias_map = {normalize_name(src): normalize_name(dst) for src, dst in DEFAULT_ALIAS_MAP.items()}

    all_sheet_stats: List[SheetStats] = []
    all_row_reports: List[RowReport] = []
    updated_files: List[Path] = []

    ods_files = discover_ods_files(datashare_dir)
    for file_path in ods_files:
        cluster = infer_cluster_from_filename(file_path)
        if not cluster:
            all_sheet_stats.append(
                SheetStats(
                    cluster="",
                    file_path=str(file_path),
                    sheet_name="",
                    release="",
                    skipped_reason="cluster_not_inferable_from_filename",
                )
            )
            all_row_reports.append(
                RowReport(
                    cluster="",
                    file_path=str(file_path),
                    sheet_name="",
                    release="",
                    software_name="",
                    matched_easyconfigs=[],
                    action="skipped",
                    reason="cluster_not_inferable_from_filename",
                )
            )
            continue
        if args.cluster and cluster != args.cluster:
            continue

        logging.info("Processing ODS file=%s cluster=%s", file_path, cluster)
        stats, reports, changed = process_ods_file(
            file_path=file_path,
            cluster=cluster,
            repo_root=repo_root,
            dry_run=args.dry_run,
            alias_map=alias_map,
        )
        all_sheet_stats.extend(stats)
        all_row_reports.extend(reports)
        if changed:
            updated_files.append(file_path)

    if not ods_files:
        # Legacy fallback: try CSV files by cluster.
        clusters = [args.cluster] if args.cluster else list(SUPPORTED_CLUSTERS)
        for cluster in clusters:
            file_path = discover_cluster_csv_file(datashare_dir, cluster)
            if not file_path:
                continue
            logging.info("Processing CSV file=%s cluster=%s", file_path, cluster)
            stats, reports, changed = process_csv_file(
                file_path=file_path,
                cluster=cluster,
                repo_root=repo_root,
                dry_run=args.dry_run,
                alias_map=alias_map,
            )
            all_sheet_stats.extend(stats)
            all_row_reports.extend(reports)
            if changed:
                updated_files.append(file_path)

    report_out = (
        Path(args.report_out).expanduser().resolve()
        if args.report_out
        else datashare_dir / f"datashare_update_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    serialize_report(all_sheet_stats, all_row_reports, report_out)

    total_rows = sum(stat.rows_scanned for stat in all_sheet_stats)
    total_updated = sum(stat.updated_rows for stat in all_sheet_stats)
    print("=== Datashare Software Stack Update Summary ===")
    print(f"Sheets processed: {len(all_sheet_stats)}")
    print(f"Rows scanned: {total_rows}")
    print(f"Rows updated: {total_updated}")
    print(f"Files updated: {len(updated_files)}")
    print(f"Report written: {report_out}")

    for stat in all_sheet_stats:
        print(
            f" - cluster={stat.cluster or 'n/a'}, file={stat.file_path}, sheet={stat.sheet_name or 'n/a'}, "
            f"release={stat.release or 'n/a'}, rows={stat.rows_scanned}, matched={stat.matched_rows}, "
            f"updated={stat.updated_rows}, skipped_reason={stat.skipped_reason or 'none'}"
        )

    if args.dry_run:
        return 0

    if args.authenticated_upload:
        upload_files_via_authenticated_webdav(
            updated_files,
            webdav_url=args.webdav_url,
            username=args.webdav_username,
            password=args.webdav_password,
        )
    elif args.public_upload:
        upload_files_via_public_share_webdav(
            updated_files,
            share_url=args.public_share_url,
            share_password=args.public_share_password,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
