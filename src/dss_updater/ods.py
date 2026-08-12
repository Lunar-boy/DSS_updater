"""ODS loading, in-place cell manipulation, and atomic saving."""

from __future__ import annotations

import os
import shutil
import tempfile
import zipfile
from collections.abc import Callable, Sequence
from pathlib import Path

from odf import teletype
from odf.element import CDATASection, Element, Node, Text
from odf.opendocument import load as odf_load
from odf.table import Table, TableCell, TableRow
from odf.text import P

from .models import ColumnIndices
from .safety import (
    FileFingerprint,
    SafetyError,
    backup_file,
    ensure_fingerprint_unchanged,
    fsync_directory,
)

CELL_QNAMES = {
    ("urn:oasis:names:tc:opendocument:xmlns:table:1.0", "table-cell"),
    ("urn:oasis:names:tc:opendocument:xmlns:table:1.0", "covered-table-cell"),
}


class ODSValidationError(SafetyError):
    """Raised when a staged or replaced ODS cannot be validated."""


def _cell_text(cell: object) -> str:
    return teletype.extractText(cell) or ""


def _expanded_cells(row_elem: object) -> list[object]:
    expanded: list[object] = []
    for child in row_elem.childNodes:
        if child.qname in CELL_QNAMES:
            expanded.extend([child] * int(child.getAttribute("numbercolumnsrepeated") or "1"))
    return expanded


def _clone_node(node: object):
    node_type = getattr(node, "nodeType", None)
    owner_document = getattr(node, "ownerDocument", None)
    if node_type == Node.ELEMENT_NODE:
        clone = Element(qname=node.qname, check_grammar=False)
        clone.ownerDocument = owner_document
        for (namespace, localpart), value in node.attributes.items():
            clone.setAttrNS(namespace, localpart, value)
        for child in node.childNodes:
            clone.appendChild(_clone_node(child))
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


def _ensure_single_cell(row_elem: object, col_idx: int):
    current_col = 0
    children = [cell for cell in row_elem.childNodes if cell.qname in CELL_QNAMES]
    for cell in children:
        repeated = int(cell.getAttribute("numbercolumnsrepeated") or "1")
        start, end = current_col, current_col + repeated - 1
        if start <= col_idx <= end:
            if repeated == 1:
                return cell
            before_count, after_count = col_idx - start, end - col_idx
            before = _clone_node(cell) if before_count else None
            target = _clone_node(cell)
            after = _clone_node(cell) if after_count else None
            if before is not None:
                before.setAttribute("numbercolumnsrepeated", str(before_count))
                row_elem.insertBefore(before, cell)
            target.removeAttribute("numbercolumnsrepeated")
            row_elem.insertBefore(target, cell)
            if after is not None:
                after.setAttribute("numbercolumnsrepeated", str(after_count))
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
    raise RuntimeError("Failed to ensure writable ODS cell")


def _set_cell_text(cell: object, value: str) -> None:
    for child in list(cell.childNodes):
        if child.qname == ("urn:oasis:names:tc:opendocument:xmlns:text:1.0", "p"):
            cell.removeChild(child)
    cell.setAttribute("valuetype", "string")
    cell.addElement(P(text=value))


def load_workbook(path: Path):
    doc = odf_load(str(path))
    tables = doc.spreadsheet.getElementsByType(Table)
    if not tables:
        raise ValueError(f"No tables found in ODS: {path}")
    workbook_tables = []
    for table in tables:
        row_elems = table.getElementsByType(TableRow)
        rows = [[_cell_text(cell) for cell in _expanded_cells(row)] for row in row_elems]
        workbook_tables.append((table.getAttribute("name") or "", row_elems, rows))
    return doc, workbook_tables


def apply_updates(
    *,
    row_elems: Sequence[object],
    old_rows: Sequence[Sequence[str]],
    new_rows: Sequence[Sequence[str]],
    cols: ColumnIndices,
    header_idx: int,
) -> bool:
    changed = False
    for row_idx in range(header_idx + 1, min(len(row_elems), len(new_rows))):
        old_row = old_rows[row_idx] if row_idx < len(old_rows) else []
        new_row = new_rows[row_idx]
        for col_idx in (cols.release, cols.status):
            old_value = old_row[col_idx] if col_idx < len(old_row) else ""
            new_value = new_row[col_idx] if col_idx < len(new_row) else ""
            if old_value != new_value:
                _set_cell_text(_ensure_single_cell(row_elems[row_idx], col_idx), new_value)
                changed = True
    return changed


def validate_ods(path: Path) -> None:
    """Validate ZIP integrity and prove that odfpy can reopen the workbook."""
    if not zipfile.is_zipfile(path):
        raise ODSValidationError(f"Staged ODS is not a valid ZIP container: {path}")
    try:
        with zipfile.ZipFile(path) as archive:
            corrupt_member = archive.testzip()
        if corrupt_member is not None:
            raise ODSValidationError(
                f"Staged ODS contains a corrupt ZIP member {corrupt_member!r}: {path}"
            )
        odf_load(str(path))
    except ODSValidationError:
        raise
    except Exception as exc:
        raise ODSValidationError(f"ODS validation failed for {path}: {exc}") from exc


def save_workbook_safely(
    doc: object,
    path: Path,
    original_fingerprint: FileFingerprint,
    pre_replace_check: Callable[[], None] | None = None,
) -> Path:
    """Stage, validate, concurrency-check, back up, and replace one workbook."""
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".ods.tmp", dir=path.parent
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    try:
        doc.save(str(temporary_path), addsuffix=False)
        with temporary_path.open("rb") as handle:
            os.fsync(handle.fileno())
        validate_ods(temporary_path)

        if pre_replace_check is not None:
            pre_replace_check()
        ensure_fingerprint_unchanged(path, original_fingerprint)
        backup_path = backup_file(path)
        # Copying a large backup widens the check/replace window. Check once more
        # immediately before replacement so edits made during the copy also abort.
        ensure_fingerprint_unchanged(path, original_fingerprint)

        shutil.copymode(path, temporary_path)
        os.replace(temporary_path, path)
        fsync_directory(path.parent)
        validate_ods(path)
        return backup_path
    finally:
        temporary_path.unlink(missing_ok=True)
