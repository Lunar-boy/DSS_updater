import json
import os
from pathlib import Path

import pytest

import dss_updater.cli as updater
import dss_updater.ods as ods_module
from dss_updater.cli import (
    DEFAULT_DATASHARE_DIR,
    DEFAULT_REPO_DIR,
    DEFAULT_REPORT_DIR,
    build_arg_parser,
)
from dss_updater.ods import ODSValidationError, load_workbook, save_workbook_safely
from dss_updater.reconciliation import infer_cluster_from_filename, process_ods_file
from dss_updater.reporting import serialize_report
from dss_updater.safety import (
    AmbiguousWorkbookError,
    ConcurrentModificationError,
    LibreOfficeLockError,
    NextcloudConflictError,
    ProcessLockError,
    discover_ods_files,
    ensure_fingerprint_unchanged,
    ensure_no_ambiguous_workbooks,
    ensure_no_conflict_files,
    ensure_no_libreoffice_locks,
    fingerprint_file,
    process_lock,
)


def _make_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "barnard-ci"
    repo.mkdir()
    return repo


def _add_easyconfigs(repo: Path, cluster: str, release: str, mapping: dict[str, list[str]]) -> None:
    target = repo / "easyconfigs" / cluster / release
    target.mkdir(parents=True, exist_ok=True)
    for software_name, filenames in mapping.items():
        for filename in filenames:
            (target / filename).write_text(f"name = '{software_name}'\n", encoding="utf-8")


def _make_ods(path: Path, sheets: dict[str, list[list[str]]]) -> None:
    odf = pytest.importorskip("odf")
    from odf.opendocument import OpenDocumentSpreadsheet
    from odf.table import Table, TableCell, TableRow
    from odf.text import P

    doc = OpenDocumentSpreadsheet()
    for sheet_name, rows in sheets.items():
        table = Table(name=sheet_name)
        for row_values in rows:
            row = TableRow()
            for value in row_values:
                cell = TableCell(valuetype="string")
                cell.addElement(P(text=value))
                row.addElement(cell)
            table.addElement(row)
        doc.spreadsheet.addElement(table)

    doc.save(str(path))


def _make_ods_with_repeated_easyconfig_status_cell(path: Path) -> None:
    pytest.importorskip("odf")
    from odf.opendocument import OpenDocumentSpreadsheet
    from odf.table import Table, TableCell, TableRow
    from odf.text import P

    doc = OpenDocumentSpreadsheet()
    table = Table(name="r2026")

    title_row = TableRow()
    title_cell = TableCell(valuetype="string")
    title_cell.addElement(P(text="Title"))
    title_row.addElement(title_cell)
    table.addElement(title_row)

    header_row = TableRow()
    for value in ["Category", "Software", "EasyConfig", "Status"]:
        cell = TableCell(valuetype="string")
        cell.addElement(P(text=value))
        header_row.addElement(cell)
    table.addElement(header_row)

    data_row = TableRow()
    category_cell = TableCell(valuetype="string")
    category_cell.addElement(P(text="Math"))
    data_row.addElement(category_cell)

    software_cell = TableCell(valuetype="string")
    software_cell.addElement(P(text="GROMACS"))
    data_row.addElement(software_cell)

    repeated_blank_cell = TableCell(numbercolumnsrepeated="2")
    data_row.addElement(repeated_blank_cell)
    table.addElement(data_row)

    doc.spreadsheet.addElement(table)
    doc.save(str(path))


def _sheet_rows(path: Path) -> dict[str, list[list[str]]]:
    _, workbook_tables = load_workbook(path)
    return {sheet_name: rows for sheet_name, _, rows in workbook_tables}


@pytest.mark.parametrize(
    "file_path,expected_cluster",
    [
        (Path("Software_Stack_Barnard.ods"), "barnard"),
        (Path("software_stack_ALPHA.ods"), "alpha"),
        (Path("software-stack-capella.ods"), "capella"),
        (Path("Software Stack Julia.ods"), "julia"),
        (Path("/home/nate/Desktop/Datashare/Software_Stack_Romeo.ods"), "romeo"),
        (Path("Software_Stack_Unknown.ods"), None),
    ],
)
def test_cluster_is_inferred_from_filename(file_path: Path, expected_cluster: str | None):
    assert infer_cluster_from_filename(file_path) == expected_cluster


def test_main_workflow_does_not_skip_underscore_cluster_filename(tmp_path: Path):
    repo = _make_repo(tmp_path)
    _add_easyconfigs(
        repo,
        "barnard",
        "r2026",
        {"GROMACS": ["GROMACS-2024.4-foss-2024a.eb"]},
    )

    datashare_dir = tmp_path / "Datashare"
    datashare_dir.mkdir()
    ods_path = datashare_dir / "Software_Stack_Barnard.ods"
    _make_ods(
        ods_path,
        {
            "r2026": [
                ["Title"],
                ["Category", "Software", "EasyConfig", "Status"],
                ["Math", "GROMACS", "", ""],
            ]
        },
    )

    report_path = datashare_dir / "report.json"
    exit_code = updater.main(
        [
            "--datashare-dir",
            str(datashare_dir),
            "--repo",
            str(repo),
            "--report-out",
            str(report_path),
        ]
    )

    assert exit_code == 0

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert len(report["sheets"]) == 1
    assert report["sheets"][0]["cluster"] == "barnard"
    assert report["sheets"][0]["skipped_reason"] == ""
    assert report["sheets"][0]["rows_scanned"] > 0
    assert report["sheets"][0]["updated_rows"] == 1

    updated_rows = _sheet_rows(ods_path)["r2026"]
    assert updated_rows[2][2] == "GROMACS-2024.4-foss-2024a.eb"
    assert updated_rows[2][3] == "Done"


def test_ods_multiple_sheets_all_processed_and_release_from_sheet_name(tmp_path: Path):
    repo = _make_repo(tmp_path)
    _add_easyconfigs(
        repo,
        "barnard",
        "r2026",
        {
            "GROMACS": ["GROMACS-2024.4-foss-2024a.eb"],
            "Python": ["Python-3.12.3-GCCcore-13.3.0.eb"],
        },
    )
    _add_easyconfigs(
        repo,
        "barnard",
        "r25.06",
        {
            "Julia": ["Julia-1.11.6-linux-x86_64.eb"],
        },
    )

    ods_path = tmp_path / "Software_Stack_Barnard.ods"
    _make_ods(
        ods_path,
        {
            "r2026": [
                ["Title"],
                ["Category", "Software", "Release/25.06", "Status"],
                ["Math", "GROMACS", "", ""],
                ["Core", "Python", "", ""],
            ],
            "r25.06": [
                ["Info"],
                ["Domain", "Software", "EasyConfig", "Status"],
                ["Lang", "Julia", "", ""],
            ],
        },
    )

    stats, reports, changed = process_ods_file(
        file_path=ods_path,
        cluster="barnard",
        repo_root=repo,
        dry_run=False,
        alias_map={},
    )

    assert changed is True
    assert {s.sheet_name for s in stats} == {"r2026", "r25.06"}
    assert {s.release for s in stats} == {"r2026", "r25.06"}

    rows_by_sheet = _sheet_rows(ods_path)
    r2026_rows = rows_by_sheet["r2026"]
    r2506_rows = rows_by_sheet["r25.06"]

    # release comes from sheet name, not stale header text "Release/25.06"
    assert r2026_rows[2][2] == "GROMACS-2024.4-foss-2024a.eb"
    assert r2026_rows[3][2] == "Python-3.12.3-GCCcore-13.3.0.eb"
    assert r2026_rows[2][3] == "Done"
    assert r2026_rows[3][3] == "Done"
    assert r2506_rows[2][2] == "Julia-1.11.6-linux-x86_64.eb"
    assert r2506_rows[2][3] == "Done"

    assert any(report.sheet_name == "r2026" and report.software_name == "GROMACS" for report in reports)


def test_ods_repeated_cell_is_split_and_updated_without_crash(tmp_path: Path):
    repo = _make_repo(tmp_path)
    _add_easyconfigs(
        repo,
        "barnard",
        "r2026",
        {"GROMACS": ["GROMACS-2024.4-foss-2024a.eb"]},
    )

    ods_path = tmp_path / "Software_Stack_Barnard.ods"
    _make_ods_with_repeated_easyconfig_status_cell(ods_path)

    stats, reports, changed = process_ods_file(
        file_path=ods_path,
        cluster="barnard",
        repo_root=repo,
        dry_run=False,
        alias_map={},
    )

    assert changed is True
    assert stats[0].updated_rows == 1
    assert any(report.software_name == "GROMACS" and report.action == "updated" for report in reports)

    updated_rows = _sheet_rows(ods_path)["r2026"]
    assert updated_rows[2][2] == "GROMACS-2024.4-foss-2024a.eb"
    assert updated_rows[2][3] == "Done"


def test_alpha_ods_uses_union_of_alpha_and_romeo_same_release(tmp_path: Path):
    repo = _make_repo(tmp_path)
    _add_easyconfigs(repo, "alpha", "r2026", {"GROMACS": ["GROMACS-2024.4-foss-2024a.eb"]})
    _add_easyconfigs(repo, "romeo", "r2026", {"GROMACS": ["GROMACS-2024.5-foss-2024b.eb"]})

    ods_path = tmp_path / "Software_Stack_Alpha.ods"
    _make_ods(
        ods_path,
        {
            "r2026": [
                ["Title"],
                ["Category", "Software", "EasyConfig", "Status"],
                ["Math", "GROMACS", "", ""],
            ]
        },
    )

    stats, _, changed = process_ods_file(
        file_path=ods_path,
        cluster="alpha",
        repo_root=repo,
        dry_run=False,
        alias_map={},
    )

    assert changed is True
    assert stats[0].updated_rows == 1
    updated_rows = _sheet_rows(ods_path)["r2026"]
    assert (
        updated_rows[2][2]
        == "GROMACS-2024.4-foss-2024a.eb; GROMACS-2024.5-foss-2024b.eb"
    )
    assert updated_rows[2][3] == "Done"


def test_alpha_release_isolation_does_not_leak_other_romeo_releases(tmp_path: Path):
    repo = _make_repo(tmp_path)
    _add_easyconfigs(repo, "alpha", "r2026", {"GROMACS": ["GROMACS-2024.4-foss-2024a.eb"]})
    _add_easyconfigs(repo, "romeo", "r25.06", {"GROMACS": ["GROMACS-2025.1-foss-2025a.eb"]})

    ods_path = tmp_path / "Software_Stack_Alpha.ods"
    _make_ods(
        ods_path,
        {
            "r2026": [
                ["Title"],
                ["Category", "Software", "EasyConfig", "Status"],
                ["Math", "GROMACS", "", ""],
            ]
        },
    )

    _, _, changed = process_ods_file(
        file_path=ods_path,
        cluster="alpha",
        repo_root=repo,
        dry_run=False,
        alias_map={},
    )

    assert changed is True
    updated_rows = _sheet_rows(ods_path)["r2026"]
    assert updated_rows[2][2] == "GROMACS-2024.4-foss-2024a.eb"
    assert "GROMACS-2025.1-foss-2025a.eb" not in updated_rows[2][2]
    assert updated_rows[2][3] == "Done"


def test_non_alpha_clusters_keep_single_source_lookup(tmp_path: Path):
    repo = _make_repo(tmp_path)
    _add_easyconfigs(repo, "barnard", "r2026", {"GROMACS": ["GROMACS-2024.4-foss-2024a.eb"]})
    _add_easyconfigs(repo, "romeo", "r2026", {"GROMACS": ["GROMACS-2024.5-foss-2024b.eb"]})

    ods_path = tmp_path / "Software_Stack_Barnard.ods"
    _make_ods(
        ods_path,
        {
            "r2026": [
                ["Title"],
                ["Category", "Software", "EasyConfig", "Status"],
                ["Math", "GROMACS", "", ""],
            ]
        },
    )

    _, _, changed = process_ods_file(
        file_path=ods_path,
        cluster="barnard",
        repo_root=repo,
        dry_run=False,
        alias_map={},
    )

    assert changed is True
    updated_rows = _sheet_rows(ods_path)["r2026"]
    assert updated_rows[2][2] == "GROMACS-2024.4-foss-2024a.eb"
    assert "GROMACS-2024.5-foss-2024b.eb" not in updated_rows[2][2]
    assert updated_rows[2][3] == "Done"


def test_alpha_uses_romeo_fallback_when_alpha_release_dir_missing(tmp_path: Path):
    repo = _make_repo(tmp_path)
    _add_easyconfigs(repo, "romeo", "r2026", {"GROMACS": ["GROMACS-2024.5-foss-2024b.eb"]})

    ods_path = tmp_path / "Software_Stack_Alpha.ods"
    _make_ods(
        ods_path,
        {
            "r2026": [
                ["Title"],
                ["Category", "Software", "EasyConfig", "Status"],
                ["Math", "GROMACS", "", ""],
            ]
        },
    )

    stats, reports, changed = process_ods_file(
        file_path=ods_path,
        cluster="alpha",
        repo_root=repo,
        dry_run=False,
        alias_map={},
    )

    assert changed is True
    assert stats[0].skipped_reason == ""
    assert not any("easyconfig_directory_missing" in report.reason for report in reports)
    updated_rows = _sheet_rows(ods_path)["r2026"]
    assert updated_rows[2][2] == "GROMACS-2024.5-foss-2024b.eb"
    assert updated_rows[2][3] == "Done"


def test_missing_easyconfig_directory_is_reported_and_skipped(tmp_path: Path):
    repo = _make_repo(tmp_path)
    ods_path = tmp_path / "Software_Stack_Barnard.ods"
    _make_ods(
        ods_path,
        {
            "r2026": [
                ["Title"],
                ["Category", "Software", "EasyConfig", "Status"],
                ["Math", "GROMACS", "", ""],
            ]
        },
    )

    stats, reports, changed = process_ods_file(
        file_path=ods_path,
        cluster="barnard",
        repo_root=repo,
        dry_run=False,
        alias_map={},
    )

    assert changed is False
    assert stats[0].skipped_reason == "easyconfig_directory_missing"
    assert any("easyconfig_directory_missing" in report.reason for report in reports)


def test_idempotent_second_run_has_no_changes(tmp_path: Path):
    repo = _make_repo(tmp_path)
    _add_easyconfigs(
        repo,
        "barnard",
        "r2026",
        {"GROMACS": ["GROMACS-2024.4-foss-2024a.eb"]},
    )

    ods_path = tmp_path / "Software_Stack_Barnard.ods"
    _make_ods(
        ods_path,
        {
            "r2026": [
                ["Title"],
                ["Category", "Software", "EasyConfig", "Status"],
                ["Math", "GROMACS", "", ""],
            ]
        },
    )

    first_stats, _, first_changed = process_ods_file(
        file_path=ods_path,
        cluster="barnard",
        repo_root=repo,
        dry_run=False,
        alias_map={},
    )
    assert first_changed is True
    assert first_stats[0].updated_rows == 1

    second_stats, second_reports, second_changed = process_ods_file(
        file_path=ods_path,
        cluster="barnard",
        repo_root=repo,
        dry_run=False,
        alias_map={},
    )
    assert second_changed is False
    assert second_stats[0].updated_rows == 0
    assert any(report.action == "unchanged" for report in second_reports)


def test_dry_run_reports_changes_without_writing_or_backing_up(tmp_path: Path):
    repo = _make_repo(tmp_path)
    _add_easyconfigs(
        repo,
        "barnard",
        "r2026",
        {"GROMACS": ["GROMACS-2024.4-foss-2024a.eb"]},
    )

    ods_path = tmp_path / "Software_Stack_Barnard.ods"
    _make_ods(
        ods_path,
        {
            "r2026": [
                ["Title"],
                ["Category", "Software", "EasyConfig", "Status"],
                ["Math", "GROMACS", "", ""],
            ]
        },
    )
    before = ods_path.read_bytes()

    stats, reports, changed = process_ods_file(
        file_path=ods_path,
        cluster="barnard",
        repo_root=repo,
        dry_run=True,
        alias_map={},
    )

    assert changed is True
    assert stats[0].changed is True
    assert stats[0].updated_rows == 1
    assert any(report.action == "updated" for report in reports)
    assert ods_path.read_bytes() == before
    assert not list(tmp_path.glob("Software_Stack_Barnard.ods.bak.*"))


def test_cli_dry_run_summary_and_default_report_location(
    monkeypatch, tmp_path: Path, capsys
):
    repo = _make_repo(tmp_path)
    _add_easyconfigs(
        repo,
        "barnard",
        "r2026",
        {"GROMACS": ["GROMACS-2024.4-foss-2024a.eb"]},
    )
    datashare_dir = tmp_path / "Datashare"
    datashare_dir.mkdir()
    ods_path = datashare_dir / "Software_Stack_Barnard.ods"
    _make_ods(
        ods_path,
        {
            "r2026": [
                ["Title"],
                ["Category", "Software", "EasyConfig", "Status"],
                ["Math", "GROMACS", "", ""],
            ]
        },
    )
    before = ods_path.read_bytes()
    monkeypatch.setenv("HOME", str(tmp_path / "home"))

    exit_code = updater.main(
        [
            "--datashare-dir",
            str(datashare_dir),
            "--repo",
            str(repo),
            "--dry-run",
        ]
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "=== DSS Dry-Run Reconciliation Summary ===" in output
    assert "Sheets processed: 1" in output
    assert "Rows scanned: 1" in output
    assert "Rows that would be updated: 1" in output
    assert "Files that would be updated: 1" in output
    assert "Files written: 0" in output

    reports = list((tmp_path / "home" / "dss_updater" / "reports").glob(
        "dss_update_report_*.json"
    ))
    assert len(reports) == 1
    payload = json.loads(reports[0].read_text(encoding="utf-8"))
    assert payload["sheets"][0]["updated_rows"] == 1
    assert payload["sheets"][0]["changed"] is True
    assert payload["rows"][0]["action"] == "updated"
    assert ods_path.read_bytes() == before
    assert not list(datashare_dir.glob("*.bak.*"))
    assert not list(datashare_dir.glob("dss_update_report_*.json"))


def test_report_contains_sheet_level_information(tmp_path: Path):
    sheet_stats = [
        updater.SheetStats(
            cluster="barnard",
            file_path="/tmp/Software_Stack_Barnard.ods",
            sheet_name="r2026",
            release="r2026",
            rows_scanned=4,
            matched_rows=3,
            updated_rows=2,
        )
    ]
    row_reports = [
        updater.RowReport(
            cluster="barnard",
            file_path="/tmp/Software_Stack_Barnard.ods",
            sheet_name="r2026",
            release="r2026",
            software_name="GROMACS",
            matched_easyconfigs=["GROMACS-2024.4-foss-2024a.eb"],
            action="updated",
            reason="exact_or_alias_match",
        )
    ]

    report_path = tmp_path / "report.json"
    serialize_report(sheet_stats, row_reports, report_path)

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert "sheets" in payload
    assert payload["sheets"][0]["sheet_name"] == "r2026"
    assert payload["sheets"][0]["release"] == "r2026"
    assert payload["rows"][0]["sheet_name"] == "r2026"


def test_discovery_only_returns_software_stack_workbooks(tmp_path: Path):
    expected = tmp_path / "Software_Stack_Barnard.ods"
    expected.write_bytes(b"ods")
    (tmp_path / "unrelated.ods").write_bytes(b"ods")
    (tmp_path / "Software_Stack_Barnard.csv").write_text("csv", encoding="utf-8")
    (tmp_path / "software_stack_alpha.ods").write_bytes(b"ods")

    assert discover_ods_files(tmp_path) == [expected]


def test_nextcloud_conflict_file_aborts_preflight(tmp_path: Path):
    conflict = tmp_path / "Software_Stack_Barnard (conflicted copy 2026-08-12).ods"
    conflict.write_bytes(b"conflict")

    with pytest.raises(NextcloudConflictError, match="no workbooks were modified"):
        ensure_no_conflict_files(tmp_path)


def test_cli_defaults_to_local_nextcloud_and_barnard_ci_paths():
    args = build_arg_parser().parse_args([])

    assert args.datashare_dir == DEFAULT_DATASHARE_DIR
    assert args.repo == DEFAULT_REPO_DIR
    assert DEFAULT_REPORT_DIR == "~/dss_updater/reports"
    assert not hasattr(args, "public_upload")
    assert not hasattr(args, "authenticated_upload")


def test_changed_ods_is_backed_up_and_atomically_replaced(tmp_path: Path):
    repo = _make_repo(tmp_path)
    _add_easyconfigs(repo, "barnard", "r2026", {"GROMACS": ["GROMACS-2024.4.eb"]})
    ods_path = tmp_path / "Software_Stack_Barnard.ods"
    _make_ods(
        ods_path,
        {
            "r2026": [
                ["Title"],
                ["Category", "Software", "EasyConfig", "Status"],
                ["Math", "GROMACS", "", ""],
            ]
        },
    )
    original_inode = ods_path.stat().st_ino

    _, _, changed = process_ods_file(
        file_path=ods_path,
        cluster="barnard",
        repo_root=repo,
        dry_run=False,
        alias_map={},
    )

    assert changed is True
    assert ods_path.stat().st_ino != original_inode
    assert len(list(tmp_path.glob("Software_Stack_Barnard.ods.bak.*"))) == 1
    assert not list(tmp_path.glob(".Software_Stack_Barnard.ods.*.tmp"))


def test_cli_conflict_abort_writes_nothing(tmp_path: Path):
    repo = _make_repo(tmp_path)
    datashare_dir = tmp_path / "Datashare"
    datashare_dir.mkdir()
    ods_path = datashare_dir / "Software_Stack_Barnard.ods"
    ods_path.write_bytes(b"original")
    (datashare_dir / "Software_Stack_Barnard (conflicted copy).ods").write_bytes(b"conflict")
    report_path = tmp_path / "report.json"

    with pytest.raises(SystemExit, match="no workbooks were modified"):
        updater.main(
            [
                "--datashare-dir",
                str(datashare_dir),
                "--repo",
                str(repo),
                "--report-out",
                str(report_path),
            ]
        )

    assert ods_path.read_bytes() == b"original"
    assert not report_path.exists()
    assert not list(datashare_dir.glob("*.bak.*"))


def test_alias_map_preserves_exact_matching_behavior(tmp_path: Path):
    repo = _make_repo(tmp_path)
    _add_easyconfigs(repo, "barnard", "r2026", {"Canonical Name": ["Canonical-1.0.eb"]})
    ods_path = tmp_path / "Software_Stack_Barnard.ods"
    _make_ods(
        ods_path,
        {
            "r2026": [
                ["Title"],
                ["Category", "Software", "EasyConfig", "Status"],
                ["Tools", "ODS Alias", "", ""],
            ]
        },
    )

    _, reports, changed = process_ods_file(
        file_path=ods_path,
        cluster="barnard",
        repo_root=repo,
        dry_run=False,
        alias_map={"ods alias": "canonical name"},
    )

    assert changed is True
    assert any(report.software_name == "ODS Alias" and report.action == "updated" for report in reports)
    assert _sheet_rows(ods_path)["r2026"][2][2] == "Canonical-1.0.eb"


def test_sha256_fingerprint_detects_same_size_and_mtime_change(tmp_path: Path):
    path = tmp_path / "Software_Stack_Barnard.ods"
    path.write_bytes(b"original")
    original = fingerprint_file(path)

    path.write_bytes(b"modified")
    assert path.stat().st_size == original.size
    path.touch()
    # Restore the recorded mtime so SHA256 is the field that detects the edit.
    os.utime(path, ns=(original.mtime_ns, original.mtime_ns))

    with pytest.raises(ConcurrentModificationError, match="changed during reconciliation"):
        ensure_fingerprint_unchanged(path, original)


def test_concurrent_modification_discards_staged_update(monkeypatch, tmp_path: Path):
    repo = _make_repo(tmp_path)
    _add_easyconfigs(repo, "barnard", "r2026", {"GROMACS": ["GROMACS-2024.4.eb"]})
    ods_path = tmp_path / "Software_Stack_Barnard.ods"
    _make_ods(
        ods_path,
        {
            "r2026": [
                ["Title"],
                ["Category", "Software", "EasyConfig", "Status"],
                ["Math", "GROMACS", "", ""],
            ]
        },
    )
    external_bytes = ods_path.read_bytes() + b"external change"
    real_validate = ods_module.validate_ods
    validation_calls = 0

    def validate_then_modify(path: Path) -> None:
        nonlocal validation_calls
        real_validate(path)
        validation_calls += 1
        if validation_calls == 1:
            ods_path.write_bytes(external_bytes)

    monkeypatch.setattr(ods_module, "validate_ods", validate_then_modify)

    with pytest.raises(ConcurrentModificationError, match="discarded"):
        process_ods_file(
            file_path=ods_path,
            cluster="barnard",
            repo_root=repo,
            dry_run=False,
            alias_map={},
        )

    assert ods_path.read_bytes() == external_bytes
    assert not list(tmp_path.glob("*.bak.*"))
    assert not list(tmp_path.glob(".Software_Stack_Barnard.ods.*.ods.tmp"))


def test_invalid_staged_ods_is_rejected_before_backup(tmp_path: Path):
    ods_path = tmp_path / "Software_Stack_Barnard.ods"
    _make_ods(ods_path, {"r2026": [["Software", "EasyConfig", "Status"]]})
    original_bytes = ods_path.read_bytes()
    original_fingerprint = fingerprint_file(ods_path)

    class InvalidDocument:
        def save(self, filename: str, addsuffix: bool = False) -> None:
            Path(filename).write_bytes(b"not an ODS ZIP")

    with pytest.raises(ODSValidationError, match="not a valid ZIP"):
        save_workbook_safely(InvalidDocument(), ods_path, original_fingerprint)

    assert ods_path.read_bytes() == original_bytes
    assert not list(tmp_path.glob("*.bak.*"))
    assert not list(tmp_path.glob(".Software_Stack_Barnard.ods.*.ods.tmp"))


def test_relevant_libreoffice_lock_aborts(tmp_path: Path):
    target = tmp_path / "Software_Stack_Barnard.ods"
    target.write_bytes(b"ods")
    lock = tmp_path / ".~lock.Software_Stack_Barnard.ods#"
    lock.write_text("lock metadata", encoding="utf-8")

    with pytest.raises(LibreOfficeLockError, match="Close the workbook"):
        ensure_no_libreoffice_locks(tmp_path, [target])


def test_cli_libreoffice_lock_returns_nonzero_before_reading_ods(tmp_path: Path):
    repo = _make_repo(tmp_path)
    datashare_dir = tmp_path / "Datashare"
    datashare_dir.mkdir()
    target = datashare_dir / "Software_Stack_Barnard.ods"
    target.write_bytes(b"not opened because preflight aborts")
    (datashare_dir / ".~lock.Software_Stack_Barnard.ods#").write_text(
        "lock metadata", encoding="utf-8"
    )

    with pytest.raises(SystemExit, match="LibreOffice lock"):
        updater.main(["--datashare-dir", str(datashare_dir), "--repo", str(repo)])


def test_process_lock_rejects_second_holder(tmp_path: Path):
    with process_lock(tmp_path):
        with pytest.raises(ProcessLockError, match="already running"):
            with process_lock(tmp_path):
                pytest.fail("second process lock should not be acquired")


def test_cli_returns_nonzero_when_process_lock_is_held(tmp_path: Path):
    repo = _make_repo(tmp_path)
    datashare_dir = tmp_path / "Datashare"
    datashare_dir.mkdir()

    with process_lock(datashare_dir):
        with pytest.raises(SystemExit, match="already running"):
            updater.main(["--datashare-dir", str(datashare_dir), "--repo", str(repo)])


def test_duplicate_or_variant_cluster_workbooks_abort(tmp_path: Path):
    canonical = tmp_path / "Software_Stack_Barnard.ods"
    duplicate = tmp_path / "Software_Stack_Barnard (copy).ods"
    canonical.write_bytes(b"canonical")
    duplicate.write_bytes(b"duplicate")

    with pytest.raises(AmbiguousWorkbookError, match="will not merge"):
        ensure_no_ambiguous_workbooks(tmp_path, updater.SUPPORTED_CLUSTERS)


def test_conflict_appearing_after_preflight_aborts_before_replace(monkeypatch, tmp_path: Path):
    repo = _make_repo(tmp_path)
    _add_easyconfigs(repo, "barnard", "r2026", {"GROMACS": ["GROMACS-2024.4.eb"]})
    datashare_dir = tmp_path / "Datashare"
    datashare_dir.mkdir()
    ods_path = datashare_dir / "Software_Stack_Barnard.ods"
    _make_ods(
        ods_path,
        {
            "r2026": [
                ["Title"],
                ["Category", "Software", "EasyConfig", "Status"],
                ["Math", "GROMACS", "", ""],
            ]
        },
    )
    original_bytes = ods_path.read_bytes()
    real_validate = ods_module.validate_ods
    validation_calls = 0

    def validate_then_create_conflict(path: Path) -> None:
        nonlocal validation_calls
        real_validate(path)
        validation_calls += 1
        if validation_calls == 1:
            (datashare_dir / "Software_Stack_Barnard (conflicted copy).ods").write_bytes(
                b"late conflict"
            )

    monkeypatch.setattr(ods_module, "validate_ods", validate_then_create_conflict)

    with pytest.raises(SystemExit, match="Nextcloud conflict"):
        updater.main(
            [
                "--datashare-dir",
                str(datashare_dir),
                "--repo",
                str(repo),
                "--report-out",
                str(tmp_path / "report.json"),
            ]
        )

    assert ods_path.read_bytes() == original_bytes
    assert not list(datashare_dir.glob("*.bak.*"))
    assert not list(datashare_dir.glob(".Software_Stack_Barnard.ods.*.ods.tmp"))
    assert not (tmp_path / "report.json").exists()


def test_libreoffice_lock_appearing_after_preflight_aborts_before_replace(
    monkeypatch, tmp_path: Path
):
    repo = _make_repo(tmp_path)
    _add_easyconfigs(repo, "barnard", "r2026", {"GROMACS": ["GROMACS-2024.4.eb"]})
    datashare_dir = tmp_path / "Datashare"
    datashare_dir.mkdir()
    ods_path = datashare_dir / "Software_Stack_Barnard.ods"
    _make_ods(
        ods_path,
        {
            "r2026": [
                ["Title"],
                ["Category", "Software", "EasyConfig", "Status"],
                ["Math", "GROMACS", "", ""],
            ]
        },
    )
    original_bytes = ods_path.read_bytes()
    real_validate = ods_module.validate_ods
    validation_calls = 0

    def validate_then_create_lock(path: Path) -> None:
        nonlocal validation_calls
        real_validate(path)
        validation_calls += 1
        if validation_calls == 1:
            (datashare_dir / ".~lock.Software_Stack_Barnard.ods#").write_text(
                "late LibreOffice lock", encoding="utf-8"
            )

    monkeypatch.setattr(ods_module, "validate_ods", validate_then_create_lock)

    with pytest.raises(SystemExit, match="LibreOffice lock"):
        updater.main(
            [
                "--datashare-dir",
                str(datashare_dir),
                "--repo",
                str(repo),
                "--report-out",
                str(tmp_path / "report.json"),
            ]
        )

    assert ods_path.read_bytes() == original_bytes
    assert not list(datashare_dir.glob("*.bak.*"))
    assert not list(datashare_dir.glob(".Software_Stack_Barnard.ods.*.ods.tmp"))
    assert not (tmp_path / "report.json").exists()
