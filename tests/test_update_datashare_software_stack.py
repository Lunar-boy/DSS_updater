import json
import types
from pathlib import Path

import pytest

import scripts.update_datashare_software_stack as updater
from scripts.update_datashare_software_stack import (
    build_public_share_dav_base,
    extract_share_token_from_url,
    infer_cluster_from_filename,
    load_ods_workbook,
    process_ods_file,
    serialize_report,
    upload_files_via_public_share_webdav,
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
    _, workbook_tables = load_ods_workbook(path)
    return {sheet_name: rows for sheet_name, _, rows in workbook_tables}


def test_extract_share_token_from_url_parses_normal_share_url():
    share_url = "https://datashare.tu-dresden.de/s/jJPQxRTHY9fbPtR?dir=/&editing=false&openfile=true"
    assert extract_share_token_from_url(share_url) == "jJPQxRTHY9fbPtR"


@pytest.mark.parametrize(
    "share_url",
    [
        "",
        "datashare.tu-dresden.de/s/token",
        "https://datashare.tu-dresden.de/no-share-path",
        "https://datashare.tu-dresden.de/s/",
    ],
)
def test_extract_share_token_from_url_rejects_malformed_urls(share_url: str):
    with pytest.raises(ValueError):
        extract_share_token_from_url(share_url)


def test_build_public_share_dav_base():
    share_url = "https://datashare.tu-dresden.de/s/jJPQxRTHY9fbPtR?dir=/&editing=false"
    assert (
        build_public_share_dav_base(share_url)
        == "https://datashare.tu-dresden.de/public.php/dav/files/jJPQxRTHY9fbPtR"
    )


def test_public_share_upload_without_password_has_no_auth(monkeypatch, tmp_path: Path):
    file_path = tmp_path / "Software_Stack_Barnard.ods"
    file_path.write_text("content", encoding="utf-8")

    calls = []

    def fake_put(url, **kwargs):
        calls.append((url, kwargs))
        return types.SimpleNamespace(status_code=201)

    monkeypatch.setattr(updater, "requests", types.SimpleNamespace(put=fake_put))

    upload_files_via_public_share_webdav(
        [file_path],
        share_url="https://datashare.tu-dresden.de/s/shareToken",
        share_password=None,
    )

    assert len(calls) == 1
    called_url, called_kwargs = calls[0]
    assert called_url.endswith("/public.php/dav/files/shareToken/Software_Stack_Barnard.ods")
    assert "auth" not in called_kwargs


def test_public_share_upload_with_password_uses_anonymous_auth(monkeypatch, tmp_path: Path):
    file_path = tmp_path / "Software_Stack_Barnard.ods"
    file_path.write_text("content", encoding="utf-8")

    calls = []

    def fake_put(url, **kwargs):
        calls.append((url, kwargs))
        return types.SimpleNamespace(status_code=201)

    monkeypatch.setattr(updater, "requests", types.SimpleNamespace(put=fake_put))

    upload_files_via_public_share_webdav(
        [file_path],
        share_url="https://datashare.tu-dresden.de/s/shareToken",
        share_password="secret123",
    )

    assert len(calls) == 1
    _, called_kwargs = calls[0]
    assert called_kwargs["auth"] == ("anonymous", "secret123")


@pytest.mark.parametrize(
    "status,expected_text",
    [
        (401, "Wrong share password"),
        (403, "missing upload permissions"),
        (404, "DAV endpoint is not supported"),
        (405, "DAV endpoint is not supported"),
    ],
)
def test_public_share_upload_fails_with_clear_errors(monkeypatch, tmp_path: Path, status: int, expected_text: str):
    file_path = tmp_path / "Software_Stack_Barnard.ods"
    file_path.write_text("content", encoding="utf-8")

    def fake_put(url, **kwargs):
        return types.SimpleNamespace(status_code=status)

    monkeypatch.setattr(updater, "requests", types.SimpleNamespace(put=fake_put))

    with pytest.raises(RuntimeError) as exc:
        upload_files_via_public_share_webdav(
            [file_path],
            share_url="https://datashare.tu-dresden.de/s/shareToken",
            share_password=None,
        )

    message = str(exc.value)
    assert f"HTTP {status}" in message
    assert expected_text in message


def test_public_share_upload_403_with_password_reports_wrong_password(monkeypatch, tmp_path: Path):
    file_path = tmp_path / "Software_Stack_Barnard.ods"
    file_path.write_text("content", encoding="utf-8")

    def fake_put(url, **kwargs):
        return types.SimpleNamespace(status_code=403)

    monkeypatch.setattr(updater, "requests", types.SimpleNamespace(put=fake_put))

    with pytest.raises(RuntimeError) as exc:
        upload_files_via_public_share_webdav(
            [file_path],
            share_url="https://datashare.tu-dresden.de/s/shareToken",
            share_password="wrong-password",
        )

    assert "Wrong password" in str(exc.value)


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


def test_dry_run_does_not_write_changes(tmp_path: Path):
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

    stats, _, changed = process_ods_file(
        file_path=ods_path,
        cluster="barnard",
        repo_root=repo,
        dry_run=True,
        alias_map={},
    )

    assert changed is False
    assert stats[0].updated_rows == 0
    assert ods_path.read_bytes() == before


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
