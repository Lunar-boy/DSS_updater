import json
from pathlib import Path

import pytest

from dss_updater.inventory import read_inventory_index
from dss_updater.reconciliation import reconcile_sheet


def _add_repo_entry(repo: Path, name: str, filenames: list[str]) -> None:
    target = repo / "easyconfigs" / "barnard" / "r2026"
    target.mkdir(parents=True, exist_ok=True)
    for filename in filenames:
        (target / filename).write_text(f"name = '{name}'\n", encoding="utf-8")


def _write_inventory(inventory: Path, entries: list[dict[str, object]]) -> None:
    target = inventory / "barnard" / "r2026.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({"cluster": "barnard", "release": "r2026", "software": entries}), encoding="utf-8")


def _reconcile(repo: Path, inventory: Path, software: str = "GROMACS", rows=None):
    rows = rows or [
        ["Software", "EasyConfig", "Status"],
        [software, "", ""],
    ]
    return reconcile_sheet(
        cluster="barnard",
        file_path=Path("Software_Stack_Barnard.ods"),
        sheet_name="r2026",
        release="r2026",
        rows=rows,
        alias_map={},
        repo_root=repo,
        inventory_dir=inventory,
    )


@pytest.mark.parametrize(
    "repo_files,installed_files,expected_status,expected_source,expected_reason",
    [
        (["GROMACS-repo.eb"], ["GROMACS-installed.eb"], "Done", "both", "exact_or_alias_match"),
        ([], ["GROMACS-installed.eb"], "Done", "installed", "exact_or_alias_match"),
        (["GROMACS-repo.eb"], [], "", "repo", "repo_only_not_installed"),
        ([], [], "", None, "no_match"),
    ],
)
def test_all_four_reconciliation_states(
    tmp_path: Path,
    repo_files: list[str],
    installed_files: list[str],
    expected_status: str,
    expected_source: str | None,
    expected_reason: str,
):
    repo = tmp_path / "repo"
    repo.mkdir()
    inventory = tmp_path / "inventory"
    inventory.mkdir()
    if repo_files:
        _add_repo_entry(repo, "GROMACS", repo_files)
    if installed_files:
        _write_inventory(inventory, [{"name": "GROMACS", "easyconfigs": installed_files}])

    result = _reconcile(repo, inventory)

    assert result.new_rows[1][2] == expected_status
    assert result.reports[0].source == expected_source
    assert result.reports[0].reason == expected_reason
    if expected_status == "Done":
        assert result.new_rows[1][1] == "; ".join(sorted(set(repo_files + installed_files)))


def test_inventory_preserves_multiple_versions_and_is_idempotent(tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()
    inventory = tmp_path / "inventory"
    inventory.mkdir()
    versions = ["GROMACS-2024.4.eb", "GROMACS-2025.1.eb"]
    _write_inventory(inventory, [{"name": "GROMACS", "easyconfigs": versions}])

    first = _reconcile(repo, inventory)
    second = _reconcile(repo, inventory, rows=[list(row) for row in first.new_rows])

    assert first.new_rows[1][1] == "; ".join(versions)
    assert first.stats.changed is True
    assert second.stats.changed is False
    assert second.stats.updated_rows == 0
    assert second.reports[0].action == "unchanged"


def test_missing_inventory_file_means_repo_entry_is_not_installed(tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()
    inventory = tmp_path / "inventory"
    inventory.mkdir()
    _add_repo_entry(repo, "GROMACS", ["GROMACS-2024.4.eb"])

    result = _reconcile(repo, inventory)

    assert result.stats.changed is False
    assert result.new_rows[1][2] == ""
    assert result.reports[0].source == "repo"
    assert result.reports[0].reason == "repo_only_not_installed"


def test_inventory_reader_accepts_name_mapping_format(tmp_path: Path):
    target = tmp_path / "barnard" / "r2026.json"
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps({"GROMACS": ["GROMACS-2024.4.eb"]}), encoding="utf-8")

    assert read_inventory_index(tmp_path, "barnard", "r2026") == {
        "gromacs": ["GROMACS-2024.4.eb"]
    }


def test_alpha_inventory_uses_alpha_and_romeo_for_the_same_release(tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()
    inventory = tmp_path / "inventory"
    for cluster, filename in (
        ("alpha", "GROMACS-alpha.eb"),
        ("romeo", "GROMACS-romeo.eb"),
    ):
        target = inventory / cluster / "r2026.json"
        target.parent.mkdir(parents=True)
        target.write_text(
            json.dumps({"software": [{"name": "GROMACS", "easyconfigs": [filename]}]}),
            encoding="utf-8",
        )

    result = reconcile_sheet(
        cluster="alpha",
        file_path=Path("Software_Stack_Alpha.ods"),
        sheet_name="r2026",
        release="r2026",
        rows=[["Software", "EasyConfig", "Status"], ["GROMACS", "", ""]],
        alias_map={},
        repo_root=repo,
        inventory_dir=inventory,
    )

    assert result.new_rows[1][1] == "GROMACS-alpha.eb; GROMACS-romeo.eb"
    assert result.new_rows[1][2] == "Done"
    assert result.reports[0].source == "installed"
