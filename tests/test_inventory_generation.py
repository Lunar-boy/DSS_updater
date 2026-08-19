import base64
import json
import subprocess
from pathlib import Path

import pytest

import dss_updater.inventory_cli as inventory_cli
from dss_updater.inventory_generation import (
    InventoryGenerationError,
    build_inventory,
    scan_local_tree,
    scan_remote_tree,
    write_inventory,
)


def _installed_easyconfig(root: Path, prefix: str, filename: str, name: str) -> Path:
    path = root / prefix / "easybuild" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"name = {name!r}\n", encoding="utf-8")
    return path


def test_generates_single_version_and_creates_output_parents(tmp_path: Path):
    root = tmp_path / "software"
    _installed_easyconfig(root, "bzip2/1.0.8", "bzip2-1.0.8-GCCcore-14.2.0.eb", "bzip2")
    output = tmp_path / "inventory" / "romeo" / "r2026.json"

    assert inventory_cli.main([
        "--cluster", "romeo",
        "--release", "r2026",
        "--software-root", str(root),
        "--output", str(output),
    ]) == 0

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["cluster"] == "romeo"
    assert payload["release"] == "r2026"
    assert payload["generated_at"]
    assert payload["software"] == [{
        "name": "bzip2",
        "easyconfigs": ["bzip2-1.0.8-GCCcore-14.2.0.eb"],
    }]


def test_multiple_versions_are_sorted_and_duplicate_filenames_are_removed(tmp_path: Path):
    root = tmp_path / "software"
    _installed_easyconfig(root, "z-copy", "GROMACS-2025.1-foss-2025a.eb", "GROMACS")
    _installed_easyconfig(root, "a-copy", "GROMACS-2024.4-foss-2024a.eb", "GROMACS")
    _installed_easyconfig(root, "duplicate", "GROMACS-2024.4-foss-2024a.eb", "  gromacs  ")

    payload = build_inventory(scan_local_tree(root), "romeo", "r2026", "2026-08-19T18:00:00+02:00")

    assert payload["software"] == [{
        "name": "GROMACS",
        "easyconfigs": [
            "GROMACS-2024.4-foss-2024a.eb",
            "GROMACS-2025.1-foss-2025a.eb",
        ],
    }]


def test_empty_installation_tree_produces_empty_inventory(tmp_path: Path):
    root = tmp_path / "software"
    root.mkdir()

    payload = build_inventory(scan_local_tree(root), "julia", "r2026", "2026-08-19T18:00:00+02:00")

    assert payload["software"] == []


def test_malformed_easyconfig_reports_its_path(tmp_path: Path):
    root = tmp_path / "software"
    path = root / "broken" / "easybuild" / "broken-1.0.eb"
    path.parent.mkdir(parents=True)
    path.write_text("this is not an EasyConfig name assignment\n", encoding="utf-8")

    with pytest.raises(InventoryGenerationError, match=r"broken-1\.0\.eb.*name assignment"):
        build_inventory(scan_local_tree(root), "romeo", "r2026", "2026-08-19T18:00:00+02:00")


def test_missing_software_root_is_a_useful_error(tmp_path: Path):
    missing = tmp_path / "missing"

    with pytest.raises(InventoryGenerationError, match="does not exist"):
        scan_local_tree(missing)


def test_output_is_deterministic_and_unchanged_inventory_is_not_rewritten(tmp_path: Path):
    records = [
        ("/z/easybuild/Zlib-2.eb", b"name = 'Zlib'\n"),
        ("/a/easybuild/alpha-1.eb", b"name = 'alpha'\n"),
        ("/b/easybuild/Zlib-1.eb", b"name = 'zlib'\n"),
    ]
    first = build_inventory(records, "romeo", "r2026", "2026-08-19T18:00:00+02:00")
    second = build_inventory(reversed(records), "romeo", "r2026", "2026-08-20T18:00:00+02:00")
    output = tmp_path / "romeo" / "r2026.json"

    assert first["software"] == second["software"]
    assert write_inventory(first, output) is True
    original_bytes = output.read_bytes()
    original_mtime = output.stat().st_mtime_ns
    assert write_inventory(second, output) is False
    assert output.read_bytes() == original_bytes
    assert output.stat().st_mtime_ns == original_mtime


def test_only_files_directly_inside_an_easybuild_directory_are_collected(tmp_path: Path):
    root = tmp_path / "software"
    _installed_easyconfig(root, "package", "included.eb", "included")
    nested = root / "package" / "easybuild" / "nested" / "excluded.eb"
    nested.parent.mkdir()
    nested.write_text("name = 'excluded'\n", encoding="utf-8")
    other = root / "package" / "not-easybuild" / "excluded-too.eb"
    other.parent.mkdir()
    other.write_text("name = 'excluded-too'\n", encoding="utf-8")

    assert [Path(path).name for path, _ in scan_local_tree(root)] == ["included.eb"]


def test_remote_scan_uses_system_ssh_and_returns_easyconfig_contents():
    calls = []
    encoded = base64.b64encode(b"name = 'GROMACS'\n").decode("ascii")

    def runner(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps([["/software/GROMACS/easybuild/GROMACS-1.eb", encoded]]).encode(),
            stderr=b"",
        )

    records = scan_remote_tree("romeo", "/software/rome r2026", runner=runner)

    assert records == [("/software/GROMACS/easybuild/GROMACS-1.eb", b"name = 'GROMACS'\n")]
    assert calls[0][0][:3] == ["ssh", "--", "romeo"]
    assert "python3 -c" in calls[0][0][3]
    assert calls[0][1]["check"] is False


def test_remote_scan_reports_ssh_failure():
    def runner(command, **kwargs):
        return subprocess.CompletedProcess(command, 255, stdout=b"", stderr=b"ssh: host unavailable\n")

    with pytest.raises(InventoryGenerationError, match="romeo.*host unavailable"):
        scan_remote_tree("romeo", "/software/rome/r2026", runner=runner)
