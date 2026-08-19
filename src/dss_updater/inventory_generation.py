"""Collect canonical inventories from local or remote EasyBuild installations."""

from __future__ import annotations

import base64
import binascii
import json
import os
import shlex
import subprocess
from collections import defaultdict
from collections.abc import Callable, Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

from .easyconfigs import decode_easyconfig_content, normalize_name, parse_name_from_easyconfig_content


class InventoryGenerationError(RuntimeError):
    """Raised when an installation tree cannot be safely inventoried."""


EasyConfigRecord = tuple[str, bytes]


def scan_local_tree(software_root: Path) -> list[EasyConfigRecord]:
    """Read files matching ``<root>/**/easybuild/*.eb`` without changing the tree."""
    if not software_root.exists():
        raise InventoryGenerationError(f"Software root does not exist: {software_root}")
    if not software_root.is_dir():
        raise InventoryGenerationError(f"Software root is not a directory: {software_root}")
    if not os.access(software_root, os.R_OK | os.X_OK):
        raise InventoryGenerationError(f"Software root is not readable: {software_root}")

    records: list[EasyConfigRecord] = []

    def walk_error(error: OSError) -> None:
        raise error

    try:
        for directory, dirnames, filenames in os.walk(software_root, onerror=walk_error):
            dirnames.sort()
            if Path(directory).name != "easybuild":
                continue
            for filename in sorted(filenames):
                if not filename.endswith(".eb"):
                    continue
                path = Path(directory) / filename
                if path.is_file():
                    records.append((str(path), path.read_bytes()))
    except OSError as exc:
        target = exc.filename or software_root
        raise InventoryGenerationError(f"Cannot read EasyBuild installation tree at {target}: {exc}") from exc
    return records


_REMOTE_SCANNER = r"""
import base64
import json
import os
import sys

root = sys.argv[1]
try:
    if not os.path.exists(root):
        raise RuntimeError("software root does not exist: " + root)
    if not os.path.isdir(root):
        raise RuntimeError("software root is not a directory: " + root)
    if not os.access(root, os.R_OK | os.X_OK):
        raise RuntimeError("software root is not readable: " + root)
    records = []
    def walk_error(error):
        raise error
    for directory, dirnames, filenames in os.walk(root, onerror=walk_error):
        dirnames.sort()
        if os.path.basename(directory) != "easybuild":
            continue
        for filename in sorted(filenames):
            if filename.endswith(".eb"):
                path = os.path.join(directory, filename)
                if os.path.isfile(path):
                    with open(path, "rb") as handle:
                        records.append([path, base64.b64encode(handle.read()).decode("ascii")])
    print(json.dumps(records, separators=(",", ":")))
except Exception as error:
    print(str(error), file=sys.stderr)
    sys.exit(2)
""".strip()


def scan_remote_tree(
    ssh_host: str,
    software_root: str,
    *,
    runner: Callable[..., subprocess.CompletedProcess[bytes]] | None = None,
) -> list[EasyConfigRecord]:
    """Use the system SSH client to read matching remote EasyConfigs."""
    if not ssh_host or not software_root or "\0" in ssh_host or "\0" in software_root:
        raise InventoryGenerationError("SSH host and software root must be non-empty valid strings")
    remote_command = "python3 -c {} {}".format(
        shlex.quote(_REMOTE_SCANNER), shlex.quote(software_root)
    )
    run_command = runner or subprocess.run
    try:
        result = run_command(
            ["ssh", "--", ssh_host, remote_command],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as exc:
        raise InventoryGenerationError(f"Could not run system ssh for host {ssh_host}: {exc}") from exc
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip() or f"exit status {result.returncode}"
        raise InventoryGenerationError(f"Remote inventory scan failed on {ssh_host}: {detail}")
    try:
        payload = json.loads(result.stdout.decode("utf-8"))
        if not isinstance(payload, list):
            raise ValueError("scanner response is not a list")
        records: list[EasyConfigRecord] = []
        for item in payload:
            if not (
                isinstance(item, list)
                and len(item) == 2
                and all(isinstance(value, str) for value in item)
            ):
                raise ValueError("scanner response contains an invalid record")
            records.append((item[0], base64.b64decode(item[1], validate=True)))
        return records
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, binascii.Error) as exc:
        raise InventoryGenerationError(f"Invalid response from remote inventory scan on {ssh_host}: {exc}") from exc


def build_inventory(
    records: Iterable[EasyConfigRecord],
    cluster: str,
    release: str,
    generated_at: str,
) -> dict[str, Any]:
    """Parse, normalize, deduplicate, and sort collected EasyConfigs."""
    filenames: dict[str, set[str]] = defaultdict(set)
    display_names: dict[str, set[str]] = defaultdict(set)
    for source_path, raw_content in records:
        content = decode_easyconfig_content(raw_content)
        name = parse_name_from_easyconfig_content(content)
        if name is None:
            raise InventoryGenerationError(
                f"Malformed EasyConfig {source_path}: no recognizable non-empty name assignment"
            )
        normalized = normalize_name(name)
        filenames[normalized].add(Path(source_path).name)
        display_names[normalized].add(name)

    software = [
        {
            "name": sorted(display_names[name], key=lambda value: (value.casefold(), value))[0],
            "easyconfigs": sorted(filenames[name]),
        }
        for name in sorted(filenames)
    ]
    return {
        "cluster": cluster,
        "release": release,
        "generated_at": generated_at,
        "software": software,
    }


def write_inventory(payload: dict[str, Any], output: Path) -> bool:
    """Write canonical JSON, preserving an unchanged inventory's timestamp and bytes."""
    if output.exists() and not output.is_file():
        raise InventoryGenerationError(f"Inventory output is not a regular file: {output}")
    if output.is_file():
        try:
            existing = json.loads(output.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            existing = None
        canonical_keys = {"cluster", "release", "generated_at", "software"}
        if (
            isinstance(existing, dict)
            and set(existing) == canonical_keys
            and isinstance(existing.get("generated_at"), str)
            and bool(existing["generated_at"])
            and all(existing.get(key) == payload.get(key) for key in ("cluster", "release", "software"))
        ):
            return False

    try:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    except OSError as exc:
        raise InventoryGenerationError(f"Cannot write inventory {output}: {exc}") from exc
    return True


def current_timestamp() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")
