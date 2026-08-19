"""Read per-cluster/per-release EasyBuild installation inventories."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from .easyconfigs import fallback_name_from_filename, normalize_name


class InventoryFormatError(ValueError):
    """Raised when an inventory file does not have the documented structure."""


def inventory_path(inventory_dir: Path, cluster: str, release: str) -> Path:
    return inventory_dir / cluster / f"{release}.json"


def _filenames(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    raise InventoryFormatError("easyconfigs must be a string or a list of strings")


def _entries(payload: Any) -> Iterable[tuple[str, list[str]]]:
    if isinstance(payload, dict) and "software" in payload:
        payload = payload["software"]
    elif isinstance(payload, dict) and "entries" in payload:
        payload = payload["entries"]

    if isinstance(payload, Mapping):
        ignored_metadata = {"cluster", "release", "generated_at"}
        for name, value in payload.items():
            if name in ignored_metadata:
                continue
            yield str(name), _filenames(value)
        return

    if not isinstance(payload, list):
        raise InventoryFormatError("inventory must contain a 'software' list or a name-to-easyconfigs object")
    for item in payload:
        if isinstance(item, str):
            yield fallback_name_from_filename(item), [item]
            continue
        if not isinstance(item, dict):
            raise InventoryFormatError("inventory entries must be objects or EasyConfig filenames")
        name = item.get("name") or item.get("software")
        filenames = item.get("easyconfigs", item.get("easyconfig"))
        if not isinstance(name, str) or not name.strip() or filenames is None:
            raise InventoryFormatError("each inventory entry needs 'name' and 'easyconfigs'")
        yield name, _filenames(filenames)


def read_inventory_index(inventory_dir: Path, cluster: str, release: str) -> dict[str, list[str]]:
    """Return an empty index when the cluster/release inventory file is absent."""
    path = inventory_path(inventory_dir, cluster, release)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        index: dict[str, set[str]] = defaultdict(set)
        for name, filenames in _entries(payload):
            index[normalize_name(name)].update(filename.strip() for filename in filenames if filename.strip())
    except (json.JSONDecodeError, InventoryFormatError) as exc:
        raise InventoryFormatError(f"Invalid inventory file {path}: {exc}") from exc
    return {name: sorted(filenames) for name, filenames in index.items()}


def read_merged_inventory_index(
    inventory_dir: Path, source_clusters: Iterable[str], release: str
) -> dict[str, list[str]]:
    merged: dict[str, set[str]] = defaultdict(set)
    for cluster in source_clusters:
        for name, filenames in read_inventory_index(inventory_dir, cluster, release).items():
            merged[name].update(filenames)
    return {name: sorted(filenames) for name, filenames in merged.items()}
