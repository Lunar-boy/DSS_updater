"""Index and match barnard-ci EasyBuild easyconfigs."""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable, Sequence
from pathlib import Path


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def normalize_name(value: str) -> str:
    return normalize_text(value).casefold()


def parse_name_from_easyconfig_content(content: str) -> str | None:
    match = re.search(r"^\s*name\s*=\s*(['\"])(.+?)\1\s*$", content, re.MULTILINE)
    if not match:
        return None
    return match.group(2).strip() or None


def decode_easyconfig_content(content: bytes) -> str:
    """Decode an EasyConfig using the encodings supported by the repository index."""
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError:
        return content.decode("latin-1")


def fallback_name_from_filename(filename: str) -> str:
    stem = Path(filename).stem
    parts = stem.split("-")
    if len(parts) == 1:
        return stem
    for idx, token in enumerate(parts):
        if re.match(r"^v?\d", token):
            return stem if idx == 0 else "-".join(parts[:idx])
    return parts[0]


def read_easyconfig_index(repo_root: Path, cluster: str, release: str) -> dict[str, list[str]]:
    target_dir = repo_root / "easyconfigs" / cluster / release
    if not target_dir.is_dir():
        return {}

    index: dict[str, set[str]] = defaultdict(set)
    for eb_path in sorted(target_dir.glob("*.eb")):
        content = decode_easyconfig_content(eb_path.read_bytes())
        name = parse_name_from_easyconfig_content(content) or fallback_name_from_filename(eb_path.name)
        index[normalize_name(name)].add(eb_path.name)
    return {name: sorted(filenames) for name, filenames in index.items()}


def source_clusters(cluster: str) -> tuple[str, ...]:
    return ("alpha", "romeo") if cluster == "alpha" else (cluster,)


def source_dirs(repo_root: Path, cluster: str, release: str) -> list[Path]:
    return [repo_root / "easyconfigs" / source / release for source in source_clusters(cluster)]


def read_merged_easyconfig_index(repo_root: Path, cluster: str, release: str) -> dict[str, list[str]]:
    merged: dict[str, set[str]] = defaultdict(set)
    for source in source_clusters(cluster):
        for name, filenames in read_easyconfig_index(repo_root, source, release).items():
            merged[name].update(filenames)
    return {name: sorted(filenames) for name, filenames in merged.items()}


def merge_filenames(existing_value: str, new_filenames: Sequence[str]) -> str:
    existing = [normalize_text(part) for part in (existing_value or "").split(";") if normalize_text(part)]
    return "; ".join(sorted(set(existing) | set(new_filenames)))


def fuzzy_candidates(software_name: str, available_names: Iterable[str]) -> list[str]:
    normalized = normalize_name(software_name)
    return [candidate for candidate in available_names if normalized in candidate or candidate in normalized]
