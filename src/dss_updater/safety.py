"""Filesystem discovery, locking, fingerprints, backups, and atomic writes."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import os
import re
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


ODS_PATTERN = "Software_Stack_*.ods"
CONFLICT_MARKER = "conflicted copy"
WORKBOOK_PREFIX = "software_stack_"
CANONICAL_WORKBOOK_PATTERN = re.compile(r"^Software_Stack_([A-Za-z]+)\.ods$")
DEFAULT_BACKUP_DIR = "~/dss_updater/bak"


class SafetyError(RuntimeError):
    """Base class for errors that must abort a local reconciliation run."""


class NextcloudConflictError(SafetyError):
    """Raised when a likely Nextcloud conflict is present."""


class AmbiguousWorkbookError(SafetyError):
    """Raised when more than one workbook may represent the same cluster."""


class LibreOfficeLockError(SafetyError):
    """Raised when LibreOffice appears to have a target workbook open."""


class ProcessLockError(SafetyError):
    """Raised when another DSS_updater process owns the run lock."""


class ConcurrentModificationError(SafetyError):
    """Raised when a target changes after it was initially read."""


@dataclass(frozen=True)
class FileFingerprint:
    size: int
    mtime_ns: int
    sha256: str
    device: int
    inode: int


def fingerprint_file(path: Path) -> FileFingerprint:
    """Fingerprint one stable path, rejecting changes that occur while hashing."""
    try:
        with path.open("rb") as handle:
            before = os.fstat(handle.fileno())
            digest = hashlib.sha256()
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
            after = os.fstat(handle.fileno())
        path_after = path.stat()
    except FileNotFoundError as exc:
        raise ConcurrentModificationError(f"Target workbook disappeared: {path}") from exc

    stable_fields_before = (before.st_size, before.st_mtime_ns, before.st_dev, before.st_ino)
    stable_fields_after = (after.st_size, after.st_mtime_ns, after.st_dev, after.st_ino)
    path_identity_after = (path_after.st_dev, path_after.st_ino)
    if stable_fields_before != stable_fields_after or path_identity_after != (after.st_dev, after.st_ino):
        raise ConcurrentModificationError(f"Target workbook changed while it was being fingerprinted: {path}")

    return FileFingerprint(
        size=after.st_size,
        mtime_ns=after.st_mtime_ns,
        sha256=digest.hexdigest(),
        device=after.st_dev,
        inode=after.st_ino,
    )


def ensure_fingerprint_unchanged(path: Path, expected: FileFingerprint) -> None:
    current = fingerprint_file(path)
    if current != expected:
        raise ConcurrentModificationError(
            f"Target workbook changed during reconciliation: {path}. "
            "The generated update was discarded and the original was not overwritten."
        )


def find_conflict_files(directory: Path) -> list[Path]:
    return sorted(path for path in directory.iterdir() if CONFLICT_MARKER in path.name.casefold())


def ensure_no_conflict_files(directory: Path) -> None:
    conflicts = find_conflict_files(directory)
    if conflicts:
        names = ", ".join(path.name for path in conflicts)
        raise NextcloudConflictError(
            f"Nextcloud conflict file(s) found in {directory}: {names}. "
            "Resolve them before running DSS_updater; no workbooks were modified."
        )


def _cluster_hint(path: Path, supported_clusters: Sequence[str]) -> str | None:
    tokens = [token for token in re.split(r"[^a-z0-9]+", path.stem.casefold()) if token]
    return next((token for token in tokens if token in supported_clusters), None)


def find_ambiguous_workbooks(directory: Path, supported_clusters: Sequence[str]) -> list[Path]:
    """Find duplicate cluster workbooks and non-canonical variants."""
    candidates = sorted(
        path
        for path in directory.iterdir()
        if path.is_file()
        and path.suffix.casefold() == ".ods"
        and path.name.casefold().startswith(WORKBOOK_PREFIX)
    )
    by_cluster: dict[str, list[Path]] = defaultdict(list)
    variants: list[Path] = []
    for path in candidates:
        cluster = _cluster_hint(path, supported_clusters)
        if cluster is None:
            continue
        by_cluster[cluster].append(path)
        match = CANONICAL_WORKBOOK_PATTERN.fullmatch(path.name)
        if match is None or match.group(1).casefold() != cluster:
            variants.append(path)

    ambiguous = set(variants)
    for paths in by_cluster.values():
        if len(paths) > 1:
            ambiguous.update(paths)
    return sorted(ambiguous)


def ensure_no_ambiguous_workbooks(directory: Path, supported_clusters: Sequence[str]) -> None:
    ambiguous = find_ambiguous_workbooks(directory, supported_clusters)
    if ambiguous:
        names = ", ".join(path.name for path in ambiguous)
        raise AmbiguousWorkbookError(
            f"Ambiguous or duplicate software-stack workbook(s) found in {directory}: {names}. "
            "Resolve the directory state manually; DSS_updater will not merge them."
        )


def find_libreoffice_locks(directory: Path, targets: Sequence[Path]) -> list[Path]:
    target_names = {target.name.casefold() for target in targets}
    locks = []
    for path in directory.iterdir():
        name = path.name
        if not (name.startswith(".~lock.") and name.endswith("#")):
            continue
        locked_name = name[len(".~lock.") : -1].casefold()
        if locked_name in target_names:
            locks.append(path)
    return sorted(locks)


def ensure_no_libreoffice_locks(directory: Path, targets: Sequence[Path]) -> None:
    locks = find_libreoffice_locks(directory, targets)
    if locks:
        names = ", ".join(path.name for path in locks)
        raise LibreOfficeLockError(
            f"LibreOffice lock file(s) found for target workbooks: {names}. "
            "Close the workbook in LibreOffice before running DSS_updater."
        )


def ensure_safe_directory_state(
    directory: Path,
    targets: Sequence[Path],
    supported_clusters: Sequence[str],
) -> None:
    """Run the reusable conflict, variant, and LibreOffice safety checks."""
    ensure_no_conflict_files(directory)
    ensure_no_ambiguous_workbooks(directory, supported_clusters)
    ensure_no_libreoffice_locks(directory, targets)


def process_lock_path(directory: Path) -> Path:
    directory_key = hashlib.sha256(str(directory.resolve()).encode("utf-8")).hexdigest()[:20]
    return Path(tempfile.gettempdir()) / f"dss_updater-{os.getuid()}-{directory_key}.lock"


@contextlib.contextmanager
def process_lock(directory: Path) -> Iterator[Path]:
    """Acquire a non-blocking, per-directory advisory process lock."""
    lock_path = process_lock_path(directory)
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(lock_path, flags, 0o600)
    with os.fdopen(descriptor, "r+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ProcessLockError(
                f"Another DSS_updater process is already running for {directory} (lock: {lock_path})."
            ) from exc
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()}\ndirectory={directory.resolve()}\n")
        handle.flush()
        os.fsync(handle.fileno())
        try:
            yield lock_path
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def discover_ods_files(directory: Path) -> list[Path]:
    return sorted(path for path in directory.glob(ODS_PATTERN) if path.is_file())


def backup_file(path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_dir = Path(DEFAULT_BACKUP_DIR).expanduser()
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{path.name}.bak.{timestamp}"
    shutil.copy2(path, backup_path)
    return backup_path


def fsync_directory(directory: Path) -> None:
    directory_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def atomic_write(path: Path, writer: Callable[[Path], None]) -> None:
    """Write beside *path*, flush it, then atomically replace the destination."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    try:
        writer(temporary_path)
        with temporary_path.open("rb") as handle:
            os.fsync(handle.fileno())
        if path.exists():
            shutil.copymode(path, temporary_path)
        os.replace(temporary_path, path)
        fsync_directory(path.parent)
    finally:
        temporary_path.unlink(missing_ok=True)
