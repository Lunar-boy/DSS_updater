"""Command-line interface for the local-only ODS reconciler."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from .easyconfigs import normalize_name
from .models import RowReport, SheetStats
from .reconciliation import DEFAULT_ALIAS_MAP, SUPPORTED_CLUSTERS, infer_cluster_from_filename, process_ods_file
from .reporting import serialize_report
from .safety import (
    SafetyError,
    discover_ods_files,
    ensure_safe_directory_state,
    process_lock,
)

DEFAULT_DATASHARE_DIR = "~/Nextcloud/Shared/Software-Stack for all Cluster"
DEFAULT_REPO_DIR = "~/Desktop/barnard-ci"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reconcile local Software_Stack_*.ods files from barnard-ci easyconfigs."
    )
    parser.add_argument("--datashare-dir", default=DEFAULT_DATASHARE_DIR, help="Local Nextcloud-synchronized directory")
    parser.add_argument("--repo", default=DEFAULT_REPO_DIR, help="Path to the barnard-ci repository")
    parser.add_argument("--cluster", choices=SUPPORTED_CLUSTERS, help="Process only files for one cluster")
    parser.add_argument("--dry-run", action="store_true", help="Compute changes without writing workbooks")
    parser.add_argument("--report-out", help="Output path for the JSON report")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")
    datashare_dir = Path(args.datashare_dir).expanduser().resolve()
    repo_root = Path(args.repo).expanduser().resolve()
    if not datashare_dir.is_dir():
        raise SystemExit(f"Datashare directory not found: {datashare_dir}")
    if not repo_root.is_dir():
        raise SystemExit(f"Repository directory not found: {repo_root}")
    try:
        with process_lock(datashare_dir):
            return _run_locked(args, datashare_dir, repo_root)
    except SafetyError as exc:
        raise SystemExit(f"Safety check failed: {exc}") from exc


def _run_locked(args: argparse.Namespace, datashare_dir: Path, repo_root: Path) -> int:
    ods_files = discover_ods_files(datashare_dir)
    target_files = [
        file_path
        for file_path in ods_files
        if not args.cluster or infer_cluster_from_filename(file_path) == args.cluster
    ]
    ensure_safe_directory_state(datashare_dir, target_files, SUPPORTED_CLUSTERS)

    alias_map = {normalize_name(source): normalize_name(target) for source, target in DEFAULT_ALIAS_MAP.items()}
    all_stats: list[SheetStats] = []
    all_reports: list[RowReport] = []
    updated_files: list[Path] = []
    for file_path in ods_files:
        cluster = infer_cluster_from_filename(file_path)
        if not cluster:
            all_stats.append(SheetStats("", str(file_path), "", "", skipped_reason="cluster_not_inferable_from_filename"))
            all_reports.append(RowReport("", str(file_path), "", "", "", [], "skipped", "cluster_not_inferable_from_filename"))
            continue
        if args.cluster and cluster != args.cluster:
            continue
        logging.info("Processing local ODS file=%s cluster=%s", file_path, cluster)
        stats, reports, changed = process_ods_file(
            file_path=file_path,
            cluster=cluster,
            repo_root=repo_root,
            dry_run=args.dry_run,
            alias_map=alias_map,
            pre_replace_check=lambda target=file_path: ensure_safe_directory_state(
                datashare_dir, [target], SUPPORTED_CLUSTERS
            ),
        )
        all_stats.extend(stats)
        all_reports.extend(reports)
        if changed:
            updated_files.append(file_path)

    report_out = (
        Path(args.report_out).expanduser().resolve()
        if args.report_out
        else datashare_dir / f"dss_update_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    serialize_report(all_stats, all_reports, report_out)
    print("=== DSS Local ODS Reconciliation Summary ===")
    print(f"Sheets processed: {len(all_stats)}")
    print(f"Rows scanned: {sum(stat.rows_scanned for stat in all_stats)}")
    print(f"Rows updated: {sum(stat.updated_rows for stat in all_stats)}")
    print(f"Files updated: {len(updated_files)}")
    print(f"Report written: {report_out}")
    for stat in all_stats:
        print(
            f" - cluster={stat.cluster or 'n/a'}, file={stat.file_path}, sheet={stat.sheet_name or 'n/a'}, "
            f"release={stat.release or 'n/a'}, rows={stat.rows_scanned}, matched={stat.matched_rows}, "
            f"updated={stat.updated_rows}, skipped_reason={stat.skipped_reason or 'none'}"
        )
    return 0
