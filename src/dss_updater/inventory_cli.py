"""Command-line interface for EasyBuild installation inventory generation."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from .inventory_generation import (
    InventoryGenerationError,
    build_inventory,
    current_timestamp,
    scan_local_tree,
    scan_remote_tree,
    write_inventory,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a DSS inventory from an EasyBuild installation tree."
    )
    parser.add_argument("--cluster", required=True, help="Cluster name recorded in the inventory")
    parser.add_argument("--release", required=True, help="Release recorded in the inventory, for example r2026")
    parser.add_argument("--software-root", required=True, help="Local or remote EasyBuild software root")
    parser.add_argument("--output", required=True, help="Local path for the generated inventory JSON")
    parser.add_argument("--ssh-host", help="SSH alias used to scan the software root remotely")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        records = (
            scan_remote_tree(args.ssh_host, args.software_root)
            if args.ssh_host
            else scan_local_tree(Path(args.software_root).expanduser().resolve())
        )
        payload = build_inventory(records, args.cluster, args.release, current_timestamp())
        output = Path(args.output).expanduser().resolve()
        changed = write_inventory(payload, output)
    except InventoryGenerationError as exc:
        raise SystemExit(f"Inventory generation failed: {exc}") from exc

    action = "Written" if changed else "Unchanged"
    print(f"{action}: {output}")
    print(f"Software entries: {len(payload['software'])}")
    print(f"EasyConfigs: {sum(len(item['easyconfigs']) for item in payload['software'])}")
    return 0
