## What it processes

Only regular files named `Software_Stack_*.ods` in the selected directory are discovered.
Unrelated `.ods` files and all other file types are ignored.

For each discovered workbook, the reconciler:

1. infers the cluster from the filename;
2. processes every release-named sheet (`rNN.NN` or `rYYYY`);
3. dynamically detects the software, release/easyconfig, and status columns;
4. independently indexes `barnard-ci/easyconfigs/<cluster>/<release>/*.eb` and the matching
   installation inventory;
5. merges both sources by normalized software name and matches configured aliases;
6. merges reconciled `.eb` filenames (including multiple versions) with `; ` and sets the
   status to `Done` only when the software is installed;
7. creates a timestamped backup and atomically replaces a changed ODS file;
8. writes a JSON report.

Alpha sheets use the union of Alpha and Romeo easyconfigs for the same release. Other clusters
use only their own easyconfig directory. Matches are idempotent: rerunning against unchanged
inputs does not rewrite a workbook or add duplicate filenames.

When inventory reconciliation is enabled, a missing cluster/release inventory file is an empty
inventory for that release; repository entries alone are not marked `Done`. Sheets with invalid
release names or missing required columns are skipped and recorded in the report. In legacy mode
(no `--inventory-dir`), missing EasyConfig directories retain their former skip behavior.

## Inventory JSON

Generate an inventory on a cluster when `dss-inventory` is installed there:

```bash
dss-inventory \
  --cluster romeo \
  --release r2026 \
  --software-root /software/rome/r2026 \
  --output romeo/r2026.json
```

The default local mode recursively scans
`<software-root>/**/easybuild/*.eb`. It uses each EasyConfig's `name` assignment, groups names
with the same normalization used by reconciliation, and sorts and deduplicates all filenames.
Multiple installed versions and toolchains are retained. Missing or unreadable roots and
EasyConfigs without a recognizable name are reported as errors. The installation tree is only
read; output parent directories are created as needed.

Alternatively, collect through one of the SSH aliases already configured in `~/.ssh/config`:

```bash
dss-inventory \
  --ssh-host romeo \
  --cluster romeo \
  --release r2026 \
  --software-root /software/rome/r2026 \
  --output ~/dss_updater/inventory/romeo/r2026.json
```

Remote mode invokes the system `ssh` command and a remote `python3`; it adds no SSH library
dependency. The remote filesystem is scanned read-only and the resulting JSON is written on the
machine where `dss-inventory` was invoked. Aliases such as `barnard`, `capella`, `romeo`,
`alpha`, and `julia` work through their existing SSH configuration.

Generation is deterministic: software and filenames have stable ordering. If the cluster,
release, and collected software are unchanged, an existing canonical inventory is left byte-for-
byte untouched and keeps its original `generated_at` timestamp.

Place one UTF-8 JSON file at `<inventory-dir>/<cluster>/<release>.json`, for example
`/srv/dss-inventory/barnard/r2026.json`. The canonical format is:

```json
{
  "cluster": "barnard",
  "release": "r2026",
  "generated_at": "2026-08-19T12:00:00+02:00",
  "software": [
    {
      "name": "GROMACS",
      "easyconfigs": [
        "GROMACS-2024.4-foss-2024a.eb",
        "GROMACS-2025.1-foss-2025a.eb"
      ]
    }
  ]
}
```

For simple generators, a top-level name-to-filename-list object is also accepted. Missing files
are allowed and mean that nothing was observed as installed for that cluster and release.

## Reconciliation policy

Each normalized software name has an `in_repo` flag, an `installed` flag, and the union of its
EasyConfig filenames:

| Repository | Inventory | ODS result |
|---|---|---|
| present | present | Write the EasyConfig(s); status `Done`; report source `both` |
| absent | present | Write the installed EasyConfig(s); status `Done`; report source `installed` |
| present | absent | Leave the ODS row unchanged; report source `repo` and not installed |
| absent | absent | Keep the row unmatched and include fuzzy-match diagnostics |

If inventory input is omitted entirely, the CLI preserves its pre-inventory behavior and treats
the repository index as installed. Supplying `--inventory-dir` opts into the policy above.

## Local file safety

The CLI holds a non-blocking, per-directory `fcntl.flock` for the entire run. A second
`dss-update` process targeting the same directory exits with an error instead of running
concurrently. A stale lock file is harmless because ownership is determined by the operating
system lock, not by the file's existence.

Before processing, the tool aborts if it finds:

- a name containing `conflicted copy` (case-insensitive);
- duplicate or non-canonical `Software_Stack_*.ods` variants for a known cluster; or
- a relevant LibreOffice `.~lock.*#` file for a target workbook.

Conflicts must be resolved manually; DSS_updater never attempts to merge them.

Each source workbook is fingerprinted using its size, nanosecond modification time, SHA256,
device, and inode before it is read. A changed workbook follows this write sequence:

```text
save to same-directory temporary ODS
  -> validate ZIP integrity and reopen with odfpy
  -> re-check conflicts, workbook variants, and LibreOffice locks
  -> confirm the original fingerprint is unchanged
  -> back up the original to ~/dss_updater/bak/
  -> confirm the fingerprint again
  -> atomic os.replace()
  -> reopen and validate the installed ODS
```

If staging validation or a fingerprint comparison fails, the generated temporary file is
discarded and the original is not overwritten.


## Backups and reports

Before a changed workbook is replaced, the original is copied to
`~/dss_updater/bak/<workbook>.bak.<timestamp>`; for example,
`~/dss_updater/bak/Software_Stack_Barnard.ods.bak.20260812_212000_123456`. The backup directory
is created automatically when a workbook is going to be replaced. No backup is created for an
unchanged workbook or during a dry run. ODS output is still written to a temporary file beside
the source workbook and atomically renamed into place so the sync client does not observe a
partially written ODS.

By default, JSON reports are created as
`~/dss_updater/reports/dss_update_report_YYYYMMDD_HHMMSS.json`; the report directory is
created automatically. Use `--report-out` to override the report path.

The JSON report contains sheet-level counts and row-level match actions and reasons. Every exact
or alias match includes `source` with `repo`, `installed`, or `both`. Dry runs report the rows and
files that would be updated, but never write or back up a workbook.

## Package architecture

```text
src/dss_updater/
  cli.py             argument parsing and run orchestration
  inventory_cli.py   inventory-generation command-line interface
  inventory_generation.py local/SSH scanning and canonical JSON generation
  models.py          shared result models
  easyconfigs.py     EasyConfig indexing, normalization, and matching
  inventory.py       installed-software JSON inventory parsing and indexing
  ods.py             ODS loading, cell updates, and atomic saving
  reconciliation.py domain reconciliation and Alpha/Romeo rules
  reporting.py       JSON report serialization
  safety.py          discovery, conflict detection, backups, atomic writes
```

## Tests

```bash
python3 -m pytest -q
```
