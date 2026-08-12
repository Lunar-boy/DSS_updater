# DSS_updater

`DSS_updater` is a local-only ODS reconciler. It reads EasyBuild easyconfigs from a local
`barnard-ci` checkout and updates matching software-stack workbooks in a directory already
synchronized by the Nextcloud Desktop Client.

```text
barnard-ci
    ->
DSS_updater
    ->
local Nextcloud synchronized folder
    ->
Nextcloud Desktop Client
    ->
TU Dresden Datashare
```

`DSS_updater` modifies local files only. It does not connect to, authenticate with, or upload
anything to TU Dresden Datashare. Cloud synchronization is exclusively the responsibility of
the Nextcloud Desktop Client. The tool does not invoke `nextcloudcmd` or any other sync client.

## What it processes

Only regular files named `Software_Stack_*.ods` in the selected directory are discovered.
Unrelated `.ods` files and all other file types are ignored.

For each discovered workbook, the reconciler:

1. infers the cluster from the filename;
2. processes every release-named sheet (`rNN.NN` or `rYYYY`);
3. dynamically detects the software, release/easyconfig, and status columns;
4. indexes `barnard-ci/easyconfigs/<cluster>/<release>/*.eb`;
5. matches normalized software names, including configured aliases;
6. merges matching `.eb` filenames and sets the status to `Done`;
7. creates a timestamped backup and atomically replaces a changed ODS file;
8. writes a JSON report.

Alpha sheets use the union of Alpha and Romeo easyconfigs for the same release. Other clusters
use only their own easyconfig directory. Matches are idempotent: rerunning against unchanged
inputs does not rewrite a workbook or add duplicate filenames.

Sheets with invalid release names, missing required columns, or missing easyconfig directories
are skipped and recorded in the report.

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
  -> back up the original
  -> confirm the fingerprint again
  -> atomic os.replace()
  -> reopen and validate the installed ODS
```

If staging validation or a fingerprint comparison fails, the generated temporary file is
discarded and the original is not overwritten.

## Expected layout

```text
~/Desktop/barnard-ci/
  easyconfigs/
    alpha/
      r2026/
        GROMACS-2024.4-foss-2024a.eb
    romeo/
      r2026/
        GROMACS-2024.5-foss-2024b.eb

~/Nextcloud/Shared/Software-Stack for all Cluster/
  Software_Stack_Alpha.ods
  Software_Stack_Barnard.ods
```

## Install

Python 3.10 or newer is required.

```bash
cd /home/nate/Desktop/DSS_updater
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -e '.[dev]'
```

## Run

The defaults are:

- Datashare directory: `~/Nextcloud/Shared/Software-Stack for all Cluster`
- barnard-ci repository: `~/Desktop/barnard-ci`

Preview a run without changing ODS files:

```bash
dss-update --dry-run
```

Apply local changes:

```bash
dss-update
```

Select one cluster or override paths:

```bash
dss-update \
  --cluster barnard \
  --repo /path/to/barnard-ci \
  --datashare-dir "/path/to/local Nextcloud folder" \
  --report-out /path/to/report.json
```

Use `dss-update --help` for the complete local CLI. There are no upload options or cloud
credentials.

## Backups and reports

Before a changed workbook is replaced, the original is copied to
`<workbook>.bak.<timestamp>`. ODS output is written to a temporary file in the same directory
and atomically renamed into place so the sync client does not observe a partially written ODS.

The JSON report contains sheet-level counts and row-level match actions and reasons. Dry runs
still write a report but never write or back up a workbook.

## Package architecture

```text
src/dss_updater/
  cli.py             argument parsing and run orchestration
  models.py          shared result models
  easyconfigs.py     EasyConfig indexing, normalization, and matching
  ods.py             ODS loading, cell updates, and atomic saving
  reconciliation.py domain reconciliation and Alpha/Romeo rules
  reporting.py       JSON report serialization
  safety.py          discovery, conflict detection, backups, atomic writes
```

## Tests

```bash
python3 -m pytest -q
```
