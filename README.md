# Datashare Software Stack Updater

`datashare-stack-update` updates Datashare software-stack files from local `barnard-ci` easyconfigs.

The primary workflow is **ODS-first**:
- each `Software_Stack_<Cluster>.ods` workbook is processed
- **all sheets** are evaluated
- each sheet name (for example `r24.10`, `r25.06`, `r2026`) is treated as the release
- rows are updated with matching `.eb` filenames and status `Done`

CSV is still supported as a legacy fallback, but ODS drives the architecture.

## What The Tool Does

For each ODS workbook:
1. infer cluster from filename (`Software_Stack_Barnard.ods` -> `barnard`)
2. iterate all sheets/tables in the workbook
3. validate sheet name as release-like (`rNN.NN` or `rYYYY`)
4. detect header row and required columns dynamically
5. scan `barnard-ci/easyconfigs/<cluster>/<release>/*.eb`
6. match software names (normalized matching, alias support if configured)
7. write matching filename(s) into release/easyconfig column
8. write status as exactly `Done`
9. preserve workbook layout by editing ODS cells in place
10. create backup before write and emit JSON report

If a sheet name is invalid or an easyconfig directory is missing, that sheet is skipped and reported clearly.

## Expected `barnard-ci` Layout

```text
barnard-ci/
  easyconfigs/
    barnard/
      r2026/
        GROMACS-2024.4-foss-2024a.eb
      r25.06/
        Julia-1.11.6-linux-x86_64.eb
    capella/
      r24.10/
        ...
```

## Required ODS Columns

Each processed sheet must have a header row with these logical columns (names can vary):
- software column (`Software`, `Softwares`, ...)
- release/easyconfig column (`Release`, `EasyConfig`, ...)
- status column (`Status`)

Header row is detected dynamically per sheet.

## Install

### 1. Install from source (editable)

```bash
cd /path/to/Datashare
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install dev/test dependencies

```bash
python3 -m pip install -e .[dev]
```
### 3. Create your App password from TUD Nextcloud

### 4. Put your old ods list at ~/DSS_updater 


## Run

### Console entry point

```bash
datashare-stack-update --datashare-dir ~/Desktop/Datashare --repo ~/Desktop/barnard-ci
```

### Run from source directly

```bash
python3 scripts/update_datashare_software_stack.py --datashare-dir ~/Desktop/Datashare --repo ~/Desktop/barnard-ci
```

### Cluster-only run

```bash
datashare-stack-update --datashare-dir ~/Desktop/Datashare --repo ~/Desktop/barnard-ci --cluster barnard
```

### Dry-run

```bash
datashare-stack-update --datashare-dir ~/Desktop/Datashare --repo ~/Desktop/barnard-ci --dry-run
```

`--dry-run` computes matches and reporting but does not modify files.

## Upload Modes

Upload is explicit and separated by mode.

### Authenticated account WebDAV mode

```bash
datashare-stack-update \
  --datashare-dir ~/Desktop/Datashare \
  --repo ~/Desktop/barnard-ci \
  --authenticated-upload \
  --webdav-url "https://datashare.tu-dresden.de/remote.php/dav/files/chwu350f/Shared/Software-Stack%20for%20all%20Cluster" \
  --webdav-username  \
  --webdav-password 
```

You can also omit those CLI args and provide equivalent environment variables:
- `DATASHARE_WEBDAV_URL`
- `DATASHARE_WEBDAV_USERNAME`
- `DATASHARE_WEBDAV_PASSWORD`


## Backups And Reports

- before writing an updated file: `<file>.bak.<timestamp>` is created
- JSON report is written after each run (default in Datashare dir)
- use `--report-out /path/report.json` to choose output path

Report contains:
- `sheets`: sheet-level processing summary (`file`, `sheet_name`, `release`, counts, skipped_reason)
- `rows`: row-level actions and reasons

## Tests

```bash
python3 -m pytest -q
```
