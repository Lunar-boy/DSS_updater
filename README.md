# DSS_updater

`DSS_updater` provides an inventory generator and a local-only ODS reconciler. It scans the
actual EasyBuild installation tree (locally or through an existing SSH alias), writes a local
JSON inventory, combines that inventory with the EasyConfigs available in a local `barnard-ci`
checkout, and updates software-stack workbooks in a directory already synchronized by the
Nextcloud Desktop Client.

```text
cluster installation -> dss-inventory -> inventory JSON
                                             \
barnard-ci EasyConfig index -----------------> dss-update -> local ODS
                                                               -> Nextcloud Desktop Client
                                                               -> TU Dresden Datashare
```

`dss-update` modifies local files only. `dss-inventory` can read a cluster installation over
SSH, but always writes its JSON output locally. Neither command connects to or uploads anything
to TU Dresden Datashare. Cloud synchronization is exclusively the responsibility of the
Nextcloud Desktop Client. The tool does not invoke `nextcloudcmd` or any other sync client.

---

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

/srv/dss-inventory/
  barnard/
    r2026.json
  alpha/
    r2026.json
  romeo/
    r2026.json

~/Nextcloud/Shared/Software-Stack for all Cluster/
  Software_Stack_Alpha.ods
  Software_Stack_Barnard.ods
```
---

## Install

Python 3.10 or newer is required.

```bash
cd /home/nate/Desktop/DSS_updater
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -e '.[dev]'
```
---

## Run

The defaults are:

- Datashare directory: `~/Nextcloud/Shared/Software-Stack for all Cluster`
- barnard-ci repository: `~/Desktop/barnard-ci`
- Report directory: `~/dss_updater/reports/`
- Backup directory: `~/dss_updater/bak/`

Preview a run without changing ODS files:

```bash
dss-update --dry-run
```

Apply local changes:

```bash
dss-update
```

Run the complete inventory-to-ODS workflow:

```bash
dss-inventory \
  --ssh-host romeo \
  --cluster romeo \
  --release r2026 \
  --software-root /software/rome/r2026 \
  --output ~/dss_updater/inventory/romeo/r2026.json

dss-update \
  --cluster romeo \
  --inventory-dir ~/dss_updater/inventory
```

Select one cluster or override paths:

```bash
dss-update \
  --cluster barnard \
  --repo /path/to/barnard-ci \
  --inventory-dir /path/to/inventory \
  --datashare-dir "/path/to/local Nextcloud folder" \
  --report-out /path/to/report.json
```

Use `dss-update --help` for the complete local CLI. There are no upload options or cloud
credentials.
