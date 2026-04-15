# Developer Note

## Files created/modified

- `scripts/update_datashare_software_stack.py`
- `tests/test_update_datashare_software_stack.py`
- `README.md`
- `DEVELOPER_NOTE.md`

## Assumptions made

- Datashare sheet structure includes one title row and one header row (header row auto-detected).
- The release target is parseable from the release column header when present (fallback mapping used otherwise).
- Software matching is normalized exact matching first (case-insensitive, collapsed whitespace), with optional alias support.
- If ODS files are present, `odfpy` is available for cell-level updates; otherwise CSV fallback is supported.

## Unresolved edge cases

- Some ODS files may rely heavily on repeated-cell encodings and complex formatting conventions; script handles repeated cells for target columns but should be validated on each real ODS template.
- If a sheet contains multiple data tables in one document, the script currently updates the first table.
- Fuzzy matching is intentionally conservative (no automatic updates for ambiguous candidates).
