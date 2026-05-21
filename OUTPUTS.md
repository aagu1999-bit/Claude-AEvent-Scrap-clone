# Output file convention

Every script that writes a generated file to `outputs/` also writes a
sidecar `.md` next to it with the same basename. The CSV/XLSX/etc. stays
clean for downstream tools (pandas, Excel, sheet uploaders); the sidecar
carries the human context.

## Naming

```
outputs/<basename>.csv  →  outputs/<basename>.md
outputs/<basename>.xlsx →  outputs/<basename>.md
outputs/<basename>.json →  outputs/<basename>.md
```

Same basename, `.md` extension. Co-located in `ls`.

## What the sidecar must include

At minimum:

1. **Generated:** ISO timestamp + which script wrote it
2. **Mode:** dry-run / applied / report / whatever applies
3. **Command:** the exact `python3 ...` invocation that produced it (reproducible)
4. **What's in this file:** one-paragraph description of contents + row count
5. **Columns:** what each column means (especially non-obvious ones like `_source_file`)
6. **Snapshot of state at generation time:** sheet row counts, archive
   counts, key tab sizes — whatever upstream state would matter when
   interpreting the file later
7. **What to do with this file:** concrete next steps for the user
8. **Caveats:** known issues, when to be skeptical of the data, what could
   make it stale

## Why this exists

Output files outlive the conversation that produced them. A bare CSV in
`outputs/` doesn't tell you what flags it ran with, what the sheet looked
like at the time, or whether it's safe to feed back into something else.
Six months later, "what was this file?" requires git archaeology and
guesswork.

The sidecar makes the file self-explanatory months later, and is cheap
enough to write that every output should have one.

## File types and their tools

### `recovery_pending_<ts>.csv` / `recovery_applied_<ts>.csv`
Written by [recover_from_csv_xlsx.py](recover_from_csv_xlsx.py).
Contains events present in the local outputs/Events_*.{csv,xlsx} archive
but missing from the live All_Events sheet. Pending = dry-run preview;
applied = post-write audit trail.

### Other tools — TODO retrofit

These tools currently write CSVs WITHOUT sidecars. Retrofit after the
recovery workflow stabilizes:

- [purge_events_by_date.py](purge_events_by_date.py) →
  `outputs/purge_events_<ts>.csv` (no sidecar yet)
- [audit_log_vs_sheet.py](audit_log_vs_sheet.py) →
  `outputs/audit_log_vs_sheet_<ts>.csv` (no sidecar yet)
- [repair_flags_from_log.py](repair_flags_from_log.py) →
  `outputs/repair_flags_<ts>.csv` (no sidecar yet)

## For new tools

When you add a script that writes to `outputs/`:

1. Write a `write_sidecar_md(path, sidecar_data)` helper (or import a
   shared one once we extract it). Call it immediately after the primary
   output file is written.
2. Add a section to this OUTPUTS.md describing the new file type.
3. Mention the sidecar in the tool's module docstring under `OUTPUT`.
