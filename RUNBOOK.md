# Parquet reconciliation benchmark — runbook

Compares two approaches for validating a dbt-built table against an
Ab Initio parquet drop:

- **parquet_python** — the existing approach: a Snowpark proc downloads
  both parquet files and diffs them with pyarrow/pandas.
- **audit_helper** — the proposed approach: dbt's audit_helper package
  compares the dbt model directly against Ab Initio's data loaded into a
  Snowflake table, using set-based SQL.

Results are logged side by side to `benchmark_runs` for comparison.

## Prerequisites

- dbt platform access to the target project (no `profiles.yml` needed).
- Python 3 with `numpy`, `pandas`, `pyarrow`, `snowflake-connector-python`
  installed.
- Environment variables for the Python scripts (`scripts/upload_to_stage.py`,
  `scripts/download_dbt_export.py`):

  ```
  SNOWFLAKE_ACCOUNT
  SNOWFLAKE_USERNAME
  SNOWFLAKE_PASSWORD        (or SNOWFLAKE_AUTHENTICATOR=externalbrowser for SSO)
  SNOWFLAKE_ROLE
  SNOWFLAKE_WAREHOUSE
  SNOWFLAKE_DB
  SNOWFLAKE_SCHEMA
  ```

  `SNOWFLAKE_DB`/`SNOWFLAKE_SCHEMA` **must match** whatever database/schema
  your dbt platform target actually resolves to — the Python scripts and
  dbt need to see the same objects. If they don't match you'll hit
  `Stage ... does not exist or not authorized`.

## One-time setup

```
dbt deps
dbt seed
dbt run-operation create_compare_parquet_files
```

## Per data-size run

Pick a row count (default is 1,000,000 if omitted) and run this sequence
once per size you want to test:

```
dbt run --select bench_tbl_a_dbt bench_tbl_b_dbt --vars '{"benchmark_row_count": 5000000}'
dbt run-operation export_benchmark_dbt_tables
python scripts/download_dbt_export.py
python scripts/generate_abinitio_from_dbt_export.py
python scripts/upload_to_stage.py
dbt run-operation load_benchmark_source_tables
```

**Do not run `dbt run --select benchmark`** (the whole folder) after this —
it rebuilds `bench_parquet_summary`/`bench_parquet_details` as empty
tables, wiping any accumulated history in them. Only `bench_tbl_a_dbt`/
`bench_tbl_b_dbt` are safe to rebuild repeatedly (their data is
deterministic — see "Why HASH, not RANDOM" below).

The first time only, also build the parquet-approach output tables:

```
dbt run --select bench_parquet_summary bench_parquet_details
```

## Running the benchmark (repeat 3-5x per data size)

```
dbt run-operation run_benchmark_parquet_approach
dbt run-operation run_benchmark_audit_helper
```

Each call appends a row to `benchmark_runs` (a plain table created by a
macro, never touched by `dbt run`) — nothing needs resetting between
repeats. Both macros disable result caching (`USE_CACHED_RESULT = FALSE`)
so repeated runs measure real execution, not a cached answer.

## Analyzing results

```sql
SELECT approach, table_short_name, row_count_a,
       MEDIAN(elapsed_ms) AS median_elapsed_ms,
       COUNT(*) AS n_runs
FROM benchmark_runs
GROUP BY 1, 2, 3
ORDER BY 3, 1, 2;
```

Use median, not mean — warehouse cold-start variance skews averages on a
small number of trials.

## Verifying detection parity

`dummy_data/ground_truth.json` (written by
`generate_abinitio_from_dbt_export.py`) records the exact corruption
injected: value mismatches, dropped/extra rows, a decimal-precision
mismatch, and a column-order swap. Compare it against each approach's
reported `colseq_mismatch`/`coltype_mismatch`/`row_mismatch_count` in
`benchmark_runs` to confirm both approaches actually detect the same
things, not just measure speed differently.

Known, expected gap: `audit_helper`'s `coltype_mismatch` will be **0**
for the decimal-precision case. `compare_relation_columns` only compares
`INFORMATION_SCHEMA.DATA_TYPE`, which Snowflake reports as a generic
`NUMBER` regardless of precision/scale — it cannot distinguish
`decimal(18,4)` from `decimal(18,2)`. This is a real, permanent
limitation of the audit_helper approach, not a bug to fix.

## Why HASH(id, ...), not RANDOM(), in bench_tbl_a_dbt / bench_tbl_b_dbt

Snowflake's docs explicitly state `RANDOM(seed)` is **not** guaranteed
reproducible across separate query executions, even with a fixed seed.
The ground-truth tables must produce identical data on every rebuild,
since the Ab Initio file is generated once from an earlier export and
frozen — any later rebuild with different values silently invalidates the
whole comparison (every row appears mismatched). `HASH(id, 'column_name')`
is a pure, deterministic function of `id`, so it's safe to rebuild these
models any number of times.

## Troubleshooting log (issues hit and fixed during setup)

| Symptom | Cause | Fix |
|---|---|---|
| `Max file size (16777216) exceeded for unload single file mode` | `SINGLE=TRUE` still enforces the 16MB default `MAX_FILE_SIZE` | Added `MAX_FILE_SIZE = 5368709120` (5GB max) to the export macro |
| `syntax error ... unexpected '-'` on GET/PUT | Local path contains spaces (`Bankwest - Parquet Questions`) | Wrap the whole `file://` URI in single quotes |
| `Stage ... does not exist or not authorized` | `SNOWFLAKE_DB`/`SNOWFLAKE_SCHEMA` env vars didn't match the dbt platform target | Set them to the same database/schema dbt actually resolves to |
| `KeyError: 'balance'` / `ArrowTypeError` on id column | Snowflake folds unquoted SQL aliases to uppercase; `COPY INTO ... FROM <table>` (no SELECT) unloads with generic `_COL_N` names, no headers | Wrap export in `FROM (SELECT * FROM <table>)` + `HEADER = TRUE`; resolve column names case-insensitively in the Python script; coerce id values to match the real inferred pyarrow type (often `decimal128`, not `int64`) |
| `invalid value 'ARRAY_CONSTRUCT(...)' for property 'FILES'` | `INFER_SCHEMA`'s `FILES` parameter expects a literal list, not a function call | Use `FILES => ('file.parquet')` |
| `Remote file '.../benchmarkabinitio_tbl_a.parquet' was not found` | `FILES` concatenates directly onto the path with no separator | Ensure the folder path always ends in `/` before use |
| `TRY_CAST cannot be used with arguments of types NULL and DATE` | Passing bare `NULL` for the proc's `period` arg collapses `TRY_TO_DATE(...)`'s result type | Pass `TO_VARCHAR(CURRENT_DATE())` instead of `NULL` |
| `tried to use + operator on unsupported types number and undefined` | audit_helper's unquoted aliases (`count`, `in_a`, ...) are also uppercased by Snowflake | Added `row_to_lower_dict()` helper for case-insensitive row access |
| `row_mismatch_count` = 0 for `parquet_python` on every run | Seed's `PKeys` value (`id`) didn't case-match the real column name (`ID`); the proc's pkey-presence guard silently skips the entire row-value comparison with no error | Set `PKeys` in `seeds/benchmark_filelists.csv` to match real column casing (`ID`) |
| `row_mismatch_count` = 100% of rows | `bench_tbl_a_dbt`/`bench_tbl_b_dbt` used unseeded `RANDOM()` and got rebuilt (via `dbt run --select benchmark`) after the Ab Initio file was already frozen, so live and frozen data diverged completely | Switched to deterministic `HASH(id, ...)`; avoid rebuilding the `benchmark` folder wholesale |
