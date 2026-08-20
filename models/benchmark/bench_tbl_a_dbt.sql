{{ config(materialized='table') }}

{#
  Synthetic "dbt-built" ground-truth table — generated natively in
  Snowflake, mirroring what a real dbt model actually is: a table, with no
  file I/O involved in its creation. Exported to parquet by
  export_benchmark_dbt_tables(), then read back by
  scripts/generate_abinitio_from_dbt_export.py to produce the corrupted
  Ab Initio sibling using dbt's real exported schema/types, not a guess.

  Values are derived deterministically from `id` via HASH(), not RANDOM() —
  Snowflake's docs explicitly state RANDOM(seed) is NOT guaranteed
  reproducible across separate query executions even with a fixed seed.
  This table must produce identical data on every rebuild, since the
  Ab Initio file is generated once from an earlier export and frozen —
  any later rebuild with different values would silently invalidate the
  comparison (this bit us: a `dbt run --select benchmark` rebuilt this
  model with fresh RANDOM() values after the Ab Initio file was already
  generated, and every row appeared mismatched as a result).

  SEQ4() is called once in the base CTE and reused, since calling it
  multiple times in one SELECT does not yield the same value per row
  across calls.
#}
WITH base AS (
    SELECT (SEQ4() + 1)::BIGINT AS id
    FROM TABLE(GENERATOR(ROWCOUNT => {{ var('benchmark_row_count', 1000000) }}))
)
SELECT
    id,
    DECODE(MOD(ABS(HASH(id, 'acct_type')), 4), 0, 'SAV', 1, 'CHK', 2, 'LOAN', 'TERM')::VARCHAR AS acct_type,
    CAST((MOD(ABS(HASH(id, 'balance')), 5500001) - 500000) / 100.0 AS NUMBER(18,4)) AS balance,
    MOD(ABS(HASH(id, 'open_days')), 5475)::INT AS open_days,
    ('acct-desc-' || LPAD(id::VARCHAR, 8, '0'))::VARCHAR AS description,
    DECODE(MOD(ABS(HASH(id, 'status_flag')), 3), 0, 'A', 1, 'I', 'C')::VARCHAR AS status_flag
FROM base
