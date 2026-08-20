{{ config(materialized='table') }}

{#
  Synthetic "dbt-built" ground-truth table — generated natively in
  Snowflake (GENERATOR + SEQ4/UNIFORM), mirroring what a real dbt model
  actually is: a table, with no file I/O involved in its creation.
  Exported to parquet by export_benchmark_dbt_tables(), then read back by
  scripts/generate_abinitio_from_dbt_export.py to produce the corrupted
  Ab Initio sibling using dbt's real exported schema/types, not a guess.
#}
SELECT
    (SEQ4() + 1)::BIGINT AS id,
    DECODE(UNIFORM(0, 3, RANDOM()), 0, 'SAV', 1, 'CHK', 2, 'LOAN', 'TERM')::VARCHAR AS acct_type,
    CAST(UNIFORM(-500000, 5000000, RANDOM()) / 100.0 AS NUMBER(18,4)) AS balance,
    UNIFORM(0, 5474, RANDOM())::INT AS open_days,
    ('acct-desc-' || LPAD((SEQ4() + 1)::VARCHAR, 8, '0'))::VARCHAR AS description,
    DECODE(UNIFORM(0, 2, RANDOM()), 0, 'A', 1, 'I', 'C')::VARCHAR AS status_flag
FROM TABLE(GENERATOR(ROWCOUNT => {{ var('benchmark_row_count', 1000000) }}))
