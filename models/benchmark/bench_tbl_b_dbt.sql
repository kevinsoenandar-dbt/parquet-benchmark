{{ config(materialized='table') }}

{# See bench_tbl_a_dbt.sql for the rationale (deterministic HASH(id, ...)
   instead of RANDOM(), and computing id once via a CTE) — same pattern,
   second table pair. #}
WITH base AS (
    SELECT (SEQ4() + 1)::BIGINT AS id
    FROM TABLE(GENERATOR(ROWCOUNT => {{ var('benchmark_row_count', 1000000) }}))
)
SELECT
    id,
    ('REF' || LPAD(id::VARCHAR, 9, '0'))::VARCHAR AS ref_code,
    CAST(MOD(ABS(HASH(id, 'amount')), 10000001) / 100.0 AS NUMBER(18,2)) AS amount,
    DECODE(MOD(ABS(HASH(id, 'notes')), 4), 0, 'auto-generated', 1, 'manual-adj', 2, 'system-sync', '')::VARCHAR AS notes
FROM base
