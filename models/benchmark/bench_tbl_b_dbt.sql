{{ config(materialized='table') }}

{# See bench_tbl_a_dbt.sql for the rationale — same pattern, second table pair. #}
SELECT
    (SEQ4() + 1)::BIGINT AS id,
    ('REF' || LPAD((SEQ4() + 1)::VARCHAR, 9, '0'))::VARCHAR AS ref_code,
    CAST(UNIFORM(0, 10000000, RANDOM()) / 100.0 AS NUMBER(18,2)) AS amount,
    DECODE(UNIFORM(0, 3, RANDOM()), 0, 'auto-generated', 1, 'manual-adj', 2, 'system-sync', '')::VARCHAR AS notes
FROM TABLE(GENERATOR(ROWCOUNT => {{ var('benchmark_row_count', 1000000) }}))
