{{ config(materialized='table') }}

{# Schema-only. Populated by run_benchmark_parquet_approach() via the
   COMPARE_PARQUET_FILES proc, mirroring parquet_run_summary__collections. #}
SELECT
    CAST(NULL AS INTEGER)         AS RunNo,
    CAST(NULL AS INTEGER)         AS FileNo,
    CAST(NULL AS DATE)            AS Period,
    CAST(NULL AS TIMESTAMP_NTZ)   AS Load_ts,
    CAST(NULL AS INTEGER)         AS RowCount_FileAb,
    CAST(NULL AS INTEGER)         AS RowCount_FileDbt,
    CAST(NULL AS INTEGER)         AS ColCount_FileAb,
    CAST(NULL AS INTEGER)         AS ColCount_FileDbt,
    CAST(NULL AS INTEGER)         AS ColSeq_Mismatched,
    CAST(NULL AS INTEGER)         AS ColType_Mismatched,
    CAST(NULL AS INTEGER)         AS ColVal_Mismatched,
    CAST(NULL AS VARCHAR)         AS Status
WHERE 1 = 0
