{{
  config(
    materialized = 'incremental',
    unique_key = ['RunNo', 'FileNo'],
    on_schema_change = 'ignore',
    pre_hook = [
      "{% if flags.FULL_REFRESH %}TRUNCATE TABLE IF EXISTS {{ ref('parquet_run_details__collections') }}{% endif %}"
    ],
    post_hook = ["{{ run_parquet_reconciliation() }}"]
  )
}}

{# Schema-only model. Body emits zero rows; every row is inserted by the
   post-hook orchestrator via run_parquet_reconciliation, which calls the
   COMPARE_PARQUET_FILES stored procedure once per active file in the seed
   parquet_filelists. #}
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
