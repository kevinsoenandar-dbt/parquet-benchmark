{{ config(materialized='table') }}

{# Schema-only. Populated by run_benchmark_parquet_approach() via the
   COMPARE_PARQUET_FILES proc, mirroring parquet_run_details__collections. #}
SELECT
    CAST(NULL AS INTEGER) AS RunNo,
    CAST(NULL AS INTEGER) AS FileNo,
    CAST(NULL AS VARCHAR) AS Type,
    CAST(NULL AS VARCHAR) AS Type_Ref,
    CAST(NULL AS VARCHAR) AS PKeys,
    CAST(NULL AS VARCHAR) AS FileAb_Val,
    CAST(NULL AS VARCHAR) AS FileDbt_Val
WHERE 1 = 0
