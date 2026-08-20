{{
  config(
    materialized = 'incremental',
    on_schema_change = 'ignore'
  )
}}

{# Schema-only model. Rows are inserted by the orchestrator invoked from
   parquet_run_summary's post-hook. #}
SELECT
    CAST(NULL AS INTEGER) AS RunNo,
    CAST(NULL AS INTEGER) AS FileNo,
    CAST(NULL AS VARCHAR) AS Type,
    CAST(NULL AS VARCHAR) AS Type_Ref,
    CAST(NULL AS VARCHAR) AS PKeys,
    CAST(NULL AS VARCHAR) AS FileAb_Val,
    CAST(NULL AS VARCHAR) AS FileDbt_Val
WHERE 1 = 0
