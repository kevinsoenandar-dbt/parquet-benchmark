{% macro run_benchmark_parquet_approach() %}
{#
  Benchmarks the EXISTING approach: the Snowpark COMPARE_PARQUET_FILES proc
  reading both parquet files directly from stage and diffing via
  pyarrow/pandas. Requires, in order:
    - dbt run-operation create_compare_parquet_files   (deploys the proc)
    - dbt run --select bench_tbl_a_dbt bench_tbl_b_dbt (builds dbt tables)
    - dbt run-operation export_benchmark_dbt_tables    (dbt file -> stage)
    - scripts/download_dbt_export.py                   (dbt file -> local)
    - scripts/generate_abinitio_from_dbt_export.py      (-> abinitio file)
    - scripts/upload_to_stage.py                        (abinitio -> stage)

  Writes one summary/detail row (existing shape) into bench_parquet_summary
  / bench_parquet_details per active benchmark_filelists row, and logs
  elapsed_ms + mismatch counts into benchmark_runs for direct comparison
  against run_benchmark_audit_helper.

  Run: dbt run-operation run_benchmark_parquet_approach
#}
  {% if not execute %}{{ return('') }}{% endif %}
  {% do create_benchmark_runs_table() %}

  {% set database = target.database %}
  {% set driver_rel = ref('benchmark_filelists') %}
  {% set summary_rel = ref('bench_parquet_summary') %}
  {% set details_rel = ref('bench_parquet_details') %}
  {% set summary_fqn = summary_rel.database ~ '.' ~ summary_rel.schema ~ '.' ~ summary_rel.identifier %}
  {% set details_fqn = details_rel.database ~ '.' ~ details_rel.schema ~ '.' ~ details_rel.identifier %}

  {% set next_run_query %}
    SELECT COALESCE(MAX(RunNo), 0) + 1 FROM {{ summary_rel }}
  {% endset %}
  {% set run_no = run_query(next_run_query).rows[0][0] %}

  {% set rows_query %}
    SELECT FileNo, TableShortName, Folder, FileNameAb, FileNameDbt, PKeys
    FROM {{ driver_rel }} WHERE UPPER(IsActive) = 'Y' ORDER BY FileNo
  {% endset %}
  {% set active_result = run_query(rows_query) %}

  {% for row in active_result.rows %}
    {% set file_no = row[0] %}
    {% set table_short = row[1] %}
    {% set folder = resolve_benchmark_folder(row[2]) %}
    {% set filename_ab = row[3] %}
    {% set filename_dbt = row[4] %}
    {% set pkeys_str = row[5] %}
    {% set file_ab = folder ~ '/' ~ filename_ab %}
    {% set file_dbt = folder ~ '/' ~ filename_dbt %}

    {% set pkeys_literals = [] %}
    {% for k in pkeys_str.split(';') %}
      {% do pkeys_literals.append("'" ~ k.strip() ~ "'") %}
    {% endfor %}
    {% set pkeys_sql = "ARRAY_CONSTRUCT(" ~ (pkeys_literals | join(', ')) ~ ")" %}

    {% set query_tag = 'bench_parquet_' ~ table_short ~ '_' ~ modules.datetime.datetime.now().strftime('%Y%m%d%H%M%S') %}
    {% do run_query("ALTER SESSION SET QUERY_TAG = '" ~ query_tag ~ "'") %}

    {% set start_ms = current_epoch_ms() %}

    {% set call_sql %}
      CALL {{ database }}.PRODUCT.COMPARE_PARQUET_FILES(
          '{{ file_ab }}',
          '{{ file_dbt }}',
          {{ pkeys_sql }},
          ARRAY_CONSTRUCT(),
          {{ run_no }},
          {{ file_no }},
          NULL,
          '{{ summary_fqn }}',
          '{{ details_fqn }}'
      )
    {% endset %}
    {% do run_query(call_sql) %}

    {% set elapsed = elapsed_ms_since(start_ms) %}

    {% set summary_query %}
      SELECT RowCount_FileAb, RowCount_FileDbt, ColSeq_Mismatched, ColType_Mismatched, ColVal_Mismatched
      FROM {{ summary_rel }}
      WHERE RunNo = {{ run_no }} AND FileNo = {{ file_no }}
    {% endset %}
    {% set r = run_query(summary_query).rows[0] %}

    {% do log_benchmark_run(
          approach='parquet_python',
          table_short_name=table_short,
          row_count_a=r[0], row_count_b=r[1],
          colseq_mismatch=r[2], coltype_mismatch=r[3], row_mismatch_count=r[4],
          elapsed_ms=elapsed, query_tag=query_tag
       ) %}

    {{ log("run_benchmark_parquet_approach: " ~ table_short ~ " elapsed_ms=" ~ elapsed, info=True) }}
  {% endfor %}
{% endmacro %}
