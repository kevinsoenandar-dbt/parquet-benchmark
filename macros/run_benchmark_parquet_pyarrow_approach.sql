{% macro run_benchmark_parquet_pyarrow_approach() %}
{#
  Benchmarks the PYARROW-NATIVE rewrite of the existing approach: the
  COMPARE_PARQUET_FILES_PYARROW proc (Table.join() + pyarrow.compute
  instead of pandas merge()+iterrows()). Identical to
  run_benchmark_parquet_approach() except it calls the separate
  _PYARROW proc and tags results with approach='parquet_pyarrow', so all
  three approaches (parquet_python, parquet_pyarrow, audit_helper) can be
  compared side by side in benchmark_runs without overwriting anything.

  Requires the same setup as run_benchmark_parquet_approach(), plus:
    - dbt run-operation create_compare_parquet_files_pyarrow (deploys this proc)

  A stored proc has no warehouse of its own — it runs on whatever
  warehouse is active in the session. Pass --vars '{"snowpark_warehouse":
  "<name>"}' to run the proc on a (typically Snowpark-optimized) warehouse
  instead of the default target warehouse, e.g. to test past a memory
  ceiling. Omit the var to run on the default warehouse as before.

  Run: dbt run-operation run_benchmark_parquet_pyarrow_approach
       dbt run-operation run_benchmark_parquet_pyarrow_approach --args '{}' --vars '{"snowpark_warehouse": "SNOWPARK_OPT_WH"}'
#}
  {% if not execute %}{{ return('') }}{% endif %}
  {% do create_benchmark_runs_table() %}
  {% set original_wh = switch_to_benchmark_warehouse() %}

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

    {% set query_tag = 'bench_parquet_pyarrow_' ~ table_short ~ '_' ~ modules.datetime.datetime.now().strftime('%Y%m%d%H%M%S') %}
    {% do run_query("ALTER SESSION SET QUERY_TAG = '" ~ query_tag ~ "'") %}
    {# Disable result caching so repeated runs measure real execution time,
       not a cached answer from an identical prior query. #}
    {% do run_query("ALTER SESSION SET USE_CACHED_RESULT = FALSE") %}

    {% set start_ms = current_epoch_ms() %}

    {% set call_sql %}
      CALL {{ database }}.{{ schema }}.COMPARE_PARQUET_FILES_PYARROW(
          '{{ file_ab }}',
          '{{ file_dbt }}',
          {{ pkeys_sql }},
          ARRAY_CONSTRUCT(),
          {{ run_no }},
          {{ file_no }},
          TO_VARCHAR(CURRENT_DATE()),
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
          approach='parquet_pyarrow',
          table_short_name=table_short,
          row_count_a=r[0], row_count_b=r[1],
          colseq_mismatch=r[2], coltype_mismatch=r[3], row_mismatch_count=r[4],
          elapsed_ms=elapsed, query_tag=query_tag
       ) %}

    {{ log("run_benchmark_parquet_pyarrow_approach: " ~ table_short ~ " elapsed_ms=" ~ elapsed, info=True) }}
  {% endfor %}

  {% do restore_warehouse(original_wh) %}
{% endmacro %}
