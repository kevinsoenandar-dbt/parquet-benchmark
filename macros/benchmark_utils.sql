{% macro create_benchmark_runs_table() %}
{#
  One-off utility table for the benchmark harness (not a customer-facing
  model — created imperatively so both approaches can log to it via
  run-operation without needing a dbt build cycle).
#}
  {% set sql %}
    CREATE TABLE IF NOT EXISTS {{ target.database }}.{{ target.schema }}.benchmark_runs (
        run_id INTEGER AUTOINCREMENT,
        approach VARCHAR,
        table_short_name VARCHAR,
        row_count_a INTEGER,
        row_count_b INTEGER,
        colseq_mismatch INTEGER,
        coltype_mismatch INTEGER,
        row_mismatch_count INTEGER,
        elapsed_ms INTEGER,
        query_tag VARCHAR,
        load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
  {% endset %}
  {% do run_query(sql) %}
{% endmacro %}


{% macro log_benchmark_run(approach, table_short_name, row_count_a, row_count_b,
                            colseq_mismatch, coltype_mismatch, row_mismatch_count,
                            elapsed_ms, query_tag) %}
  {% set sql %}
    INSERT INTO {{ target.database }}.{{ target.schema }}.benchmark_runs
    (approach, table_short_name, row_count_a, row_count_b, colseq_mismatch,
     coltype_mismatch, row_mismatch_count, elapsed_ms, query_tag)
    VALUES (
        '{{ approach }}', '{{ table_short_name }}',
        {{ row_count_a }}, {{ row_count_b }},
        {{ colseq_mismatch }}, {{ coltype_mismatch }}, {{ row_mismatch_count }},
        {{ elapsed_ms }}, '{{ query_tag }}'
    )
  {% endset %}
  {% do run_query(sql) %}
{% endmacro %}


{% macro resolve_benchmark_folder(folder_tmpl) %}
{# Resolves {database} and {schema} tokens used in seeds/benchmark_filelists.csv #}
  {{ return(folder_tmpl | replace('{database}', target.database) | replace('{schema}', target.schema)) }}
{% endmacro %}


{% macro elapsed_ms_since(start_ts) %}
{# start_ts must be an epoch-millisecond NUMBER captured via current_epoch_ms() #}
  {% set result = run_query("SELECT " ~ current_epoch_ms_sql() ~ " - " ~ start_ts) %}
  {{ return(result.rows[0][0] | int) }}
{% endmacro %}


{% macro current_epoch_ms_sql() %}
  {{ return("DATE_PART('epoch_millisecond', CURRENT_TIMESTAMP())") }}
{% endmacro %}


{% macro current_epoch_ms() %}
  {% set result = run_query("SELECT " ~ current_epoch_ms_sql()) %}
  {{ return(result.rows[0][0] | int) }}
{% endmacro %}
