{% macro load_benchmark_source_tables() %}
{#
  Loads the dummy Ab Initio / dbt parquet pairs (pushed to stage by
  scripts/upload_to_stage.py) into native Snowflake tables, since
  audit_helper compares relations, not files.

  Each file gets its own CREATE TABLE ... USING TEMPLATE, built from
  INFER_SCHEMA scoped to that single file via the FILES parameter — the
  Ab Initio and dbt files intentionally have different schemas (decimal
  precision, column order) so they must be inferred independently rather
  than both being packed into one CREATE TABLE.

  Deploy once, or re-run any time the dummy files change:
    dbt run-operation load_benchmark_source_tables
#}
  {% if not execute %}{{ return('') }}{% endif %}

  {% set database = target.database %}
  {% set schema = target.schema %}
  {% set file_format_fqn = database ~ '.' ~ schema ~ '.benchmark_parquet_format' %}

  {% do run_query("CREATE FILE FORMAT IF NOT EXISTS " ~ file_format_fqn ~ " TYPE = PARQUET") %}

  {% set driver_rel = ref('benchmark_filelists') %}
  {% set rows_query %}
    SELECT FileNo, TableShortName, Folder, FileNameAb, FileNameDbt
    FROM {{ driver_rel }}
    WHERE UPPER(IsActive) = 'Y'
    ORDER BY FileNo
  {% endset %}
  {% set active_result = run_query(rows_query) %}

  {% for row in active_result.rows %}
    {% set table_short = row[1] %}
    {% set folder = resolve_benchmark_folder(row[2]) %}
    {% set filename_ab = row[3] %}
    {% set filename_dbt = row[4] %}

    {% for suffix, filename in [('abinitio', filename_ab), ('dbt', filename_dbt)] %}
      {% set target_table = database ~ '.' ~ schema ~ '.bench_' ~ table_short ~ '_' ~ suffix %}

      {% set create_table_sql %}
        CREATE OR REPLACE TABLE {{ target_table }}
          USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(
              INFER_SCHEMA(
                LOCATION => '{{ folder }}',
                FILE_FORMAT => '{{ file_format_fqn }}',
                FILES => ARRAY_CONSTRUCT('{{ filename }}')
              )
            )
          )
      {% endset %}
      {% do run_query(create_table_sql) %}

      {% set copy_into_sql %}
        COPY INTO {{ target_table }}
        FROM {{ folder }}
        FILES = ('{{ filename }}')
        FILE_FORMAT = (FORMAT_NAME = '{{ file_format_fqn }}')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      {% endset %}
      {% do run_query(copy_into_sql) %}

      {{ log("load_benchmark_source_tables: loaded " ~ target_table, info=True) }}
    {% endfor %}
  {% endfor %}
{% endmacro %}
