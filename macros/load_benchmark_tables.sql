{% macro load_benchmark_source_tables() %}
{#
  Loads the Ab Initio parquet files (pushed to stage by
  scripts/upload_to_stage.py) into native Snowflake tables, since
  audit_helper compares relations, not files. The dbt side needs no
  loading — it's already a real table (bench_tbl_a_dbt / bench_tbl_b_dbt)
  built by dbt itself; the parquet export exists only for the existing
  Snowpark/pyarrow approach to read.

  Each file gets its own CREATE TABLE ... USING TEMPLATE, built from
  INFER_SCHEMA scoped to that single file via the FILES parameter, since
  the folder also contains the dbt-side file.

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
    SELECT FileNo, TableShortName, Folder, FileNameAb
    FROM {{ driver_rel }}
    WHERE UPPER(IsActive) = 'Y'
    ORDER BY FileNo
  {% endset %}
  {% set active_result = run_query(rows_query) %}

  {% for row in active_result.rows %}
    {% set table_short = row[1] %}
    {% set folder = resolve_benchmark_folder(row[2]) %}
    {% set filename_ab = row[3] %}
    {% set target_table = database ~ '.' ~ schema ~ '.bench_' ~ table_short ~ '_abinitio' %}

    {% set create_table_sql %}
      CREATE OR REPLACE TABLE {{ target_table }}
        USING TEMPLATE (
          SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
          FROM TABLE(
            INFER_SCHEMA(
              LOCATION => '{{ folder }}',
              FILE_FORMAT => '{{ file_format_fqn }}',
              FILES => ARRAY_CONSTRUCT('{{ filename_ab }}')
            )
          )
        )
    {% endset %}
    {% do run_query(create_table_sql) %}

    {% set copy_into_sql %}
      COPY INTO {{ target_table }}
      FROM {{ folder }}
      FILES = ('{{ filename_ab }}')
      FILE_FORMAT = (FORMAT_NAME = '{{ file_format_fqn }}')
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    {% endset %}
    {% do run_query(copy_into_sql) %}

    {{ log("load_benchmark_source_tables: loaded " ~ target_table, info=True) }}
  {% endfor %}
{% endmacro %}
