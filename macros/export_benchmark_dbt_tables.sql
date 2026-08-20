{% macro export_benchmark_dbt_tables() %}
{#
  Exports the two synthetic "dbt" ground-truth tables to the benchmark
  stage as single parquet files — mirrors the customer's real
  export-EOD-to-parquet macro. This becomes:
    - the file the existing Snowpark/pyarrow approach reads directly, and
    - the input scripts/generate_abinitio_from_dbt_export.py reads back to
      produce the corrupted Ab Initio sibling using dbt's actual exported
      schema/types, instead of a Python-side guess.

  Run after building the two dbt tables:
    dbt run --select bench_tbl_a_dbt bench_tbl_b_dbt
    dbt run-operation export_benchmark_dbt_tables
#}
  {% if not execute %}{{ return('') }}{% endif %}

  {% set database = target.database %}
  {% set schema = target.schema %}
  {% set stage_fqn = database ~ '.' ~ schema ~ '.benchmark_stage' %}

  {% do run_query("CREATE STAGE IF NOT EXISTS " ~ stage_fqn) %}

  {% for table_short in ['tbl_a', 'tbl_b'] %}
    {% set model_rel = ref('bench_' ~ table_short ~ '_dbt') %}
    {# MAX_FILE_SIZE default is 16MB even in SINGLE=TRUE mode — a 1M-row
       table exceeds that, so it must be raised explicitly (5GB is the max
       Snowflake allows).

       COPY INTO ... FROM <table_name> directly unloads with generic
       positional column names (_COL_0, _COL_1, ...) — a SELECT wrapping
       the table plus HEADER = TRUE is required to preserve real column
       names in the Parquet file/metadata. #}
    {% set copy_sql %}
      COPY INTO @{{ stage_fqn }}/benchmark/dbt_{{ table_short }}.parquet
      FROM (SELECT * FROM {{ model_rel }})
      FILE_FORMAT = (TYPE = PARQUET)
      HEADER = TRUE
      SINGLE = TRUE
      MAX_FILE_SIZE = 5368709120
      OVERWRITE = TRUE
    {% endset %}
    {% do run_query(copy_sql) %}
    {{ log("export_benchmark_dbt_tables: exported " ~ table_short ~ " to stage", info=True) }}
  {% endfor %}
{% endmacro %}
