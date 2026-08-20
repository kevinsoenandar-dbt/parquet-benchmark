{% macro run_benchmark_audit_helper() %}
{#
  Benchmarks the PROPOSED approach: audit_helper set-based SQL comparison
  against the tables loaded by load_benchmark_source_tables().

  compare_relation_columns() output columns (confirmed against the
  dbt-labs/dbt-audit-helper source): column_name, a_ordinal_position,
  b_ordinal_position, a_data_type, b_data_type, has_ordinal_position_match,
  has_data_type_match, in_a_only, in_b_only, in_both.

  compare_relations() with summarize=true (default) returns: in_a, in_b,
  count, percent_of_total — up to 4 rows classifying whole-row matches.

  Run: dbt run-operation run_benchmark_audit_helper
#}
  {% if not execute %}{{ return('') }}{% endif %}
  {% do create_benchmark_runs_table() %}

  {% set driver_rel = ref('benchmark_filelists') %}
  {% set rows_query %}
    SELECT TableShortName, PKeys FROM {{ driver_rel }}
    WHERE UPPER(IsActive) = 'Y' ORDER BY FileNo
  {% endset %}
  {% set active_result = run_query(rows_query) %}

  {% for row in active_result.rows %}
    {% set table_short = row[0] %}
    {% set pkey = row[1] %}
    {% set query_tag = 'bench_audit_helper_' ~ table_short ~ '_' ~ modules.datetime.datetime.now().strftime('%Y%m%d%H%M%S') %}
    {% do run_query("ALTER SESSION SET QUERY_TAG = '" ~ query_tag ~ "'") %}

    {% set a_relation = api.Relation.create(database=target.database, schema=target.schema, identifier='bench_' ~ table_short ~ '_dbt') %}
    {% set b_relation = api.Relation.create(database=target.database, schema=target.schema, identifier='bench_' ~ table_short ~ '_abinitio') %}

    {% set start_ms = current_epoch_ms() %}

    {% set colcompare_sql = audit_helper.compare_relation_columns(a_relation, b_relation) %}
    {% set colcompare_result = run_query(colcompare_sql) %}
    {% set colseq_mismatch = namespace(n=0) %}
    {% set coltype_mismatch = namespace(n=0) %}
    {% for r in colcompare_result.rows %}
      {% if not r['has_ordinal_position_match'] %}{% set colseq_mismatch.n = colseq_mismatch.n + 1 %}{% endif %}
      {% if not r['has_data_type_match'] %}{% set coltype_mismatch.n = coltype_mismatch.n + 1 %}{% endif %}
    {% endfor %}

    {% set row_compare_sql = audit_helper.compare_relations(a_relation=a_relation, b_relation=b_relation, primary_key=pkey) %}
    {% set row_compare_result = run_query(row_compare_sql) %}
    {% set row_mismatch = namespace(n=0) %}
    {% set row_count_a = namespace(n=0) %}
    {% set row_count_b = namespace(n=0) %}
    {% for r in row_compare_result.rows %}
      {% if not (r['in_a'] and r['in_b']) %}{% set row_mismatch.n = row_mismatch.n + r['count'] %}{% endif %}
      {% if r['in_a'] %}{% set row_count_a.n = row_count_a.n + r['count'] %}{% endif %}
      {% if r['in_b'] %}{% set row_count_b.n = row_count_b.n + r['count'] %}{% endif %}
    {% endfor %}

    {% set elapsed = elapsed_ms_since(start_ms) %}

    {% do log_benchmark_run(
          approach='audit_helper',
          table_short_name=table_short,
          row_count_a=row_count_a.n,
          row_count_b=row_count_b.n,
          colseq_mismatch=colseq_mismatch.n,
          coltype_mismatch=coltype_mismatch.n,
          row_mismatch_count=row_mismatch.n,
          elapsed_ms=elapsed,
          query_tag=query_tag
       ) %}

    {{ log("run_benchmark_audit_helper: " ~ table_short ~ " elapsed_ms=" ~ elapsed, info=True) }}
  {% endfor %}
{% endmacro %}
