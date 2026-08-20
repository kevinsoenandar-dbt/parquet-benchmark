{% macro run_parquet_reconciliation() %}
{#
  Orchestrator for PARQUET_RUN_SUMMARY + PARQUET_RUN_DETAILS.
  Invoked as a post-hook on parquet_run_summary__collections. Reads active
  rows in parquet_filelists__collections, resolves per-model primary keys,
  invokes COMPARE_PARQUET_FILES for each pair (FileAb, FileDbt), and INSERTs
  one summary row + N detail rows per active file.

  Deploy COMPARE_PARQUET_FILES first:
      dbt run-operation create_compare_parquet_files
#}

{% if not execute %}
  {{ return('') }}
{% endif %}

{# Ignore-list for field-value comparison — applied on top of the pkeys #}
{% set ignore_cols = [
    'snap_d', 'load_ts', 'etl_ts', 'etl_actv_c',
    'row_secu_accs_c', 'etl_id'
] %}

{# Resolve business date used to substitute {YYYYMMDD} in FileNameAb/FileNameDbt.
   Priority: business_date var (YYYY-MM-DD) > run_started_at.
   Note: get_export_business_date() is not used here — it assumes `this` has a
   gleod_ts column, which parquet_run_summary__collections does not. #}
{% set biz_date_var = var('business_date', none) %}
{% if biz_date_var is not none %}
  {% set date_str = biz_date_var | replace('-', '') %}
  {% set period_iso = biz_date_var %}
{% else %}
  {% set date_str = run_started_at.strftime('%Y%m%d') %}
  {% set period_iso = run_started_at.strftime('%Y-%m-%d') %}
{% endif %}

{% set database = target.database %}
{% set summary_rel = ref('parquet_run_summary__collections') %}
{% set details_rel = ref('parquet_run_details__collections') %}
{% set driver_rel = ref('parquet_filelists__collections') %}

{# One RunNo per invocation, shared across every driver row this run processes #}
{% set next_run_query %}
  SELECT COALESCE(MAX(RunNo), 0) + 1 AS next_run FROM {{ summary_rel }}
{% endset %}
{% set next_run_result = run_query(next_run_query) %}
{% set run_no = next_run_result.rows[0][0] %}

{% set active_query %}
  SELECT FileNo, TableShortName, ModelName, Folder, FileNameAb, FileNameDbt, PKeys
  FROM {{ driver_rel }}
  WHERE UPPER(IsActive) = 'Y'
  ORDER BY FileNo
{% endset %}
{% set active_result = run_query(active_query) %}

{% if active_result is none or active_result.rows | length == 0 %}
  {{ log("run_parquet_reconciliation: no active rows in parquet_filelists__collections — skipping.", info=True) }}
  {{ return('') }}
{% endif %}

{% for row in active_result.rows %}
  {% set file_no = row[0] %}
  {% set table_short = row[1] %}
  {% set model_name = row[2] %}
  {% set folder_tmpl = row[3] %}
  {% set filename_ab_tmpl = row[4] %}
  {% set filename_dbt_tmpl = row[5] %}
  {% set pkeys_str = row[6] %}

  {# Resolve tokens: {database} → target.database, {YYYYMMDD} → business date.
     Trim trailing slash on folder so the join produces exactly one separator. #}
  {% set folder = (folder_tmpl | replace('{database}', database) | replace('{YYYYMMDD}', date_str)).rstrip('/') %}
  {% set filename_ab = filename_ab_tmpl | replace('{YYYYMMDD}', date_str) %}
  {% set filename_dbt = filename_dbt_tmpl | replace('{YYYYMMDD}', date_str) %}
  {% set file_ab = folder ~ '/' ~ filename_ab %}
  {% set file_dbt = folder ~ '/' ~ filename_dbt %}

  {# PKeys come from the driver seed — semicolon-separated, ordered. #}
  {% set pkeys = [] %}
  {% if pkeys_str is not none %}
    {% for k in pkeys_str.split(';') %}
      {% set k_trim = k.strip() %}
      {% if k_trim %}{% do pkeys.append(k_trim) %}{% endif %}
    {% endfor %}
  {% endif %}
  {% if pkeys | length == 0 %}
    {{ exceptions.raise_compiler_error(
        "run_parquet_reconciliation: PKeys is empty for FileNo=" ~ file_no
        ~ " in parquet_filelists__collections. Populate the semicolon-separated column."
    ) }}
  {% endif %}

  {% set pkeys_literals = [] %}
  {% for k in pkeys %}
    {% do pkeys_literals.append("'" ~ k | replace("'", "''") ~ "'") %}
  {% endfor %}
  {% set ignore_literals = [] %}
  {% for k in ignore_cols %}
    {% do ignore_literals.append("'" ~ k | replace("'", "''") ~ "'") %}
  {% endfor %}
  {% set pkeys_sql = "ARRAY_CONSTRUCT(" ~ (pkeys_literals | join(', ')) ~ ")" %}
  {% set ignore_sql = "ARRAY_CONSTRUCT(" ~ (ignore_literals | join(', ')) ~ ")" %}

  {{ log("run_parquet_reconciliation: comparing FileNo=" ~ file_no ~ " " ~ file_ab ~ " vs " ~ file_dbt, info=True) }}

  {# The procedure writes directly into summary_rel and details_rel — pass
     their fully-qualified names as parameters. #}
  {% set summary_fqn = summary_rel.database ~ '.' ~ summary_rel.schema ~ '.' ~ summary_rel.identifier %}
  {% set details_fqn = details_rel.database ~ '.' ~ details_rel.schema ~ '.' ~ details_rel.identifier %}

  {% set call_sql %}
    CALL {{ database }}.PRODUCT.COMPARE_PARQUET_FILES(
        '{{ file_ab | replace("'", "''") }}',
        '{{ file_dbt | replace("'", "''") }}',
        {{ pkeys_sql }},
        {{ ignore_sql }},
        {{ run_no }},
        {{ file_no }},
        '{{ period_iso }}',
        '{{ summary_fqn }}',
        '{{ details_fqn }}'
    )
  {% endset %}
  {% do run_query(call_sql) %}

{% endfor %}

{{ log("run_parquet_reconciliation: RunNo=" ~ run_no ~ " completed for " ~ (active_result.rows | length) ~ " active file(s).", info=True) }}

{% endmacro %}
