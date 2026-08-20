{% macro create_compare_parquet_files() %}

{# Creates a Snowpark Python stored procedure that compares two parquet files
   (FileAb from legacy Ab Initio vs FileDbt from dbt export_to_stage) and
   writes results directly into the two reconciliation tables.

   The procedure:
     - Downloads both parquets from their Snowflake stages
     - Reads them via pyarrow, sorts by pkeys
     - Compares column sequence, column types, and cell values
     - INSERTs one row into summary_table and N rows into details_table
     - System fields listed in ignore_cols are skipped for value comparison

   Deploy once per environment:
     dbt run-operation create_compare_parquet_files
#}

{% set sql %}
CREATE OR REPLACE PROCEDURE {{ target.database }}.PRODUCT.COMPARE_PARQUET_FILES(
    file_ab VARCHAR,
    file_dbt VARCHAR,
    pkeys ARRAY,
    ignore_cols ARRAY,
    run_no NUMBER,
    file_no NUMBER,
    period VARCHAR,
    summary_table VARCHAR,
    details_table VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pyarrow', 'pandas')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import os
import tempfile
import pyarrow.parquet as pq
import pandas as pd

def _download(session, stage_uri, tmp_dir):
    # stage_uri like '@DB.SCH.STAGE/sub/file.parquet' — session.file.get needs
    # the full URI and a local directory; it preserves the filename.
    session.file.get(stage_uri, tmp_dir)
    return os.path.join(tmp_dir, os.path.basename(stage_uri))

def _pk_repr(row, pkeys):
    parts = []
    for k in pkeys:
        v = row[k]
        parts.append('NULL' if v is None or (not isinstance(v, (list, dict)) and pd.isna(v))
                     else str(v))
    return ';'.join(parts)

def _sql_lit(v):
    if v is None:
        return 'NULL'
    return "'" + str(v).replace("'", "''") + "'"

def run(session, file_ab, file_dbt, pkeys, ignore_cols, run_no, file_no,
        period, summary_table, details_table):
    pkeys = list(pkeys or [])
    ignore_cols = set(ignore_cols or [])
    details = []

    tmp_ab = tempfile.mkdtemp()
    tmp_dbt = tempfile.mkdtemp()
    try:
        local_ab = _download(session, file_ab, tmp_ab)
        local_dbt = _download(session, file_dbt, tmp_dbt)

        table_ab = pq.read_table(local_ab)
        table_dbt = pq.read_table(local_dbt)

        cols_ab = table_ab.schema.names
        cols_dbt = table_dbt.schema.names
        rowcount_ab = table_ab.num_rows
        rowcount_dbt = table_dbt.num_rows
        colcount_ab = len(cols_ab)
        colcount_dbt = len(cols_dbt)

        # Detail rows are 5-tuples: (Type, Type_Ref, PKeys, FileAb_Val, FileDbt_Val).
        # PKeys is the semicolon-joined pkey values for ColVal rows and empty for
        # ColSeq/ColType. Cap detail rows per (Type, Type_Ref) at DETAIL_LIMIT to
        # avoid unbounded inserts; summary counters are NOT capped.
        DETAIL_LIMIT = 15
        _counts = {}

        def _emit(typ, type_ref, pkeys_val, ab_val, dbt_val):
            k = (typ, type_ref)
            n = _counts.get(k, 0)
            if n >= DETAIL_LIMIT:
                return
            _counts[k] = n + 1
            details.append((typ, type_ref, pkeys_val, ab_val, dbt_val))


        # ColSeq — positional column-name comparison
        colseq_mismatched = 0
        for i in range(max(colcount_ab, colcount_dbt)):
            name_ab = cols_ab[i] if i < colcount_ab else None
            name_dbt = cols_dbt[i] if i < colcount_dbt else None
            if name_ab != name_dbt:
                colseq_mismatched += 1
                _emit('ColSeq', f'pos={i}', None, name_ab, name_dbt)

        # ColType — for columns present in both, compare pyarrow type strings
        type_ab = {f.name: str(f.type) for f in table_ab.schema}
        type_dbt = {f.name: str(f.type) for f in table_dbt.schema}
        coltype_mismatched = 0
        for col in cols_ab:
            if col in type_dbt and type_ab[col] != type_dbt[col]:
                coltype_mismatched += 1
                _emit('ColType', col, None, type_ab[col], type_dbt[col])

        # ColVal — sort by pkeys, outer-merge, compare non-ignored columns.
        # One detail row per differing column per matched key.
        colval_mismatched = 0
        pkeys_present = [k for k in pkeys if k in cols_ab and k in cols_dbt]
        if pkeys and len(pkeys_present) == len(pkeys):
            df_ab = table_ab.to_pandas().sort_values(pkeys).reset_index(drop=True)
            df_dbt = table_dbt.to_pandas().sort_values(pkeys).reset_index(drop=True)

            common_cols = [c for c in cols_ab
                           if c in cols_dbt and c not in ignore_cols and c not in pkeys]

            merged = df_ab.merge(
                df_dbt, on=pkeys, how='outer', indicator=True,
                suffixes=('__ab', '__dbt'),
            )

            for _, row in merged.iterrows():
                pk_repr = _pk_repr(row, pkeys)
                if row['_merge'] == 'left_only':
                    colval_mismatched += 1
                    _emit('ColVal', '<row>', pk_repr, '<row present>', '<row missing>')
                elif row['_merge'] == 'right_only':
                    colval_mismatched += 1
                    _emit('ColVal', '<row>', pk_repr, '<row missing>', '<row present>')
                else:
                    for col in common_cols:
                        v_ab = row[f'{col}__ab']
                        v_dbt = row[f'{col}__dbt']
                        v_ab_null = pd.isna(v_ab) if not isinstance(v_ab, (list, dict)) else False
                        v_dbt_null = pd.isna(v_dbt) if not isinstance(v_dbt, (list, dict)) else False
                        if v_ab_null and v_dbt_null:
                            continue
                        if v_ab_null != v_dbt_null or v_ab != v_dbt:
                            colval_mismatched += 1
                            _emit(
                                'ColVal', col, pk_repr,
                                None if v_ab_null else str(v_ab),
                                None if v_dbt_null else str(v_dbt),
                            )

        all_match = (
            rowcount_ab == rowcount_dbt
            and colcount_ab == colcount_dbt
            and colseq_mismatched == 0
            and coltype_mismatched == 0
            and colval_mismatched == 0
        )
        status = 'Pass' if all_match else 'Failed'

        # Write summary row. Period is passed as YYYY-MM-DD; TRY_TO_DATE
        # returns NULL when the caller omits it, keeping the row valid.
        insert_summary = (
            f"INSERT INTO {summary_table} "
            "(RunNo, FileNo, Period, Load_ts, "
            "RowCount_FileAb, RowCount_FileDbt, "
            "ColCount_FileAb, ColCount_FileDbt, "
            "ColSeq_Mismatched, ColType_Mismatched, ColVal_Mismatched, Status) "
            f"SELECT {run_no}, {file_no}, "
            f"TRY_TO_DATE({_sql_lit(period)}, 'YYYY-MM-DD'), "
            f"CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, "
            f"{rowcount_ab}, {rowcount_dbt}, {colcount_ab}, {colcount_dbt}, "
            f"{colseq_mismatched}, {coltype_mismatched}, {colval_mismatched}, "
            f"{_sql_lit(status)}"
        )
        session.sql(insert_summary).collect()

        # Write detail rows in batches to keep SQL text manageable.
        # d = (Type, Type_Ref, PKeys, FileAb_Val, FileDbt_Val)
        if details:
            BATCH = 500
            for start in range(0, len(details), BATCH):
                chunk = details[start:start + BATCH]
                values_sql = ', '.join(
                    f"({run_no}, {file_no}, {_sql_lit(d[0])}, {_sql_lit(d[1])}, "
                    f"{_sql_lit(d[2])}, {_sql_lit(d[3])}, {_sql_lit(d[4])})"
                    for d in chunk
                )
                insert_details = (
                    f"INSERT INTO {details_table} "
                    "(RunNo, FileNo, Type, Type_Ref, PKeys, FileAb_Val, FileDbt_Val) VALUES "
                    + values_sql
                )
                session.sql(insert_details).collect()

        return f"RunNo={run_no} FileNo={file_no} Status={status} details={len(details)}"
    finally:
        for d in (tmp_ab, tmp_dbt):
            try:
                for f in os.listdir(d):
                    try:
                        os.remove(os.path.join(d, f))
                    except OSError:
                        pass
                os.rmdir(d)
            except OSError:
                pass
$$
{% endset %}

{% if execute %}
  {{ log("Creating COMPARE_PARQUET_FILES stored procedure...", info=True) }}
  {% set result = run_query(sql) %}
  {{ log("COMPARE_PARQUET_FILES procedure created successfully.", info=True) }}
{% endif %}

{% endmacro %}
