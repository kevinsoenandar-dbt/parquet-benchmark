{% macro create_compare_parquet_files_pyarrow() %}

{# Test variant of create_compare_parquet_files() — identical ColSeq/ColType
   logic and summary/detail insert shape, but ColVal is rewritten to use
   pyarrow's native Table.join() (Acero engine, vectorized C++) instead of
   pandas to_pandas() + merge() + iterrows(). Deployed under a SEPARATE
   proc name so it can be benchmarked side by side with the original
   without touching it.

   Deploy once per environment:
     dbt run-operation create_compare_parquet_files_pyarrow
#}

{% set sql %}
CREATE OR REPLACE PROCEDURE {{ target.database }}.{{ target.schema }}.COMPARE_PARQUET_FILES_PYARROW(
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
PACKAGES = ('snowflake-snowpark-python', 'pyarrow', 'numpy')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import os
import tempfile
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

def _download(session, stage_uri, tmp_dir):
    session.file.get(stage_uri, tmp_dir)
    return os.path.join(tmp_dir, os.path.basename(stage_uri))

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

        DETAIL_LIMIT = 15
        _counts = {}

        def _emit(typ, type_ref, pkeys_val, ab_val, dbt_val):
            k = (typ, type_ref)
            n = _counts.get(k, 0)
            if n >= DETAIL_LIMIT:
                return
            _counts[k] = n + 1
            details.append((typ, type_ref, pkeys_val, ab_val, dbt_val))

        # ColSeq — positional column-name comparison (unchanged from the
        # pandas variant — this is already O(columns), not a bottleneck)
        colseq_mismatched = 0
        for i in range(max(colcount_ab, colcount_dbt)):
            name_ab = cols_ab[i] if i < colcount_ab else None
            name_dbt = cols_dbt[i] if i < colcount_dbt else None
            if name_ab != name_dbt:
                colseq_mismatched += 1
                _emit('ColSeq', f'pos={i}', None, name_ab, name_dbt)

        # ColType — unchanged
        type_ab = {f.name: str(f.type) for f in table_ab.schema}
        type_dbt = {f.name: str(f.type) for f in table_dbt.schema}
        coltype_mismatched = 0
        for col in cols_ab:
            if col in type_dbt and type_ab[col] != type_dbt[col]:
                coltype_mismatched += 1
                _emit('ColType', col, None, type_ab[col], type_dbt[col])

        # ColVal — vectorized full outer join (Acero engine) replacing the
        # pandas to_pandas()+merge()+iterrows() row-by-row loop. Indicator
        # columns (_in_ab/_in_dbt) are added before the join so left-only/
        # right-only rows can be detected from null-after-join, without
        # depending on any particular data column being present. Detail
        # rows are only materialized for the (typically small) subset of
        # rows that actually differ — the full-table comparison itself
        # stays columnar throughout.
        colval_mismatched = 0
        pkeys_present = [k for k in pkeys if k in cols_ab and k in cols_dbt]
        if pkeys and len(pkeys_present) == len(pkeys):
            common_cols = [c for c in cols_ab
                           if c in cols_dbt and c not in ignore_cols and c not in pkeys]

            table_ab_ind = table_ab.append_column(
                '_in_ab', pa.array([True] * rowcount_ab, type=pa.bool_()))
            table_dbt_ind = table_dbt.append_column(
                '_in_dbt', pa.array([True] * rowcount_dbt, type=pa.bool_()))

            joined = table_ab_ind.join(
                table_dbt_ind, keys=pkeys, join_type='full outer',
                left_suffix='__ab', right_suffix='__dbt', coalesce_keys=True,
            )

            in_ab = pc.fill_null(joined.column('_in_ab'), False).to_numpy(zero_copy_only=False)
            in_dbt = pc.fill_null(joined.column('_in_dbt'), False).to_numpy(zero_copy_only=False)
            left_only_mask = in_ab & ~in_dbt
            right_only_mask = ~in_ab & in_dbt
            both_mask = in_ab & in_dbt

            def _pk_repr_from_table(t, i):
                parts = []
                for k in pkeys:
                    v = t.column(k)[int(i)].as_py()
                    parts.append('NULL' if v is None else str(v))
                return ';'.join(parts)

            colval_mismatched += int(left_only_mask.sum()) + int(right_only_mask.sum())

            for i in np.flatnonzero(left_only_mask):
                _emit('ColVal', '<row>', _pk_repr_from_table(joined, i), '<row present>', '<row missing>')
            for i in np.flatnonzero(right_only_mask):
                _emit('ColVal', '<row>', _pk_repr_from_table(joined, i), '<row missing>', '<row present>')

            if both_mask.any():
                both_table = joined.filter(pa.array(both_mask))
                for col in common_cols:
                    col_ab = both_table.column(col + '__ab')
                    col_dbt = both_table.column(col + '__dbt')
                    ab_null = pc.is_null(col_ab).to_numpy(zero_copy_only=False)
                    dbt_null = pc.is_null(col_dbt).to_numpy(zero_copy_only=False)
                    # not_equal on nulls can itself be null — fill with True
                    # so "can't determine equality" counts as a mismatch,
                    # matching the original v_ab != v_dbt semantics.
                    not_equal = pc.fill_null(pc.not_equal(col_ab, col_dbt), True).to_numpy(zero_copy_only=False)
                    either_null = ab_null | dbt_null
                    differ = np.where(ab_null & dbt_null, False,
                                       np.where(either_null, True, not_equal))
                    n_diff = int(differ.sum())
                    if n_diff:
                        colval_mismatched += n_diff
                        for i in np.flatnonzero(differ):
                            v_ab = col_ab[int(i)].as_py()
                            v_dbt = col_dbt[int(i)].as_py()
                            _emit(
                                'ColVal', col, _pk_repr_from_table(both_table, i),
                                None if v_ab is None else str(v_ab),
                                None if v_dbt is None else str(v_dbt),
                            )

        all_match = (
            rowcount_ab == rowcount_dbt
            and colcount_ab == colcount_dbt
            and colseq_mismatched == 0
            and coltype_mismatched == 0
            and colval_mismatched == 0
        )
        status = 'Pass' if all_match else 'Failed'

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
  {{ log("Creating COMPARE_PARQUET_FILES_PYARROW stored procedure...", info=True) }}
  {% set result = run_query(sql) %}
  {{ log("COMPARE_PARQUET_FILES_PYARROW procedure created successfully.", info=True) }}
{% endif %}

{% endmacro %}
