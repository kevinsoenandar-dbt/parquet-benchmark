"""
Reads the real dbt-exported parquet files (downloaded by
download_dbt_export.py from the actual Snowflake COPY INTO ... TYPE=PARQUET
export) and writes a corrupted "Ab Initio" sibling for each, with known,
deterministic corruption recorded in ground_truth.json — so both comparison
approaches (Snowpark/pyarrow vs audit_helper) can be checked for detection
parity, not just benchmarked for speed.

Corruption injected relative to the dbt file:
  - tbl_a: row value mismatches + dropped/extra rows (ColVal case), plus
           `balance` rewritten at a reduced decimal scale (if dbt exported
           it as a Parquet decimal type) or a narrower float type
           (otherwise) than the dbt export (ColType case).
  - tbl_b: same row-level corruption, plus `ref_code`/`amount` swapped in
           column position (ColSeq case).

Types are introspected from the actual dbt-exported file at runtime rather
than assumed, since Snowflake's NUMBER -> Parquet type mapping on COPY INTO
export isn't guaranteed by anything checked here.

Run: python generate_abinitio_from_dbt_export.py [--data-dir ../dummy_data]
"""
import argparse
import json
import numbers
import os
from decimal import Decimal

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

RNG_SEED = 42
VALUE_MISMATCH_PCT = 0.0005
DROP_PCT = 0.0001
EXTRA_PCT = 0.0001


def resolve_col(df_or_table, name: str) -> str:
    """Returns the actual column name matching `name` case-insensitively.

    Snowflake folds unquoted identifiers to uppercase, so a dbt model that
    writes `AS balance` actually produces a column named BALANCE — this
    looks up whatever casing the real export used instead of assuming it.
    """
    names = df_or_table.columns if isinstance(df_or_table, pd.DataFrame) else df_or_table.schema.names
    for n in names:
        if n.lower() == name.lower():
            return n
    raise KeyError(f"no column matching '{name}' (case-insensitive) found in {list(names)}")


def coerce_id_values(values, id_pa_type):
    """Builds new id values matching whatever physical type dbt's real
    export used for the id column — Snowflake stores integer-ish types as
    NUMBER internally, so `id` often comes back as decimal128, not int64.
    A bare `.astype('int64')` breaks when the target schema expects
    decimal128, since pyarrow won't reinterpret the two byte-for-byte."""
    if pa.types.is_decimal(id_pa_type):
        return [Decimal(int(v)) for v in values]
    return [int(v) for v in values]


def inject_row_corruption(df: pd.DataFrame, rng: np.random.Generator, mutate_col: str,
                           id_col: str, id_pa_type):
    n_rows = len(df)
    df = df.copy()

    n_mismatch = int(n_rows * VALUE_MISMATCH_PCT)
    mismatch_idx = rng.choice(n_rows, size=n_mismatch, replace=False)
    col = df[mutate_col]
    sample_val = col.dropna().iloc[0] if col.notna().any() else None
    is_numberlike = pd.api.types.is_numeric_dtype(col) or isinstance(sample_val, (numbers.Number, Decimal))
    if is_numberlike:
        df.loc[df.index[mismatch_idx], mutate_col] = [v + 1 for v in col.iloc[mismatch_idx]]
    else:
        df.loc[df.index[mismatch_idx], mutate_col] = "CORRUPTED"

    n_drop = int(n_rows * DROP_PCT)
    drop_idx = rng.choice(n_rows, size=n_drop, replace=False)
    df = df.drop(df.index[drop_idx]).reset_index(drop=True)

    n_extra = int(n_rows * EXTRA_PCT)
    extra_rows = df.sample(n=n_extra, random_state=int(rng.integers(0, 1_000_000)), replace=True).copy()
    max_id = int(df[id_col].max())
    new_ids = range(max_id + 1, max_id + 1 + n_extra)

    df[id_col] = coerce_id_values(df[id_col], id_pa_type)
    extra_rows[id_col] = coerce_id_values(new_ids, id_pa_type)
    df = pd.concat([df, extra_rows], ignore_index=True)

    return df, {
        "n_value_mismatch": n_mismatch,
        "n_dropped_rows": n_drop,
        "n_extra_rows": n_extra,
        "mutated_column": mutate_col,
    }


def degrade_column_type(table: pa.Table, col: str):
    """Weakens the physical type of `col` to create a genuine ColType
    mismatch, adapting to whatever type dbt's real export actually used."""
    field = table.schema.field(col)
    idx = table.schema.get_field_index(col)
    if pa.types.is_decimal(field.type):
        precision, scale = field.type.precision, field.type.scale
        new_scale = max(scale - 2, 0)
        values = table.column(col).to_pylist()
        new_values = [None if v is None else Decimal(str(round(float(v), new_scale))) for v in values]
        new_col = pa.array(new_values, type=pa.decimal128(precision, new_scale))
        note = f"{col}: decimal({precision},{scale}) dbt vs decimal({precision},{new_scale}) abinitio"
    else:
        new_col = table.column(col).cast(pa.float32())
        note = f"{col}: {field.type} dbt vs float32 abinitio"
    return table.set_column(idx, col, new_col), note


def process_tbl_a(data_dir: str, rng: np.random.Generator, ground_truth: dict):
    dbt_table = pq.read_table(os.path.join(data_dir, "dbt_tbl_a.parquet"))
    df = dbt_table.to_pandas()
    id_col = resolve_col(df, "id")
    balance_col = resolve_col(df, "balance")
    id_pa_type = dbt_table.schema.field(id_col).type

    df, stats = inject_row_corruption(df, rng, mutate_col=balance_col, id_col=id_col, id_pa_type=id_pa_type)
    ab_table = pa.Table.from_pandas(df, schema=dbt_table.schema, preserve_index=False)
    ab_table, coltype_note = degrade_column_type(ab_table, balance_col)
    pq.write_table(ab_table, os.path.join(data_dir, "abinitio_tbl_a.parquet"))
    stats["coltype_mismatch"] = coltype_note
    ground_truth["tbl_a"] = {
        "row_count_dbt": dbt_table.num_rows,
        "row_count_abinitio": ab_table.num_rows,
        **stats,
    }


def process_tbl_b(data_dir: str, rng: np.random.Generator, ground_truth: dict):
    dbt_table = pq.read_table(os.path.join(data_dir, "dbt_tbl_b.parquet"))
    df = dbt_table.to_pandas()
    id_col = resolve_col(df, "id")
    ref_code_col = resolve_col(df, "ref_code")
    amount_col = resolve_col(df, "amount")
    id_pa_type = dbt_table.schema.field(id_col).type

    df, stats = inject_row_corruption(df, rng, mutate_col=amount_col, id_col=id_col, id_pa_type=id_pa_type)
    ab_table = pa.Table.from_pandas(df, schema=dbt_table.schema, preserve_index=False)

    cols = ab_table.schema.names
    i, j = cols.index(ref_code_col), cols.index(amount_col)
    new_order = list(range(len(cols)))
    new_order[i], new_order[j] = new_order[j], new_order[i]
    ab_table = ab_table.select(new_order)

    pq.write_table(ab_table, os.path.join(data_dir, "abinitio_tbl_b.parquet"))
    stats["colseq_mismatch"] = f"{ref_code_col}/{amount_col} swapped in abinitio file"
    ground_truth["tbl_b"] = {
        "row_count_dbt": dbt_table.num_rows,
        "row_count_abinitio": ab_table.num_rows,
        **stats,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "dummy_data"))
    args = parser.parse_args()
    data_dir = os.path.abspath(args.data_dir)

    for f in ["dbt_tbl_a.parquet", "dbt_tbl_b.parquet"]:
        if not os.path.exists(os.path.join(data_dir, f)):
            raise SystemExit(
                f"{f} not found in {data_dir} — run "
                "`dbt run-operation export_benchmark_dbt_tables` then "
                "download_dbt_export.py first"
            )

    rng = np.random.default_rng(RNG_SEED)
    ground_truth = {}
    process_tbl_a(data_dir, rng, ground_truth)
    process_tbl_b(data_dir, rng, ground_truth)

    with open(os.path.join(data_dir, "ground_truth.json"), "w") as f:
        json.dump(ground_truth, f, indent=2)

    print(f"Wrote abinitio_tbl_a.parquet, abinitio_tbl_b.parquet, ground_truth.json to {data_dir}")


if __name__ == "__main__":
    main()
