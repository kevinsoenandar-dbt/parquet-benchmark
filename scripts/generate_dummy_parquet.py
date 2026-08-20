"""
Generates 4 dummy parquet files for benchmarking the parquet-diff approach
(Snowpark/pyarrow/pandas) against the audit_helper (set-based SQL) approach.

Two synthetic table pairs, each with a "dbt" version (treated as ground
truth) and an "abinitio" version with known, deterministic corruption
injected — so both comparison approaches can be checked for detection
parity, not just speed.

Corruption injected into the *_abinitio file relative to *_dbt:
  - tbl_a: a fraction of rows get one column value changed (ColVal case),
           a fraction of dbt rows are dropped (row missing from abinitio),
           a fraction of extra rows are added (row missing from dbt),
           the `balance` column is written as decimal(18,2) instead of the
           dbt file's decimal(18,4) (ColType case).
  - tbl_b: same row-level corruption pattern, plus two columns are swapped
           in position (ColSeq case).

Run: python generate_dummy_parquet.py [--rows 1000000] [--out-dir ../dummy_data]
"""
import argparse
import json
import os
from decimal import Decimal

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

RNG_SEED = 42


def make_base_table(n_rows: int, rng: np.random.Generator) -> dict:
    ids = np.arange(1, n_rows + 1, dtype=np.int64)
    acct_types = rng.choice(["SAV", "CHK", "LOAN", "TERM"], size=n_rows)
    balances = np.round(rng.uniform(-5000, 50000, size=n_rows), 4)
    open_days = rng.integers(0, 365 * 15, size=n_rows)
    descriptions = np.array([f"acct-desc-{i:08d}" for i in ids])
    status_flags = rng.choice(["A", "I", "C"], size=n_rows)
    return {
        "id": ids,
        "acct_type": acct_types,
        "balance": balances,
        "open_days": open_days,
        "description": descriptions,
        "status_flag": status_flags,
    }


def make_base_table_b(n_rows: int, rng: np.random.Generator) -> dict:
    ids = np.arange(1, n_rows + 1, dtype=np.int64)
    ref_codes = np.array([f"REF{i:09d}" for i in ids])
    amounts = np.round(rng.uniform(0, 100000, size=n_rows), 2)
    notes = rng.choice(["auto-generated", "manual-adj", "system-sync", ""], size=n_rows)
    return {
        "id": ids,
        "ref_code": ref_codes,
        "amount": amounts,
        "notes": notes,
    }


def inject_corruption(base: dict, rng: np.random.Generator, value_mismatch_pct: float,
                       drop_pct: float, extra_pct: float, mutate_col: str):
    n_rows = len(base["id"])
    df_cols = {k: np.array(v) for k, v in base.items()}

    n_mismatch = int(n_rows * value_mismatch_pct)
    mismatch_idx = rng.choice(n_rows, size=n_mismatch, replace=False)
    if df_cols[mutate_col].dtype.kind in "fi":
        df_cols[mutate_col][mismatch_idx] = df_cols[mutate_col][mismatch_idx] + 1
    else:
        df_cols[mutate_col][mismatch_idx] = "CORRUPTED"

    n_drop = int(n_rows * drop_pct)
    drop_idx = set(rng.choice(n_rows, size=n_drop, replace=False).tolist())
    keep_mask = np.array([i not in drop_idx for i in range(n_rows)])
    df_cols = {k: v[keep_mask] for k, v in df_cols.items()}

    n_extra = int(n_rows * extra_pct)
    max_id = int(base["id"].max())
    extra = {}
    for k, v in df_cols.items():
        if k == "id":
            extra[k] = np.arange(max_id + 1, max_id + 1 + n_extra, dtype=np.int64)
        elif np.issubdtype(v.dtype, np.number):
            extra[k] = rng.choice(v, size=n_extra)
        else:
            extra[k] = rng.choice(v, size=n_extra)
    for k in df_cols:
        df_cols[k] = np.concatenate([df_cols[k], extra[k]])

    return df_cols, {
        "n_value_mismatch": n_mismatch,
        "n_dropped_rows": n_drop,
        "n_extra_rows": n_extra,
        "mutated_column": mutate_col,
    }


def write_table_a(out_dir: str, n_rows: int, rng: np.random.Generator, ground_truth: dict):
    base = make_base_table(n_rows, rng)

    # dbt (ground truth) file — balance as decimal(18,4)
    dbt_cols = dict(base)
    dbt_table = pa.table({
        "id": pa.array(dbt_cols["id"], type=pa.int64()),
        "acct_type": pa.array(dbt_cols["acct_type"], type=pa.string()),
        "balance": pa.array([Decimal(str(round(float(x), 4))) for x in dbt_cols["balance"]],
                             type=pa.decimal128(18, 4)),
        "open_days": pa.array(dbt_cols["open_days"], type=pa.int32()),
        "description": pa.array(dbt_cols["description"], type=pa.string()),
        "status_flag": pa.array(dbt_cols["status_flag"], type=pa.string()),
    })
    pq.write_table(dbt_table, os.path.join(out_dir, "dbt_tbl_a.parquet"))

    # abinitio file — corrupted row values + rows, balance as decimal(18,2) (ColType case)
    ab_cols, stats = inject_corruption(
        base, rng, value_mismatch_pct=0.0005, drop_pct=0.0001, extra_pct=0.0001,
        mutate_col="balance",
    )
    ab_table = pa.table({
        "id": pa.array(ab_cols["id"], type=pa.int64()),
        "acct_type": pa.array(ab_cols["acct_type"], type=pa.string()),
        "balance": pa.array([Decimal(str(round(float(x), 2))) for x in ab_cols["balance"]],
                             type=pa.decimal128(18, 2)),
        "open_days": pa.array(ab_cols["open_days"], type=pa.int32()),
        "description": pa.array(ab_cols["description"], type=pa.string()),
        "status_flag": pa.array(ab_cols["status_flag"], type=pa.string()),
    })
    pq.write_table(ab_table, os.path.join(out_dir, "abinitio_tbl_a.parquet"))

    stats["coltype_mismatch"] = "balance: decimal(18,4) dbt vs decimal(18,2) abinitio"
    ground_truth["tbl_a"] = {"row_count_dbt": len(dbt_cols["id"]), "row_count_abinitio": len(ab_cols["id"]), **stats}


def write_table_b(out_dir: str, n_rows: int, rng: np.random.Generator, ground_truth: dict):
    base = make_base_table_b(n_rows, rng)

    dbt_table = pa.table({
        "id": pa.array(base["id"], type=pa.int64()),
        "ref_code": pa.array(base["ref_code"], type=pa.string()),
        "amount": pa.array(base["amount"], type=pa.float64()),
        "notes": pa.array(base["notes"], type=pa.string()),
    })
    pq.write_table(dbt_table, os.path.join(out_dir, "dbt_tbl_b.parquet"))

    ab_cols, stats = inject_corruption(
        base, rng, value_mismatch_pct=0.0005, drop_pct=0.0001, extra_pct=0.0001,
        mutate_col="amount",
    )
    # ColSeq case: swap ref_code and amount column positions vs the dbt file
    ab_table = pa.table({
        "id": pa.array(ab_cols["id"], type=pa.int64()),
        "amount": pa.array(ab_cols["amount"], type=pa.float64()),
        "ref_code": pa.array(ab_cols["ref_code"], type=pa.string()),
        "notes": pa.array(ab_cols["notes"], type=pa.string()),
    })
    pq.write_table(ab_table, os.path.join(out_dir, "abinitio_tbl_b.parquet"))

    stats["colseq_mismatch"] = "ref_code/amount swapped at positions 1,2 in abinitio file"
    ground_truth["tbl_b"] = {"row_count_dbt": len(base["id"]), "row_count_abinitio": len(ab_cols["id"]), **stats}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--out-dir", default=os.path.join(os.path.dirname(__file__), "..", "dummy_data"))
    args = parser.parse_args()

    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    rng = np.random.default_rng(RNG_SEED)
    ground_truth = {"rows_requested": args.rows}

    write_table_a(out_dir, args.rows, rng, ground_truth)
    write_table_b(out_dir, args.rows, rng, ground_truth)

    with open(os.path.join(out_dir, "ground_truth.json"), "w") as f:
        json.dump(ground_truth, f, indent=2)

    print(f"Wrote 4 parquet files + ground_truth.json to {out_dir}")
    for name in ["dbt_tbl_a.parquet", "abinitio_tbl_a.parquet", "dbt_tbl_b.parquet", "abinitio_tbl_b.parquet"]:
        path = os.path.join(out_dir, name)
        print(f"  {name}: {os.path.getsize(path) / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    main()
