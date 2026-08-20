"""
Downloads the two dbt-exported parquet files (produced by
macros/export_benchmark_dbt_tables.sql) from the benchmark stage to local
disk, so generate_abinitio_from_dbt_export.py can read dbt's real exported
schema/types and inject a corrupted Ab Initio sibling from it.

Uses the same SNOWFLAKE_* env vars as upload_to_stage.py.

Run: python download_dbt_export.py [--out-dir ../dummy_data]
"""
import argparse
import os

from upload_to_stage import get_connection


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=os.path.join(os.path.dirname(__file__), "..", "dummy_data"))
    parser.add_argument("--stage", default="benchmark_stage")
    args = parser.parse_args()

    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    conn = get_connection()
    try:
        cur = conn.cursor()
        for table_short in ["tbl_a", "tbl_b"]:
            get_sql = f"GET @{args.stage}/benchmark/dbt_{table_short}.parquet file://{out_dir}/"
            print(f"Downloading dbt_{table_short}.parquet...")
            cur.execute(get_sql)
        print(f"Done. Files saved to {out_dir}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
