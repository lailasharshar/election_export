#!/usr/bin/env python3
"""
Compare two precinct CSVs (original vs combined) keyed by (state, county, precinct).
Only output differences:
  - rows missing in file1 or file2
  - value mismatches on any non-key column

Usage:
  python diff_precinct_files.py --file1 original.csv --file2 combined.csv --out diffs.csv

Options:
  --float-tol 0.0        : numeric tolerance for float comparisons (default 0, exact)
  --case-sensitive       : compare strings case-sensitively (default off)
  --only-cols col1,col2  : only compare these columns (besides the keys). If omitted, compares all shared columns.

Output CSV columns:
  state, county, precinct, diff_type, column, file1_value, file2_value, description
"""

import argparse
import os
import sys
from typing import List, Set
import pandas as pd

KEYS = ["state", "county", "precinct"]

def read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        sys.exit(f"File not found: {path}")
    # Read as strings; keep blanks as blanks
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    # Ensure key columns exist
    for k in KEYS:
        if k not in df.columns:
            sys.exit(f"{path}: missing key column '{k}'")
    # Normalize key fields
    for k in KEYS:
        df[k] = df[k].astype(str).str.strip()
    return df

def normalize_value(s: str, case_sensitive: bool) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    if not case_sensitive:
        s = s.lower()
    return s

def try_float(v: str):
    try:
        return float(v)
    except Exception:
        return None

def compare_values(v1: str, v2: str, float_tol: float, case_sensitive: bool) -> bool:
    """
    Returns True if values are considered equal (within tolerance / normalization), else False.
    """
    n1 = normalize_value(v1, case_sensitive)
    n2 = normalize_value(v2, case_sensitive)

    # If identical strings after normalization -> equal
    if n1 == n2:
        return True

    # If both look like floats, compare numerically with tolerance
    f1, f2 = try_float(n1), try_float(n2)
    if f1 is not None and f2 is not None:
        return abs(f1 - f2) <= float_tol

    return False

def main():
    ap = argparse.ArgumentParser(description="Diff two precinct CSVs by (state, county, precinct).")
    ap.add_argument("--file1", required=True, help="Original CSV file")
    ap.add_argument("--file2", required=True, help="Combined CSV file")
    ap.add_argument("--out", required=True, help="Output CSV for differences")
    ap.add_argument("--float-tol", type=float, default=0.0, help="Numeric tolerance for float comparisons")
    ap.add_argument("--case-sensitive", action="store_true", help="Enable case-sensitive string comparison")
    ap.add_argument("--only-cols", help="Comma-separated list of columns to compare (in addition to keys)")
    args = ap.parse_args()

    df1 = read_csv(args.file1)
    df2 = read_csv(args.file2)

    # Decide which columns to compare
    if args.only_cols:
        compare_cols: Set[str] = set([c.strip() for c in args.only_cols.split(",") if c.strip()])
    else:
        # Compare all columns shared by both, excluding the keys
        compare_cols = set(df1.columns).intersection(set(df2.columns)) - set(KEYS)

    # Outer merge to find matches and missing rows
    merged = df1.merge(df2, on=KEYS, how="outer", suffixes=("__f1", "__f2"), indicator=True)

    diffs: List[dict] = []

    # 1) Rows missing in file2
    only_f1 = merged["_merge"] == "left_only"
    for _, row in merged[only_f1].iterrows():
        diffs.append({
            "state": row["state"],
            "county": row["county"],
            "precinct": row["precinct"],
            "diff_type": "missing_in_file2",
            "column": "",
            "file1_value": "ROW_PRESENT",
            "file2_value": "ROW_MISSING",
            "description": f"Row exists in file1 ({os.path.basename(args.file1)}) but not in file2 ({os.path.basename(args.file2)}).",
        })

    # 2) Rows missing in file1
    only_f2 = merged["_merge"] == "right_only"
    for _, row in merged[only_f2].iterrows():
        diffs.append({
            "state": row["state"],
            "county": row["county"],
            "precinct": row["precinct"],
            "diff_type": "missing_in_file1",
            "column": "",
            "file1_value": "ROW_MISSING",
            "file2_value": "ROW_PRESENT",
            "description": f"Row exists in file2 ({os.path.basename(args.file2)}) but not in file1 ({os.path.basename(args.file1)}).",
        })

    # 3) Rows present in both: compare each selected column
    both = merged["_merge"] == "both"
    both_df = merged[both]

    # Build column-name mapping: col__f1 vs col__f2
    for col in compare_cols:
        col1 = f"{col}__f1"
        col2 = f"{col}__f2"
        if col1 not in both_df.columns or col2 not in both_df.columns:
            # if it's not in both, skip (shouldn't happen because compare_cols is intersection)
            continue

        for _, row in both_df.iterrows():
            v1 = row[col1] if col1 in row else ""
            v2 = row[col2] if col2 in row else ""
            same = compare_values(v1, v2, args.float_tol, args.case_sensitive)
            if not same:
                diffs.append({
                    "state": row["state"],
                    "county": row["county"],
                    "precinct": row["precinct"],
                    "diff_type": "value_mismatch",
                    "column": col,
                    "file1_value": "" if pd.isna(v1) else str(v1),
                    "file2_value": "" if pd.isna(v2) else str(v2),
                    "description": f"Column '{col}' differs (file1 vs file2) "
                                   f"with tol={args.float_tol}, case_sensitive={args.case_sensitive}.",
                })

    out_cols = [
        "state", "county", "precinct",
        "diff_type", "column", "file1_value", "file2_value", "description"
    ]
    out_df = pd.DataFrame(diffs, columns=out_cols)
    out_df.sort_values(by=["state", "county", "precinct", "diff_type", "column"], inplace=True, kind="stable")
    out_df.to_csv(args.out, index=False)

    print(f"Compared {len(both_df):,} shared rows; wrote {len(out_df):,} diffs to {args.out}")

if __name__ == "__main__":
    main()