#!/usr/bin/env python3
"""
Combine multiple vote-type CSV exports (from export_election_precincts.py)
into one CSV keyed by (state, county, precinct).

Changes:
- Only registration fields are treated as "shared" and validated for conflicts.
- overall_turnout and ballots_cast are treated as type-dependent; no conflicts raised.
  They are coalesced with priority: total > eday > early > absentee > mailin.

- Writes an error CSV listing conflicts in shared registration fields and omits those rows.
- Auto-names output if --out is omitted using <State>__<County>__<Year>__COMBINED.csv.
- --year can override year detection from filenames.

Usage examples:
  python combine_vote_type_exports.py --mailin mailin.csv --early early.csv --total total.csv
  python combine_vote_type_exports.py --mailin mailin.csv --early early.csv --total total.csv --year 2024
"""

import argparse, os, re, sys
from typing import Dict, List, Tuple
import pandas as pd

CSV_COLUMNS = [
    "state","county","precinct","overall_turnout","ballots_cast",
    "registered_voters","republican_registrations","democrat_registrations","other_registrations",
    "candidate_a_votes_total","candidate_b_votes_total","total_votes",
    "candidate_a_votes_election_day","candidate_b_votes_election_day","total_votes_election_day",
    "candidate_a_votes_early","candidate_b_votes_early","total_votes_early",
    "candidate_a_votes_absentee","candidate_b_votes_absentee","total_votes_absentee",
    "candidate_a_votes_mailin","candidate_b_votes_mailin","total_votes_mailin",
]

ID_COLS = ["state","county","precinct"]

# ONLY these are enforced to be consistent across inputs (conflict => error row)
SHARED_REG_COLS = [
    "registered_voters","republican_registrations","democrat_registrations","other_registrations"
]

# These are type-dependent; we do NOT error on mismatches. We pick by priority (below).
NONSHARED_BASE_COLS = ["overall_turnout","ballots_cast"]

# Vote-type columns present in the per-type CSVs
VTYPE_TO_COLS = {
    "total": ["candidate_a_votes_total","candidate_b_votes_total","total_votes"],
    "eday":  ["candidate_a_votes_election_day","candidate_b_votes_election_day","total_votes_election_day"],
    "early": ["candidate_a_votes_early","candidate_b_votes_early","total_votes_early"],
    "absentee": ["candidate_a_votes_absentee","candidate_b_votes_absentee","total_votes_absentee"],
    "mailin": ["candidate_a_votes_mailin","candidate_b_votes_mailin","total_votes_mailin"],
}

# When picking non-shared base values (turnout/ballots), use this order:
TYPE_PRIORITY = ["total","eday","early","absentee","mailin"]

YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")

def read_one(path:str)->pd.DataFrame:
    df=pd.read_csv(path,dtype=str,keep_default_na=False)
    for c in ID_COLS:
        if c not in df.columns: sys.exit(f"{path}: missing ID column {c}")
        df[c]=df[c].astype(str).str.strip()
    return df

def merge_frames(frames:List[pd.DataFrame])->pd.DataFrame:
    out=None
    for i,df in enumerate(frames):
        out=df if out is None else out.merge(df,on=ID_COLS,how="outer",suffixes=("",f"__dup{i}"))
    return out if out is not None else pd.DataFrame(columns=ID_COLS)

def coalesce_first_nonempty(df:pd.DataFrame, cols:List[str])->pd.Series:
    if not cols: return pd.Series([],dtype=str)
    s=df[cols[0]] if cols[0] in df.columns else pd.Series("", index=df.index, dtype=str)
    for c in cols[1:]:
        if c in df.columns:
            s=s.where(s.astype(str).str.len()>0, df[c])
    return s

def infer_year_from_paths(paths:List[str])->str:
    years=set()
    for p in paths:
        m=YEAR_RE.search(os.path.basename(p))
        if m: years.add(m.group(0))
    if len(years)==1: return next(iter(years))
    if len(years)>1: return "MULTI"
    return "UNKNOWN"

def infer_unique_or_multi(series:pd.Series)->str:
    vals=set(v for v in series.astype(str).str.strip().unique() if v!="")
    if len(vals)==1: return next(iter(vals))
    if len(vals)==0: return "UNKNOWN"
    return "MULTI"

def sanitize_filename(s:str)->str:
    return re.sub(r"[^A-Za-z0-9_.-]","",re.sub(r"\s+","_",s.strip()))[:120]

def main():
    ap=argparse.ArgumentParser(description="Combine vote-type CSVs into one by (state, county, precinct).")
    ap.add_argument("--total",help="CSV for Total Votes")
    ap.add_argument("--eday",help="CSV for Election Day Votes")
    ap.add_argument("--early",help="CSV for Early Votes")
    ap.add_argument("--absentee",help="CSV for Absentee Votes")
    ap.add_argument("--mailin",help="CSV for Mail In Votes")
    ap.add_argument("--out",help="Output CSV path (auto-named if omitted)")
    ap.add_argument("--err-out",help="Error CSV path (defaults to <out>.errors.csv or auto-name + .errors.csv)")
    ap.add_argument("--year",help="Override year for auto-naming")
    ap.add_argument("--fail-on-errors",action="store_true",help="Exit nonzero if conflicts found in registrations")
    args=ap.parse_args()

    # Collect inputs
    type_to_path:Dict[str,str]={}
    for typ in VTYPE_TO_COLS.keys():
        path=getattr(args,typ)
        if path:
            if not os.path.exists(path): sys.exit(f"File not found: {path}")
            type_to_path[typ]=path
    if not type_to_path: sys.exit("No input files provided!")

    # Read + keep only needed cols; rename bases with source type to retain provenance
    per_type=[]
    for typ,path in type_to_path.items():
        df=read_one(path)
        needed=set(ID_COLS + SHARED_REG_COLS + NONSHARED_BASE_COLS + VTYPE_TO_COLS[typ])
        missing=[c for c in VTYPE_TO_COLS[typ] if c not in df.columns]
        if missing: sys.exit(f"{path}: missing expected columns for {typ}: {missing}")
        df=df[[c for c in df.columns if c in needed]].copy()
        # Tag source for shared registration columns
        for col in SHARED_REG_COLS:
            if col in df.columns:
                df.rename(columns={col:f"{col}__from_{typ}"}, inplace=True)
        # Also tag non-shared base columns so we can pick by priority later (no conflicts)
        for col in NONSHARED_BASE_COLS:
            if col in df.columns:
                df.rename(columns={col:f"{col}__from_{typ}"}, inplace=True)
        per_type.append(df)

    merged=merge_frames(per_type)

    # Build conflict rows ONLY for shared registration columns
    conflict_rows=[]
    for col in SHARED_REG_COLS:
        variants=[c for c in merged.columns if c==col or c.startswith(f"{col}__from_") or c.startswith(col+"__dup")]
        if not variants: continue
        vals=merged[variants].replace("",pd.NA)
        nunq=vals.apply(lambda r:r.dropna().astype(str).nunique(),axis=1)
        for idx in merged.index[nunq>1]:
            id_vals={k:merged.at[idx,k] for k in ID_COLS}
            for vcol in variants:
                v=merged.at[idx,vcol]
                if pd.isna(v) or (isinstance(v,str) and not v.strip()): continue
                src_type="unknown"
                for t in VTYPE_TO_COLS.keys():
                    if f"__from_{t}" in vcol: src_type=t; break
                conflict_rows.append({
                    **id_vals,
                    "column": col,
                    "source_type": src_type,
                    "source_column": vcol,
                    "source_file": type_to_path.get(src_type,""),
                    "value": str(v),
                })

    exit_code=0
    if conflict_rows:
        auto_year=args.year or infer_year_from_paths(list(type_to_path.values()))
        auto_state=infer_unique_or_multi(merged["state"]) if "state" in merged else "UNKNOWN"
        auto_county=infer_unique_or_multi(merged["county"]) if "county" in merged else "UNKNOWN"
        auto_base=f"{sanitize_filename(auto_state)}__{sanitize_filename(auto_county)}__{sanitize_filename(auto_year)}__COMBINED"
        err_path=args.err_out or (args.out+".errors.csv" if args.out else f"{auto_base}.errors.csv")
        err_df=pd.DataFrame(conflict_rows,columns=[*ID_COLS,"column","source_type","source_column","source_file","value"])
        err_df.sort_values(by=ID_COLS+["column","source_type"], inplace=True, kind="stable")
        err_df.to_csv(err_path,index=False)
        conflicted=err_df[ID_COLS].drop_duplicates()
        merged=merged.merge(conflicted.assign(__drop=1), on=ID_COLS, how="left")
        dropped=int(merged["__drop"].fillna(0).sum())
        merged=merged[merged["__drop"].isna()].drop(columns="__drop")
        print(f"⚠️ Conflicts in registrations: wrote {len(err_df)} records to {err_path}; "
              f"omitted {dropped} precinct row(s).")
        if args.fail_on_errors: exit_code=2

    # For NONSHARED_BASE_COLS, select value by type priority (no conflicts)
    for base in NONSHARED_BASE_COLS:
        variants=[f"{base}__from_{t}" for t in TYPE_PRIORITY if f"{base}__from_{t}" in merged.columns]
        if not variants:
            merged[base]=""
        else:
            merged[base]=coalesce_first_nonempty(merged, variants).fillna("")
        # drop the variant columns
        for v in variants:
            if v in merged.columns:
                merged.drop(columns=v, inplace=True)

    # Coalesce registration cols now (since we've removed conflicted keys), prefer by type priority too
    for reg in SHARED_REG_COLS:
        variants=[f"{reg}__from_{t}" for t in TYPE_PRIORITY if f"{reg}__from_{t}" in merged.columns]
        if variants:
            merged[reg]=coalesce_first_nonempty(merged, variants).fillna("")
            for v in variants:
                if v in merged.columns: merged.drop(columns=v, inplace=True)
        else:
            merged[reg]=""

    # Ensure vote-type columns exist
    for cols in VTYPE_TO_COLS.values():
        for c in cols:
            if c not in merged.columns:
                merged[c]=""

    # Final order + sort
    merged=merged[CSV_COLUMNS].sort_values(by=ID_COLS, kind="stable")

    # Auto-name output if needed
    if args.out: out_path=args.out
    else:
        auto_state=infer_unique_or_multi(merged["state"]) if "state" in merged else "UNKNOWN"
        auto_county=infer_unique_or_multi(merged["county"]) if "county" in merged else "UNKNOWN"
        auto_year=args.year or infer_year_from_paths(list(type_to_path.values()))
        out_path=f"{sanitize_filename(auto_state)}__{sanitize_filename(auto_county)}__{sanitize_filename(auto_year)}__COMBINED.csv"

    merged.to_csv(out_path,index=False)
    print(f"Wrote {len(merged):,} rows to {out_path}")
    if exit_code: sys.exit(exit_code)

if __name__=="__main__": main()