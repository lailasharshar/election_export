#!/usr/bin/env python3
"""
Streamlit UI for exporting precinct-level election data back to the original wide format,
with per-run mapping overrides, selective race inclusion, and a diff tool.

Key behaviors:
- Uses only DATABASE_URL env var (no input box).
- ballots_cast column is present but intentionally left blank in exports.
- UI: map election.name -> vote type (overrides apply for this run only).
- UI: pick a subset of election names to include (e.g., only Presidential races).
- Diff: treats numeric zero as equal to blank; excludes ballots_cast from comparisons.
- Softer theme and wider preview table.

Run:
  export DATABASE_URL='postgresql://user:pass@host:5432/dbname'
  streamlit run streamlit_app.py
"""

import os
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass  # optional

# ---------- Page / Style ----------
st.set_page_config(page_title="Precinct Exporter & Diff", layout="wide")

def _inject_soft_theme():
    st.markdown(
        """
        <style>
        /* Softer background + header */
        .stApp { background-color: #f7f8fc; }
        [data-testid="stHeader"] { background-color: #f7f8fc; }
        div[data-testid="stToolbar"] { background-color: #f7f8fc; }

        /* Buttons and download buttons - soft indigo */
        .stButton > button, .stDownloadButton > button {
            background-color: #6b7fd1 !important;
            border: 1px solid #6b7fd1 !important;
            color: #ffffff !important;
            border-radius: 6px !important;
        }
        .stButton > button:hover, .stDownloadButton > button:hover {
            background-color: #5f72c1 !important;
            border-color: #5f72c1 !important;
        }

        /* Expander headers */
        details > summary {
            background-color: #eef1fb !important;
            border-radius: 6px !important;
            padding: 6px 10px !important;
        }

        /* Dataframe container subtle border */
        div[data-testid="stDataFrame"] {
            border: 1px solid #e6e9f3;
            border-radius: 6px;
            padding: 4px;
            background-color: #ffffff;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

_inject_soft_theme()

VTYPE_ORDER = ["Total Votes", "Election Day Votes", "Early Votes", "Absentee Votes", "Mail In Votes"]
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")

# ---------- DB helpers ----------
def get_engine(db_url: Optional[str]) -> Engine:
    if not db_url:
        db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        st.error("DATABASE_URL env var is required. Set it before launching.")
        st.stop()
    eng = create_engine(db_url, pool_pre_ping=True)
    # sanity ping
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))
    return eng

@st.cache_data(show_spinner=False)
def list_states(db_url: str) -> List[str]:
    eng = get_engine(db_url)
    sql = text("SELECT DISTINCT state FROM public.elections WHERE state IS NOT NULL AND state <> '' ORDER BY state")
    with eng.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return [r[0] for r in rows]

@st.cache_data(show_spinner=False)
def list_years(db_url: str, state: str) -> List[int]:
    eng = get_engine(db_url)
    sql = text("SELECT DISTINCT year FROM public.elections WHERE state = :s ORDER BY year")
    with eng.connect() as conn:
        rows = conn.execute(sql, {"s": state}).fetchall()
    return [int(r[0]) for r in rows]

@st.cache_data(show_spinner=False)
def list_counties(db_url: str, state: str, year: int) -> List[str]:
    eng = get_engine(db_url)
    sql = text("""
        SELECT DISTINCT county
        FROM public.elections
        WHERE state = :s AND year = :y
          AND county IS NOT NULL AND county <> ''
        ORDER BY county
    """)
    with eng.connect() as conn:
        rows = conn.execute(sql, {"s": state, "y": int(year)}).fetchall()
    return [r[0] for r in rows]

def guess_vote_type(name: str) -> str:
    n = name.lower()
    if "mail-in" in n or "mail in" in n: return "Mail In Votes"
    if "absentee" in n: return "Absentee Votes"
    if "early" in n: return "Early Votes"
    if "election day" in n or "in-person" in n or "in person" in n: return "Election Day Votes"
    if "all votes" in n or "total" in n: return "Total Votes"
    return "Total Votes"

@st.cache_data(show_spinner=False)
def list_election_names_with_suggestion(
        db_url: str, state: str, year: int, county_or_all: str
) -> Tuple[pd.DataFrame, str]:
    """
    Return (df, source), where df has columns [election_name, suggested_type, vote_type]
    and source is "map" if vote_type_map was used, else "heuristic".
    Gracefully falls back to heuristic if the mapping table doesn't exist or errors.
    """
    eng = get_engine(db_url)
    county = None if (county_or_all is None or county_or_all.strip().lower() == "all") else county_or_all.strip()

    params = {"s": state, "y": int(year)}
    if county:
        params["c"] = county

    # ----- Try mapping table first -----
    where_map = "e.state = :s AND e.year = :y" + (" AND e.county = :c" if county else "")
    sql_map = f"""
        SELECT DISTINCT
            e.name AS election_name,
            COALESCE(
              (
                SELECT m.vote_type
                FROM public.vote_type_map m
                WHERE (m.state  IS NULL OR m.state  = e.state)
                  AND (m.year   IS NULL OR m.year   = e.year)
                  AND (m.county IS NULL OR m.county = e.county)
                  AND e.name ILIKE m.pattern
                ORDER BY m.priority ASC
                LIMIT 1
              ),
              'Total Votes'
            ) AS suggested_type
        FROM public.elections e
        WHERE {where_map}
        ORDER BY 1
    """
    try:
        with eng.connect() as conn:
            df = pd.read_sql(sql=text(sql_map), con=conn, params=params)
        df["vote_type"] = df["suggested_type"]
        return df[["election_name", "suggested_type", "vote_type"]], "map"
    except Exception:
        # ----- Fallback: heuristic (ALSO aliased as e) -----
        where_simple = "e.state = :s AND e.year = :y" + (" AND e.county = :c" if county else "")
        sql_names = f"""
            SELECT DISTINCT e.name AS election_name
            FROM public.elections e
            WHERE {where_simple}
            ORDER BY 1
        """
        with eng.connect() as conn:
            names = pd.read_sql(sql=text(sql_names), con=conn, params=params)
        names["suggested_type"] = names["election_name"].apply(guess_vote_type)
        names["vote_type"] = names["suggested_type"]
        return names[["election_name", "suggested_type", "vote_type"]], "heuristic"

def build_name_filter_clause(names: Optional[List[str]], params: Dict[str, object]) -> str:
    """Return ' AND e.name IN (:nm_0, :nm_1, ...)' and populate params accordingly, or '' if no filter."""
    if not names:
        return ""
    placeholders = []
    for i, nm in enumerate(names):
        key = f"nm_{i}"
        params[key] = nm
        placeholders.append(f":{key}")
    return " AND e.name IN (" + ", ".join(placeholders) + ")"

def build_combined_query(
        state: str,
        year: int,
        county_or_all: str,
        overrides: Dict[str, str],
        use_map: bool,
        include_names: Optional[List[str]],
) -> Tuple[str, Dict]:
    """
    Build SQL + params to produce the combined export, with optional per-run overrides
    of vote types by election name. ballots_cast is blanked after the query.
    Filters to only the selected election names if provided.
    """
    county = None if (county_or_all is None or county_or_all.strip().lower() == "all") else county_or_all.strip()
    params: Dict[str, object] = {"state": state, "year": int(year)}

    where = "e.state = :state AND e.year = :year"
    if county:
        where += " AND e.county = :county"
        params["county"] = county
    where += build_name_filter_clause(include_names, params)

    # CTE 1: scoped_base
    if use_map:
        scoped_base_cte = f"""
        scoped_base AS (
          SELECT
            e.id, e.state, e.county, e.year, e.name,
            COALESCE(vtm.vote_type, 'Total Votes') AS vote_type_base
          FROM public.elections e
          LEFT JOIN LATERAL (
            SELECT m.vote_type
            FROM public.vote_type_map m
            WHERE (m.state  IS NULL OR m.state  = e.state)
              AND (m.year   IS NULL OR m.year   = e.year)
              AND (m.county IS NULL OR m.county = e.county)
              AND e.name ILIKE m.pattern
            ORDER BY m.priority ASC
            LIMIT 1
          ) vtm ON TRUE
          WHERE {where}
        )"""
    else:
        scoped_base_cte = f"""
        scoped_base AS (
          SELECT
            e.id, e.state, e.county, e.year, e.name,
            CASE
              WHEN lower(e.name) LIKE '%mail-in%' OR lower(e.name) LIKE '%mail in%' THEN 'Mail In Votes'
              WHEN lower(e.name) LIKE '%absentee%' THEN 'Absentee Votes'
              WHEN lower(e.name) LIKE '%early%' THEN 'Early Votes'
              WHEN lower(e.name) LIKE '%election day%' OR lower(e.name) LIKE '%in-person%' OR lower(e.name) LIKE '%in person%' THEN 'Election Day Votes'
              WHEN lower(e.name) LIKE '%all votes%' OR lower(e.name) LIKE '%total%' THEN 'Total Votes'
              ELSE 'Total Votes'
            END AS vote_type_base
          FROM public.elections e
          WHERE {where}
        )"""

    # CTE 2: overrides (optional)
    overrides_cte = ""
    if overrides:
        values_sql = []
        for i, (name, vtype) in enumerate(overrides.items()):
            params[f"ov_name_{i}"] = name
            params[f"ov_type_{i}"] = vtype
            values_sql.append(f"(:ov_name_{i}, :ov_type_{i})")
        overrides_cte = f"overrides(name, vote_type) AS (VALUES {', '.join(values_sql)})"

    # CTE 3: scoped -> apply overrides
    if overrides_cte:
        scoped_cte = """
        scoped AS (
          SELECT sb.id, sb.state, sb.county, sb.year, sb.name,
                 COALESCE(o.vote_type, sb.vote_type_base) AS vote_type
          FROM scoped_base sb
          LEFT JOIN overrides o ON o.name = sb.name
        )"""
    else:
        scoped_cte = """
        scoped AS (
          SELECT sb.id, sb.state, sb.county, sb.year, sb.name, sb.vote_type_base AS vote_type
          FROM scoped_base sb
        )"""

    with_clause = "WITH " + ",\n      ".join(
        [scoped_base_cte.strip()] + ([overrides_cte.strip()] if overrides_cte else []) + [scoped_cte.strip()]
    )

    sql = f"""
    {with_clause}
    SELECT
      s.state,
      s.county,
      ep.precinct,
      COALESCE(
        MAX(CASE WHEN s.vote_type='Total Votes' THEN ep.turnout_pct END),
        MAX(CASE WHEN s.vote_type='Election Day Votes' THEN ep.turnout_pct END),
        MAX(CASE WHEN s.vote_type='Early Votes' THEN ep.turnout_pct END),
        MAX(CASE WHEN s.vote_type='Absentee Votes' THEN ep.turnout_pct END),
        MAX(CASE WHEN s.vote_type='Mail In Votes' THEN ep.turnout_pct END)
      ) AS overall_turnout,
      -- ballots_cast intentionally omitted (blanked later)
      MAX(ep.registered_voters)         AS registered_voters,
      MAX(ep.republican_registrations)  AS republican_registrations,
      MAX(ep.democrat_registrations)    AS democrat_registrations,
      MAX(ep.other_registrations)       AS other_registrations,
      MAX(CASE WHEN s.vote_type='Total Votes' THEN ep.candidate_a_votes END) AS candidate_a_votes_total,
      MAX(CASE WHEN s.vote_type='Total Votes' THEN ep.candidate_b_votes END) AS candidate_b_votes_total,
      MAX(CASE WHEN s.vote_type='Total Votes' THEN ep.total_votes        END) AS total_votes,
      MAX(CASE WHEN s.vote_type='Election Day Votes' THEN ep.candidate_a_votes END) AS candidate_a_votes_election_day,
      MAX(CASE WHEN s.vote_type='Election Day Votes' THEN ep.candidate_b_votes END) AS candidate_b_votes_election_day,
      MAX(CASE WHEN s.vote_type='Election Day Votes' THEN ep.total_votes        END) AS total_votes_election_day,
      MAX(CASE WHEN s.vote_type='Early Votes' THEN ep.candidate_a_votes END) AS candidate_a_votes_early,
      MAX(CASE WHEN s.vote_type='Early Votes' THEN ep.candidate_b_votes END) AS candidate_b_votes_early,
      MAX(CASE WHEN s.vote_type='Early Votes' THEN ep.total_votes        END) AS total_votes_early,
      MAX(CASE WHEN s.vote_type='Absentee Votes' THEN ep.candidate_a_votes END) AS candidate_a_votes_absentee,
      MAX(CASE WHEN s.vote_type='Absentee Votes' THEN ep.candidate_b_votes END) AS candidate_b_votes_absentee,
      MAX(CASE WHEN s.vote_type='Absentee Votes' THEN ep.total_votes        END) AS total_votes_absentee,
      MAX(CASE WHEN s.vote_type='Mail In Votes' THEN ep.candidate_a_votes END) AS candidate_a_votes_mailin,
      MAX(CASE WHEN s.vote_type='Mail In Votes' THEN ep.candidate_b_votes END) AS candidate_b_votes_mailin,
      MAX(CASE WHEN s.vote_type='Mail In Votes' THEN ep.total_votes        END) AS total_votes_mailin
    FROM scoped s
    JOIN public.election_precincts ep ON ep.election_id = s.id
    GROUP BY s.state, s.county, ep.precinct
    ORDER BY s.state, s.county, ep.precinct
    """
    return sql, params

@st.cache_data(show_spinner=True)
def fetch_combined(
        db_url: str,
        state: str,
        year: int,
        county_or_all: str,
        overrides: Dict[str, str],
        include_names: Optional[List[str]],
) -> pd.DataFrame:
    eng = get_engine(db_url)

    # Try with mapping table; on error, fallback to heuristic
    for attempt in (0, 1):
        use_map = (attempt == 0)
        sql, params = build_combined_query(state, int(year), county_or_all, overrides, use_map, include_names)
        try:
            with eng.connect() as conn:
                df = pd.read_sql(sql=text(sql), con=conn, params=params)
            break
        except Exception:
            if attempt == 0:
                continue
            raise

    # Ensure required columns & order; blank ballots_cast
    cols = [
        "state","county","precinct","overall_turnout","ballots_cast",
        "registered_voters","republican_registrations","democrat_registrations","other_registrations",
        "candidate_a_votes_total","candidate_b_votes_total","total_votes",
        "candidate_a_votes_election_day","candidate_b_votes_election_day","total_votes_election_day",
        "candidate_a_votes_early","candidate_b_votes_early","total_votes_early",
        "candidate_a_votes_absentee","candidate_b_votes_absentee","total_votes_absentee",
        "candidate_a_votes_mailin","candidate_b_votes_mailin","total_votes_mailin",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df["ballots_cast"] = ""  # per spec
    df = df[cols]
    return df

# ---------- Diff helpers ----------
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

def values_equal(v1: str, v2: str, float_tol: float, case_sensitive: bool) -> bool:
    """
    Equality rules:
    - Case-insensitive by default.
    - If one side is blank and the other is a numeric zero (e.g., '0', '0.0'), treat as equal.
    - If both numeric, compare within float_tol.
    - Else compare normalized strings.
    """
    n1 = normalize_value(v1, case_sensitive)
    n2 = normalize_value(v2, case_sensitive)

    # Blank vs blank
    if n1 == "" and n2 == "":
        return True

    # Blank vs numeric zero
    f1, f2 = try_float(n1), try_float(n2)
    if (n1 == "" and f2 is not None and abs(f2) <= float_tol) or (n2 == "" and f1 is not None and abs(f1) <= float_tol):
        return True

    # Exact match after normalization
    if n1 == n2:
        return True

    # Numeric with tolerance
    if f1 is not None and f2 is not None:
        return abs(f1 - f2) <= float_tol

    return False

def diff_dataframes(
        df1: pd.DataFrame, df2: pd.DataFrame, compare_cols: Optional[List[str]],
        float_tol: float, case_sensitive: bool
) -> pd.DataFrame:
    KEYS = ["state","county","precinct"]
    for k in KEYS:
        if k not in df1.columns or k not in df2.columns:
            raise ValueError(f"Missing key column '{k}' in one of the files.")
    for k in KEYS:
        df1[k] = df1[k].astype(str).str.strip()
        df2[k] = df2[k].astype(str).str.strip()

    if compare_cols is None:
        compare_cols = list((set(df1.columns) & set(df2.columns)) - set(KEYS))
    # Remove ballots_cast from comparison per request
    compare_cols = [c for c in compare_cols if c != "ballots_cast"]
    compare_cols = sorted(compare_cols)

    merged = df1.merge(df2, on=KEYS, how="outer", suffixes=("__f1","__f2"), indicator=True)
    diffs = []

    only_f1 = merged["_merge"] == "left_only"
    for _, row in merged[only_f1].iterrows():
        diffs.append({
            "state": row["state"], "county": row["county"], "precinct": row["precinct"],
            "diff_type": "missing_in_file2", "column": "",
            "file1_value": "ROW_PRESENT", "file2_value": "ROW_MISSING",
            "description": "Row exists in file1 but not in file2."
        })

    only_f2 = merged["_merge"] == "right_only"
    for _, row in merged[only_f2].iterrows():
        diffs.append({
            "state": row["state"], "county": row["county"], "precinct": row["precinct"],
            "diff_type": "missing_in_file1", "column": "",
            "file1_value": "ROW_MISSING", "file2_value": "ROW_PRESENT",
            "description": "Row exists in file2 but not in file1."
        })

    both_df = merged[merged["_merge"] == "both"]

    for col in compare_cols:
        c1, c2 = f"{col}__f1", f"{col}__f2"
        if c1 not in both_df.columns or c2 not in both_df.columns:
            continue
        for _, row in both_df.iterrows():
            v1 = "" if pd.isna(row[c1]) else str(row[c1])
            v2 = "" if pd.isna(row[c2]) else str(row[c2])
            if not values_equal(v1, v2, float_tol=float_tol, case_sensitive=case_sensitive):
                diffs.append({
                    "state": row["state"], "county": row["county"], "precinct": row["precinct"],
                    "diff_type": "value_mismatch", "column": col,
                    "file1_value": v1, "file2_value": v2,
                    "description": f"Column '{col}' differs (file1 vs file2)."
                })

    out = pd.DataFrame(diffs, columns=["state","county","precinct","diff_type","column","file1_value","file2_value","description"])
    out.sort_values(by=["state","county","precinct","diff_type","column"], inplace=True, kind="stable")
    return out

# ---------- UI ----------
st.title("Precinct Exporter & Diff")
st.caption("Select State → Year → County, map/choose races, export combined CSV, and (optionally) diff against an original file.")

db_url = os.getenv("DATABASE_URL", "").strip()
if not db_url:
    st.error("DATABASE_URL env var is required. Set it before launching.")
    st.stop()

states = list_states(db_url)
if not states:
    st.warning("No states found in `public.elections`.")
    st.stop()

state = st.selectbox("State", states, index=0)
years = list_years(db_url, state)
if not years:
    st.warning("No years found for that state.")
    st.stop()

year_idx = len(years)-1 if len(years) > 1 else 0
year = st.selectbox("Year", years, index=year_idx)
counties = ["All"] + list_counties(db_url, state, year)
county = st.selectbox("County", counties, index=0)

# Mapping editor
st.subheader("Map election names to vote types (optional)")
st.caption("If a mapping table isn't found, I'll use a heuristic to suggest the type. Your changes below override for this export only.")
map_df, map_source = list_election_names_with_suggestion(db_url, state, year, county)
st.caption(f"Suggestions source: {'public.vote_type_map' if map_source == 'map' else 'heuristic'}")

edited_df = st.data_editor(
    map_df,
    hide_index=True,
    column_config={
        "election_name": st.column_config.TextColumn("Election Name", disabled=True, width="large"),
        "suggested_type": st.column_config.TextColumn("Suggested", disabled=True),
        "vote_type": st.column_config.SelectboxColumn("Vote Type", options=VTYPE_ORDER, required=True),
    },
    use_container_width=True,
    num_rows="fixed",
)

# Choose which election names to include (e.g., only Presidential)
all_names = edited_df["election_name"].tolist()
include_names = st.multiselect(
    "Elections to include in export & diff",
    options=all_names,
    default=all_names,
    help="Pick a subset if you only want, say, the Presidential races (e.g., 3 of 6)."
)

# Build overrides only for included names
overrides: Dict[str, str] = {}
included_set = set(include_names)
for _, r in edited_df.iterrows():
    ename = str(r["election_name"])
    if ename in included_set and r["vote_type"] != r["suggested_type"]:
        overrides[ename] = str(r["vote_type"])

# Preview & Export (wider preview)
col_prev, col_export = st.columns([3, 1])
with col_prev:
    if st.button("Preview (first 100 rows)"):
        df_preview = fetch_combined(db_url, state, year, county, overrides, include_names)
        st.write(f"Rows: {len(df_preview):,}")
        st.dataframe(df_preview.head(100), use_container_width=True)

with col_export:
    df_export = fetch_combined(db_url, state, year, county, overrides, include_names)
    fname = f"{state}__{county}__{year}.csv"
    st.download_button("Export CSV", data=df_export.to_csv(index=False), file_name=fname, mime="text/csv")

st.markdown("---")
st.subheader("Diff against original imported file (path on disk)")
st.caption("Enter a local path to your original CSV (e.g., /Users/you/Downloads/original.csv or C:\\\\path\\\\file.csv).")
orig_path = st.text_input("Original CSV path")
float_tol = st.number_input("Numeric tolerance", min_value=0.0, max_value=100000.0, value=0.0, step=0.1)
case_sensitive = st.checkbox("Case-sensitive string compare", value=False)

compare_cols = None
if st.button("Run Diff", type="primary"):
    if not orig_path or not os.path.isfile(orig_path):
        st.error("Original CSV path not found. Please enter a valid local file path.")
    else:
        df_combined_for_diff = fetch_combined(db_url, state, year, county, overrides, include_names)
        df_original = pd.read_csv(orig_path, dtype=str, keep_default_na=False)
        shared_cols = sorted(list((set(df_combined_for_diff.columns) & set(df_original.columns)) - {"state","county","precinct","ballots_cast"}))
        with st.expander("Choose columns to compare (optional)"):
            chosen = st.multiselect("Columns", options=shared_cols, default=shared_cols, key="compare_cols_picker")
            compare_cols = chosen if chosen else shared_cols

        # Run immediately with chosen (or default) columns
        diffs = diff_dataframes(df_original, df_combined_for_diff, compare_cols or None, float_tol=float_tol, case_sensitive=case_sensitive)
        st.write(f"Found **{len(diffs):,}** difference records.")
        st.dataframe(diffs.head(200), use_container_width=True)
        st.download_button(
            "Download Differences CSV",
            data=diffs.to_csv(index=False),
            file_name=f"differences__{state}__{county}__{year}.csv",
            mime="text/csv",
        )