#!/usr/bin/env python3
"""
Export precinct-level election data to CSV with vote-type-specific columns.

Usage:
  python export_election_precincts.py --db-url postgresql+psycopg://user:pass@host:5432/dbname
  # or set DATABASE_URL env var (preferred) and run without --db-url

Notes:
- Connects read-only to the given PostgreSQL database.
- Interactively guides you to pick: State -> County (or All) -> Election (by name) -> Vote Type.
- Populates only the columns for the selected vote type, leaving the others blank.
- Writes a CSV to the current directory with a descriptive filename.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from typing import List, Optional, Tuple

try:
    import pandas as pd
except Exception as e:
    print("This script requires pandas. Install with: pip install pandas", file=sys.stderr)
    raise

try:
    from sqlalchemy import create_engine, text
except Exception as e:
    print("This script requires SQLAlchemy. Install with: pip install SQLAlchemy", file=sys.stderr)
    raise


CSV_COLUMNS = [
    "state",
    "county",
    "precinct",
    "overall_turnout",
    "ballots_cast",
    "registered_voters",
    "republican_registrations",
    "democrat_registrations",
    "other_registrations",
    "candidate_a_votes_total",
    "candidate_b_votes_total",
    "total_votes",
    "candidate_a_votes_election_day",
    "candidate_b_votes_election_day",
    "total_votes_election_day",
    "candidate_a_votes_early",
    "candidate_b_votes_early",
    "total_votes_early",
    "candidate_a_votes_absentee",
    "candidate_b_votes_absentee",
    "total_votes_absentee",
    "candidate_a_votes_mailin",
    "candidate_b_votes_mailin",
    "total_votes_mailin",
]


VOTE_TYPES = [
    "Total Votes",
    "Election Day Votes",
    "Early Votes",
    "Absentee Votes",
    "Mail In Votes",
]


def guess_vote_type(election_name: str) -> str:
    """Best-effort guess of vote type from the election name string."""
    name = election_name.lower()

    # Order matters: check specific phrases before generic
    # Mail-in variants
    if "mail-in" in name or "mail in" in name or "mail-in votes" in name or "mail " in name and "presidential" in name:
        return "Mail In Votes"
    # Absentee
    if "absentee" in name:
        return "Absentee Votes"
    # Early voting / early votes
    if "early voting" in name or "early votes" in name or "early" in name:
        return "Early Votes"
    # Election Day / In-Person / In Person
    if "election day" in name or "in-person" in name or "in person" in name:
        return "Election Day Votes"
    # All votes / totals
    if "all votes" in name or "all" in name:
        return "Total Votes"

    # Fallbacks
    if "special" in name or "general" in name or "presidential" in name or "senator" in name or "senatorial" in name:
        # default to Total if unclear
        return "Total Votes"

    return "Total Votes"


def prompt_pick(label: str, options: List[str], default_index: Optional[int] = None) -> str:
    """Prompt the user to pick from numbered options. Returns the chosen string."""
    if not options:
        print(f"No options available for {label}. Exiting.", file=sys.stderr)
        sys.exit(2)

    print(f"\n{label}:")
    for i, opt in enumerate(options, start=1):
        dmark = ""
        if default_index is not None and i - 1 == default_index:
            dmark = "  [default]"
        print(f"  {i}. {opt}{dmark}")

    while True:
        raw = input(f"Choose 1–{len(options)}"
                    f"{' (press Enter for default)' if default_index is not None else ''}: ").strip()
        if not raw and default_index is not None:
            return options[default_index]

        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(options):
                return options[idx - 1]

        print("Invalid choice. Please try again.")


def sanitize_filename(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_.-]", "", s)
    return s[:160]  # keep it reasonable


def get_engine(db_url: Optional[str]) -> "Engine":
    if not db_url:
        db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("Error: provide a PostgreSQL URL via --db-url or DATABASE_URL env var.", file=sys.stderr)
        sys.exit(2)
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        print(f"Failed to connect to database: {e}", file=sys.stderr)
        sys.exit(2)


def fetch_distinct_states(engine) -> List[str]:
    sql = text("""
        SELECT DISTINCT state
        FROM public.elections
        WHERE state IS NOT NULL AND state <> ''
        ORDER BY state ASC
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return [r[0] for r in rows]


def fetch_distinct_counties(engine, state: str) -> List[str]:
    sql = text("""
        SELECT DISTINCT county
        FROM public.elections
        WHERE state = :state
          AND county IS NOT NULL AND county <> ''
        ORDER BY county ASC
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"state": state}).fetchall()
    return [r[0] for r in rows]


def fetch_election_names(engine, state: str, county: Optional[str]) -> List[str]:
    if county and county.lower() != "all":
        sql = text("""
            SELECT DISTINCT name
            FROM public.elections
            WHERE state = :state AND county = :county
            ORDER BY name ASC
        """)
        params = {"state": state, "county": county}
    else:
        sql = text("""
            SELECT DISTINCT name
            FROM public.elections
            WHERE state = :state
            ORDER BY name ASC
        """)
        params = {"state": state}

    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [r[0] for r in rows]


def fetch_rows(engine, state: str, county: Optional[str], election_name: str):
    """Fetch joined rows for the selected election(s)."""
    if county and county.lower() != "all":
        where = "e.state = :state AND e.county = :county AND e.name = :ename"
        params = {"state": state, "county": county, "ename": election_name}
    else:
        where = "e.state = :state AND e.name = :ename"
        params = {"state": state, "ename": election_name}

    sql = text(f"""
        SELECT
            e.state,
            e.county,
            ep.precinct,
            ep.turnout_pct,
            ep.total_votes,
            ep.registered_voters,
            ep.republican_registrations,
            ep.democrat_registrations,
            ep.other_registrations,
            ep.candidate_a_votes,
            ep.candidate_b_votes
        FROM public.election_precincts ep
        JOIN public.elections e
          ON e.id = ep.election_id
        WHERE {where}
        ORDER BY e.state ASC, e.county ASC, ep.precinct ASC
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    # Return as list of dicts for easier DataFrame creation
    out = []
    for r in rows:
        out.append(
            {
                "state": r[0],
                "county": r[1],
                "precinct": r[2],
                "overall_turnout": r[3],
                "ballots_cast": r[4],
                "registered_voters": r[5],
                "republican_registrations": r[6],
                "democrat_registrations": r[7],
                "other_registrations": r[8],
                "candidate_a_votes_src": r[9],
                "candidate_b_votes_src": r[10],
                "total_votes_src": r[4],  # same as ballots_cast
            }
        )
    return out


def rows_to_wide_with_vote_type(rows: List[dict], vote_type: str) -> pd.DataFrame:
    """
    Given the base rows and a selected vote_type, expand to the required CSV columns,
    populating only the columns for that vote_type and leaving others blank.
    """
    records = []
    for r in rows:
        base = {
            "state": r["state"],
            "county": r["county"],
            "precinct": r["precinct"],
            "overall_turnout": r["overall_turnout"],
            "ballots_cast": r["ballots_cast"],
            "registered_voters": r["registered_voters"],
            "republican_registrations": r["republican_registrations"],
            "democrat_registrations": r["democrat_registrations"],
            "other_registrations": r["other_registrations"],
            # Always include these "totals" columns from the source row (per spec)
            "candidate_a_votes_total": None,
            "candidate_b_votes_total": None,
            "total_votes": None,
            # Vote-type specific placeholders
            "candidate_a_votes_election_day": None,
            "candidate_b_votes_election_day": None,
            "total_votes_election_day": None,
            "candidate_a_votes_early": None,
            "candidate_b_votes_early": None,
            "total_votes_early": None,
            "candidate_a_votes_absentee": None,
            "candidate_b_votes_absentee": None,
            "total_votes_absentee": None,
            "candidate_a_votes_mailin": None,
            "candidate_b_votes_mailin": None,
            "total_votes_mailin": None,
        }

        if vote_type == "Total Votes":
            base["candidate_a_votes_total"] = r["candidate_a_votes_src"]
            base["candidate_b_votes_total"] = r["candidate_b_votes_src"]
            base["total_votes"] = r["total_votes_src"]
        elif vote_type == "Election Day Votes":
            base["candidate_a_votes_election_day"] = r["candidate_a_votes_src"]
            base["candidate_b_votes_election_day"] = r["candidate_b_votes_src"]
            base["total_votes_election_day"] = r["total_votes_src"]
        elif vote_type == "Early Votes":
            base["candidate_a_votes_early"] = r["candidate_a_votes_src"]
            base["candidate_b_votes_early"] = r["candidate_b_votes_src"]
            base["total_votes_early"] = r["total_votes_src"]
        elif vote_type == "Absentee Votes":
            base["candidate_a_votes_absentee"] = r["candidate_a_votes_src"]
            base["candidate_b_votes_absentee"] = r["candidate_b_votes_src"]
            base["total_votes_absentee"] = r["total_votes_src"]
        elif vote_type == "Mail In Votes":
            base["candidate_a_votes_mailin"] = r["candidate_a_votes_src"]
            base["candidate_b_votes_mailin"] = r["candidate_b_votes_src"]
            base["total_votes_mailin"] = r["total_votes_src"]
        else:
            # Shouldn't happen; leave all type-specific columns blank
            pass

        records.append(base)

    df = pd.DataFrame.from_records(records, columns=CSV_COLUMNS)
    return df


def main():
    parser = argparse.ArgumentParser(description="Export precinct-level election data to CSV with vote-type columns.")
    parser.add_argument("--db-url", help="SQLAlchemy/PostgreSQL URL (or set DATABASE_URL env var)")
    parser.add_argument("--out", help="Output CSV path (optional; default is auto-named)")
    args = parser.parse_args()

    engine = get_engine(args.db_url)

    # 1) Pick State
    states = fetch_distinct_states(engine)
    state = prompt_pick("Select a State (from elections.state)", states)

    # 2) Pick County (or All)
    counties = fetch_distinct_counties(engine, state)
    county_options = ["All"] + counties
    county = prompt_pick(f"Select a County in {state} (or All)", county_options)

    # 3) Pick Election Name
    election_names = fetch_election_names(engine, state, county if county.lower() != "all" else None)
    if not election_names:
        print("No elections found for the chosen scope. Exiting.", file=sys.stderr)
        sys.exit(1)
    default_vote_type_guess = guess_vote_type(election_names[0])  # placeholder until user picks
    # let them pick the election first, then re-guess
    election_name = prompt_pick("Select an Election (by name)", election_names)
    default_vote_type_guess = guess_vote_type(election_name)

    # 4) Pick Vote Type (with default guess)
    vote_type = prompt_pick("Select the Vote Type to populate", VOTE_TYPES, default_index=VOTE_TYPES.index(default_vote_type_guess))

    # 5) Fetch rows and build DataFrame
    rows = fetch_rows(engine, state, county if county.lower() != "all" else None, election_name)
    if not rows:
        print("No precinct rows found for that election selection. Exiting.", file=sys.stderr)
        sys.exit(1)

    df = rows_to_wide_with_vote_type(rows, vote_type)

    # 6) Write CSV
    if args.out:
        out_path = args.out
    else:
        fname = f"{sanitize_filename(state)}__{sanitize_filename(county)}__{sanitize_filename(election_name)}__{sanitize_filename(vote_type)}.csv"
        out_path = os.path.join(os.getcwd(), fname)

    df.to_csv(out_path, index=False)
    print(f"Wrote {len(df):,} rows to: {out_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted by user.", file=sys.stderr)
        sys.exit(130)
