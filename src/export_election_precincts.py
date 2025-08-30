#!/usr/bin/env python3
"""
Export precinct-level election data to CSV with vote-type-specific columns,
now with an explicit Year filter (picked right after State).
"""

import argparse
import os
import re
import sys
from typing import List, Optional
import pandas as pd
from sqlalchemy import create_engine, text

CSV_COLUMNS = [
    "state","county","precinct","overall_turnout","ballots_cast",
    "registered_voters","republican_registrations","democrat_registrations","other_registrations",
    "candidate_a_votes_total","candidate_b_votes_total","total_votes",
    "candidate_a_votes_election_day","candidate_b_votes_election_day","total_votes_election_day",
    "candidate_a_votes_early","candidate_b_votes_early","total_votes_early",
    "candidate_a_votes_absentee","candidate_b_votes_absentee","total_votes_absentee",
    "candidate_a_votes_mailin","candidate_b_votes_mailin","total_votes_mailin",
]

VOTE_TYPES = ["Total Votes","Election Day Votes","Early Votes","Absentee Votes","Mail In Votes"]

def guess_vote_type(election_name: str) -> str:
    name = election_name.lower()
    if "mail-in" in name or "mail in" in name: return "Mail In Votes"
    if "absentee" in name: return "Absentee Votes"
    if "early" in name: return "Early Votes"
    if "election day" in name or "in-person" in name or "in person" in name: return "Election Day Votes"
    return "Total Votes"

def prompt_pick(label: str, options: List[str], default_index: Optional[int] = None) -> str:
    print(f"\n{label}:")
    for i,opt in enumerate(options, start=1):
        dmark = "  [default]" if default_index is not None and i-1==default_index else ""
        print(f"  {i}. {opt}{dmark}")
    while True:
        raw = input(f"Choose 1–{len(options)}{' (Enter=default)' if default_index is not None else ''}: ").strip()
        if not raw and default_index is not None: return options[default_index]
        if raw.isdigit():
            idx=int(raw)
            if 1<=idx<=len(options): return options[idx-1]
        print("Invalid choice. Try again.")

def sanitize_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]","",re.sub(r"\s+","_",s.strip()))[:160]

def get_engine(db_url: Optional[str]):
    if not db_url: db_url=os.getenv("DATABASE_URL")
    if not db_url: sys.exit("Error: provide DB URL via --db-url or DATABASE_URL")
    engine=create_engine(db_url)
    with engine.connect() as conn: conn.execute(text("SELECT 1"))
    return engine

def fetch_distinct_states(engine):
    sql=text("SELECT DISTINCT state FROM elections WHERE state<>'' ORDER BY state")
    return [r[0] for r in engine.connect().execute(sql)]

def fetch_years(engine,state):
    sql=text("SELECT DISTINCT year FROM elections WHERE state=:s ORDER BY year")
    return [str(r[0]) for r in engine.connect().execute(sql,{"s":state})]

def fetch_counties(engine,state,year):
    sql=text("SELECT DISTINCT county FROM elections WHERE state=:s AND year=:y AND county<>'' ORDER BY county")
    return [r[0] for r in engine.connect().execute(sql,{"s":state,"y":int(year)})]

def fetch_elections(engine,state,year,county):
    params={"s":state,"y":int(year)}
    if county.lower()!="all":
        sql=text("SELECT DISTINCT name FROM elections WHERE state=:s AND year=:y AND county=:c ORDER BY name")
        params["c"]=county
    else:
        sql=text("SELECT DISTINCT name FROM elections WHERE state=:s AND year=:y ORDER BY name")
    return [r[0] for r in engine.connect().execute(sql,params)]

def fetch_rows(engine,state,year,county,election):
    params={"s":state,"y":int(year),"ename":election}
    where="e.state=:s AND e.year=:y AND e.name=:ename"
    if county.lower()!="all":
        where+=" AND e.county=:c"; params["c"]=county
    sql=text(f"""SELECT e.state,e.county,ep.precinct,ep.turnout_pct,ep.total_votes,
                        ep.registered_voters,ep.republican_registrations,ep.democrat_registrations,ep.other_registrations,
                        ep.candidate_a_votes,ep.candidate_b_votes
                 FROM election_precincts ep
                 JOIN elections e ON e.id=ep.election_id
                 WHERE {where}
                 ORDER BY e.state,e.county,ep.precinct""")
    return [dict(state=r[0],county=r[1],precinct=r[2],overall_turnout=r[3],
                 ballots_cast=r[4],registered_voters=r[5],republican_registrations=r[6],
                 democrat_registrations=r[7],other_registrations=r[8],
                 candidate_a_votes_src=r[9],candidate_b_votes_src=r[10],total_votes_src=r[4])
            for r in engine.connect().execute(sql,params)]

def rows_to_wide(rows,vote_type):
    recs=[]
    for r in rows:
        base={c:None for c in CSV_COLUMNS}
        base.update({k:r[k] for k in ["state","county","precinct","overall_turnout","ballots_cast",
                                      "registered_voters","republican_registrations","democrat_registrations","other_registrations"]})
        if vote_type=="Total Votes":
            base.update(candidate_a_votes_total=r["candidate_a_votes_src"],candidate_b_votes_total=r["candidate_b_votes_src"],total_votes=r["total_votes_src"])
        elif vote_type=="Election Day Votes":
            base.update(candidate_a_votes_election_day=r["candidate_a_votes_src"],candidate_b_votes_election_day=r["candidate_b_votes_src"],total_votes_election_day=r["total_votes_src"])
        elif vote_type=="Early Votes":
            base.update(candidate_a_votes_early=r["candidate_a_votes_src"],candidate_b_votes_early=r["candidate_b_votes_src"],total_votes_early=r["total_votes_src"])
        elif vote_type=="Absentee Votes":
            base.update(candidate_a_votes_absentee=r["candidate_a_votes_src"],candidate_b_votes_absentee=r["candidate_b_votes_src"],total_votes_absentee=r["total_votes_src"])
        elif vote_type=="Mail In Votes":
            base.update(candidate_a_votes_mailin=r["candidate_a_votes_src"],candidate_b_votes_mailin=r["candidate_b_votes_src"],total_votes_mailin=r["total_votes_src"])
        recs.append(base)
    return pd.DataFrame.from_records(recs,columns=CSV_COLUMNS)

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--db-url"); p.add_argument("--out")
    a=p.parse_args()
    eng=get_engine(a.db_url)
    state=prompt_pick("Select State",fetch_distinct_states(eng))
    years=fetch_years(eng,state)
    year=prompt_pick("Select Year",years,0 if len(years)==1 else len(years)-1)
    county=prompt_pick("Select County",["All"]+fetch_counties(eng,state,year))
    election=prompt_pick("Select Election",fetch_elections(eng,state,year,county))
    vt=prompt_pick("Vote Type",VOTE_TYPES,VOTE_TYPES.index(guess_vote_type(election)))
    rows=fetch_rows(eng,state,year,county,election)
    if not rows: sys.exit("No rows found")
    df=rows_to_wide(rows,vt)
    out=a.out or f"{sanitize_filename(state)}__{sanitize_filename(county)}__{sanitize_filename(year)}__{sanitize_filename(vt)}.csv"
    df.to_csv(out,index=False); print(f"Wrote {len(df)} rows to {out}")

if __name__=="__main__": main()