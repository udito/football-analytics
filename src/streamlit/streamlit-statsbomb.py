from __future__ import annotations
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import boto3
import os
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Udi Toledano Football Site", layout="wide")

# -----------------------------
# DB helpers
# -----------------------------

def get_conn_url():
    ssm = boto3.client("ssm", region_name=os.environ.get("AWS_REGION", "us-west-1"))
    param = ssm.get_parameter(Name="/football/DATABASE_URL", WithDecryption=True)
    DATABASE_URL = param["Parameter"]["Value"]
    return DATABASE_URL

@st.cache_resource(show_spinner=False)
def get_engine():
    return create_engine(get_conn_url(), pool_pre_ping=True)

@st.cache_data(ttl=600, show_spinner=False)
def sql_df(q: str, params: dict | None = None) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        return pd.read_sql(text(q), conn, params=params)

@st.cache_data(ttl=600)
def list_columns(table: str) -> set[str]:
    q = """
    SELECT column_name FROM information_schema.columns
    WHERE table_schema='public' AND table_name = :t
    """
    return set(sql_df(q, {"t": table})["column_name"].tolist())

# -----------------------------
# Load competitions/matches
# -----------------------------

@st.cache_data(ttl=600)
def load_matches() -> pd.DataFrame:
    q = """
    SELECT match_id, competition_id, season_id, match_date, home_team, away_team
    FROM public.matches
    ORDER BY match_date, match_id
    """
    df = sql_df(q)
    df["match_label"] = df["match_date"].astype(str) + " — " + df["home_team"] + " vs " + df["away_team"]
    return df

matches = load_matches()

@st.cache_data(ttl=600)
def comp_map() -> pd.DataFrame:
    q = """
    SELECT DISTINCT competition_id, season_id, competition_name, season_name
    FROM public.competitions
    ORDER BY competition_name, season_name
    """
    return sql_df(q)

cm = comp_map()

# Resolve competition/season names for UI
m = matches.merge(cm, on=["competition_id", "season_id"], how="left")
competitions = sorted(m["competition_name"].dropna().unique().tolist())
comp = st.sidebar.selectbox("Competition", competitions)
seasons = sorted(m.loc[m["competition_name"] == comp, "season_name"].dropna().unique().tolist())
season = st.sidebar.selectbox("Season", seasons)

team_candidates = sorted(pd.unique(pd.concat([
    m.loc[(m["competition_name"]==comp)&(m["season_name"]==season), ["home_team","away_team"]].stack()
]))).tolist()
team = st.sidebar.selectbox("Team (focus)", team_candidates)

team_matches = m[(m["competition_name"]==comp)&(m["season_name"]==season)&((m["home_team"]==team)|(m["away_team"]==team))].copy()
label_to_id = {row["match_label"]: int(row["match_id"]) for _, row in team_matches.iterrows()}
match_label = st.sidebar.selectbox("Match", list(label_to_id.keys()))
match_id = label_to_id[match_label]

# -----------------------------
# Events (minimal schema)
# -----------------------------

@st.cache_data(ttl=600)
def load_events(match_id: int) -> pd.DataFrame:
    q = """
    SELECT match_id, index, timestamp, type
    FROM public.events
    WHERE match_id = :mid
    ORDER BY index
    """
    df = sql_df(q, {"mid": match_id})
    # minute/second extraction from timestamp "MM:SS.mmm" if present, otherwise ordinal index buckets
    # StatsBomb timestamp usually "00:12:34.567" (HH:MM:SS.sss). We'll parse seconds.
    def to_sec(ts: str) -> float:
        try:
            hh, mm, ss = ts.split(":")
            return int(hh)*3600 + int(mm)*60 + float(ss)
        except Exception:
            return np.nan
    df["sec"] = df["timestamp"].apply(lambda x: to_sec(x) if isinstance(x, str) else np.nan)
    df["minute"] = np.floor(df["sec"]/60.0).astype("Int64")
    return df

events = load_events(match_id)

st.title("📊 StatsBomb RDS Explorer")
st.caption(f"{comp} · {season} · {match_label}")

# Tabs
T1, T2, T3, T4 = st.tabs(["Event timeline", "Type distribution", "Lineups", "Raw events"])

# -----------------------------
# Tab 1: Event timeline (stacked by type)
# -----------------------------
with T1:
    if events.empty:
        st.info("No events found for this match.")
    else:
        # Minute buckets; fallback to index buckets if timestamp missing
        if events["minute"].isna().all():
            events["bucket"] = (events["index"]//10).astype(int)  # every 10 indexes as a crude progression
            xlab = "Index buckets (×10)"
        else:
            events["bucket"] = events["minute"].fillna(0).astype(int)
            xlab = "Minute"
        gp = events.groupby(["bucket", "type"]).size().reset_index(name="count")
        fig = px.area(gp, x="bucket", y="count", color="type", groupnorm=None, title="Events over time (stacked)")
        fig.update_layout(xaxis_title=xlab, yaxis_title="Events")
        st.plotly_chart(fig, use_container_width=True)

# -----------------------------
# Tab 2: Type distribution & per-90 rates
# -----------------------------
with T2:
    if events.empty:
        st.info("No events found for this match.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            ct = events["type"].value_counts().reset_index()
            ct.columns = ["type", "count"]
            fig = px.bar(ct, x="type", y="count", title="Event counts by type")
            fig.update_layout(xaxis_title="Type", yaxis_title="Count")
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            # crude minutes estimate = last minute bucket if parsed; else 90
            if events["minute"].isna().all():
                minutes = 90
            else:
                minutes = max(1, int(events["minute"].dropna().max())+1)
            per90 = ct.copy()
            per90["per90"] = per90["count"] * 90.0 / minutes
            fig2 = px.bar(per90, x="type", y="per90", title=f"Per-90 rates (assumed {minutes} minutes)")
            fig2.update_layout(xaxis_title="Type", yaxis_title="per90")
            st.plotly_chart(fig2, use_container_width=True)

# -----------------------------
# Tab 3: Lineups
# -----------------------------
with T3:
    @st.cache_data(ttl=600)
    def load_lineups(mid: int) -> pd.DataFrame:
        q = """
        SELECT match_id, team_name, player_name
        FROM public.lineups
        WHERE match_id = :mid
        ORDER BY team_name, player_name
        """
        return sql_df(q, {"mid": mid})

    LU = load_lineups(match_id)
    if LU.empty:
        st.info("No lineups found for this match.")
    else:
        tnames = LU["team_name"].dropna().unique().tolist()
        cols = st.columns(2)
        for i, tname in enumerate(tnames[:2]):
            with cols[i]:
                st.subheader(tname)
                st.dataframe(LU[LU["team_name"] == tname][["player_name"]].reset_index(drop=True), use_container_width=True)

# -----------------------------
# Tab 4: Raw events table
# -----------------------------
with T4:
    st.dataframe(events, use_container_width=True, hide_index=True)

# -----------------------------
# Optional: Auto-detect enriched events view
# -----------------------------
cols = list_columns("events_enriched")
if {"match_id","type_name","team_name","player_name","x","y"}.issubset(cols):
    st.markdown("---")
    st.header("Enriched visualizations (from events_enriched view)")
    st.caption("Detected a view named `events_enriched` with locations/xG. Rendering shot map…")
    @st.cache_data(ttl=600)
    def load_enriched(mid: int) -> pd.DataFrame:
        q = """
        SELECT * FROM public.events_enriched WHERE match_id = :mid
        """
        return sql_df(q, {"mid": mid})
    E = load_enriched(match_id)
    shots = E[E["type_name"]=="Shot"].copy()
    if not shots.empty:
        fig = go.Figure()
        fig.add_shape(type="rect", x0=0, y0=0, x1=120, y1=80)
        fig.add_trace(go.Scatter(x=shots["x"], y=shots["y"], mode="markers",
                                 text=[f"xG {x:.2f}" if not pd.isna(x) else "xG n/a" for x in shots.get("shot_xg", 0)],
                                 hovertemplate="x=%{x:.1f}, y=%{y:.1f}<br>%{text}<extra></extra>"))
        fig.update_xaxes(range=[0,120], visible=False)
        fig.update_yaxes(range=[0,80], visible=False, scaleanchor="x", scaleratio=1)
        fig.update_layout(height=520, title="Shot Map (from events_enriched)")
        st.plotly_chart(fig, use_container_width=True)
else:
    st.sidebar.info("No events detected")