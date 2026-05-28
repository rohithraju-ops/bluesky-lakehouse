"""
Bluesky Lakehouse Dashboard
"""
import time
from datetime import datetime

import duckdb
import pandas as pd
import psycopg
import streamlit as st

RW_DSN        = "host=localhost port=4566 dbname=dev user=root"
ICEBERG_PATH  = "s3://lakehouse/warehouse/bsky/posts"

st.set_page_config(
    page_title="Bluesky Lakehouse",
    layout="wide",
    page_icon="⬡",
    initial_sidebar_state="expanded",
)

# ── Palette ───────────────────────────────────────────────────────────────────
BG     = "#12131f"
PANEL  = "#1e2035"
SIDEBAR= "#0e0f1a"
BORDER = "#2a2c45"
GOLD   = "#f0c040"
TEXT   = "#c8ccd8"
MUTED  = "#5a5f7a"
GREEN  = "#3fb950"
RED    = "#f85149"
PURPLE = "#a78bfa"
TEAL   = "#2a9d8f"

st.markdown(f"""
<style>
html,body,.stApp {{ background:{BG} !important; }}
[data-testid="stMain"],[data-testid="stMain"]>div,.main,.main .block-container
    {{ background:{BG} !important; }}
[data-testid="stSidebar"],
[data-testid="stSidebar"]>div:first-child
    {{ background:{SIDEBAR} !important; border-right:1px solid {BORDER}; }}
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] label {{ color:{MUTED} !important; }}
[data-testid="stSidebar"] strong {{ color:{TEXT}  !important; }}
[data-testid="stSidebar"] h3    {{ color:{TEXT}  !important; font-size:.95rem; }}

[data-baseweb="tab-list"] {{
    background:{PANEL} !important; border:1px solid {BORDER};
    border-radius:8px; padding:4px; gap:0; width:fit-content;
}}
[data-baseweb="tab"] {{
    border-radius:5px !important; padding:7px 26px !important;
    background:transparent !important; color:{MUTED} !important;
    font-size:13px !important; font-weight:600 !important; border:none !important;
}}
[aria-selected="true"][data-baseweb="tab"] {{
    background:{GOLD} !important; color:{SIDEBAR} !important;
}}

[data-testid="stDataFrame"]  {{ border:1px solid {BORDER}; border-radius:10px; overflow:hidden; }}
[data-testid="stAlert"]      {{ background:{PANEL} !important; border:1px solid {BORDER} !important;
                                border-radius:8px !important; }}
[data-testid="stAlert"] p    {{ color:{MUTED} !important; }}
[data-testid="stCaptionContainer"] p {{ color:{MUTED} !important; font-size:12px !important; }}
[data-testid="stExpander"]   {{ border:1px solid {BORDER} !important; border-radius:8px !important;
                                background:{PANEL} !important; }}
hr {{ border-color:{BORDER} !important; }}
</style>
""", unsafe_allow_html=True)


# ── UI primitives ─────────────────────────────────────────────────────────────
def kpi(label, value, tip=""):
    st.markdown(f"""
    <div title="{tip}" style="background:{PANEL};border:1px solid {BORDER};
        border-top:2px solid {GOLD};border-radius:10px;padding:20px 22px">
      <div style="font-size:10px;font-weight:700;letter-spacing:1.8px;
                  text-transform:uppercase;color:{MUTED};margin-bottom:10px">{label}</div>
      <div style="font-size:2.1rem;font-weight:800;color:{GOLD};line-height:1.1">{value}</div>
    </div>""", unsafe_allow_html=True)

def section(label):
    st.markdown(f"""
    <div style="font-size:10px;font-weight:700;letter-spacing:2.5px;text-transform:uppercase;
                color:{MUTED};padding-bottom:8px;border-bottom:1px solid {BORDER};
                margin:28px 0 14px 0">{label}</div>""", unsafe_allow_html=True)


# ── Data ──────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_duckdb():
    con = duckdb.connect(":memory:")
    for s in ["INSTALL httpfs; LOAD httpfs;","INSTALL iceberg; LOAD iceberg;",
              "SET s3_region='us-east-1';","SET s3_access_key_id='minio';",
              "SET s3_secret_access_key='minio123';","SET s3_endpoint='localhost:9000';",
              "SET s3_use_ssl=false;","SET s3_url_style='path';",
              "SET unsafe_enable_version_guessing=true;"]:
        con.execute(s)
    return con

def rw_query(sql):
    try:
        with psycopg.connect(RW_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                cols = [d.name for d in cur.description]
                rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.error(f"RisingWave: {e}")
        return pd.DataFrame()

def duck_query(sql):
    try:
        return get_duckdb().execute(sql).df()
    except Exception as e:
        # DuckDB connection becomes invalid after certain fatal errors — clear
        # the cache so the next call gets a fresh connection.
        if "invalidated" in str(e).lower() or "fatal" in str(e).lower():
            get_duckdb.clear()
            try:
                return get_duckdb().execute(sql).df()
            except Exception as e2:
                st.error(f"DuckDB / Iceberg: {e2}")
                return pd.DataFrame()
        st.error(f"DuckDB / Iceberg: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def duck_query_cached(sql):
    """Iceberg scans are slow — cache for 60s so live metrics aren't blocked."""
    return duck_query(sql)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Bluesky Lakehouse")
    st.caption("Real-time post analytics")
    st.divider()
    rw_ok = not rw_query("SELECT 1").empty
    st.markdown(f"{'🟢' if rw_ok else '🔴'}  RisingWave {'connected' if rw_ok else 'unreachable'}")
    st.markdown("")
    auto_refresh = st.checkbox("Auto-refresh (3s)", value=True)
    if auto_refresh:
        st.caption(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}")


# ── Page title ────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="display:flex;align-items:baseline;gap:16px;
            padding-bottom:18px;border-bottom:1px solid {BORDER};margin-bottom:24px">
  <span style="font-size:1.6rem;font-weight:800;color:{TEXT};letter-spacing:-0.5px">
    Bluesky Post Analytics
  </span>
  <span style="font-size:11px;color:{MUTED};font-weight:500;letter-spacing:0.3px">
    Jetstream &nbsp;/&nbsp; Redpanda &nbsp;/&nbsp; RisingWave &nbsp;/&nbsp; Iceberg
  </span>
</div>
""", unsafe_allow_html=True)

tab_live, tab_trends, tab_history, tab_pipeline = st.tabs(
    ["Live Stream", "Trending", "History", "Pipeline"]
)


# ═══════════════════════════════════════════════════════════════
#  TAB 1 — LIVE
# ═══════════════════════════════════════════════════════════════
with tab_live:
    section("Key Metrics")

    df_pps  = rw_query("SELECT post_count FROM posts_per_second ORDER BY second_bucket DESC LIMIT 1")
    df_tot  = rw_query("SELECT COUNT(*) AS n FROM posts_flat")
    df_lc   = rw_query("SELECT COUNT(DISTINCT lang) AS n FROM posts_per_minute_by_lang WHERE lang IS NOT NULL AND lang <> ''")
    df_spkc = rw_query("SELECT COUNT(*) AS n FROM lang_spike_detector WHERE is_spiking = true")

    c1,c2,c3,c4 = st.columns(4)
    with c1: kpi("Posts / second",   f"{int(df_pps['post_count'].iloc[0]):,}" if not df_pps.empty  else "—",
                 "Latest 1-second tumble window")
    with c2: kpi("Total posts seen", f"{int(df_tot['n'].iloc[0]):,}"          if not df_tot.empty  else "—",
                 "Rows in posts_flat — deduplicated stream MV")
    with c3: kpi("Active languages", f"{int(df_lc['n'].iloc[0])}"             if not df_lc.empty   else "—",
                 "Distinct ISO 639-1 codes posting right now")
    with c4: kpi("Active spikes",    f"{int(df_spkc['n'].iloc[0])}"           if not df_spkc.empty else "—",
                 "Languages posting > 2× their hourly average")

    section("Post Volume by Language — Last 30 Minutes")
    st.caption("Source: posts_per_minute_by_lang · 1-minute tumble windows · top 8 languages by volume")

    df_lang = rw_query("""
        SELECT minute_bucket AS minute, lang, post_count
        FROM posts_per_minute_by_lang
        WHERE lang IS NOT NULL AND lang <> ''
          AND minute_bucket >= NOW() - INTERVAL '30 minutes'
        ORDER BY minute_bucket
    """)
    if not df_lang.empty:
        top = df_lang.groupby("lang")["post_count"].sum().nlargest(8).index.tolist()
        df_piv = (df_lang[df_lang["lang"].isin(top)]
                  .pivot_table(index="minute", columns="lang",
                               values="post_count", aggfunc="sum").fillna(0))
        st.area_chart(df_piv, width="stretch")
    else:
        st.info("Waiting for posts_per_minute_by_lang to populate.")

    section("Language Spike Detector")
    st.caption("Fires when a language's 1-min rate exceeds 2× its rolling hourly average · source: lang_spike_detector")

    df_spk = rw_query("""
        SELECT lang                                                               AS "Language",
               minute_bucket                                                      AS "Minute",
               posts_in_minute                                                    AS "Posts",
               rolling_hr_avg                                                     AS "Hourly Avg",
               (ROUND(posts_in_minute::numeric/NULLIF(rolling_hr_avg,0),2))::FLOAT AS "Spike Ratio"
        FROM lang_spike_detector
        WHERE is_spiking = true
        ORDER BY minute_bucket DESC, "Spike Ratio" DESC
        LIMIT 20
    """)
    if not df_spk.empty:
        st.dataframe(df_spk, column_config={
            "Spike Ratio": st.column_config.ProgressColumn(
                "Spike Ratio", min_value=0,
                max_value=max(float(df_spk["Spike Ratio"].max()), 1.0), format="%.2f×"),
            "Minute": st.column_config.DatetimeColumn("Minute", format="MMM D, HH:mm"),
        }, width="stretch", hide_index=True)
    else:
        st.info("No active spikes — all languages within normal range.")


# ═══════════════════════════════════════════════════════════════
#  TAB 2 — TRENDING
# ═══════════════════════════════════════════════════════════════
with tab_trends:
    section("Trending Hashtags — 5-Minute Window")
    st.caption("regexp_matches on post text · most recently closed tumble window · trending_hashtags_5m")

    df_ht = rw_query("""
        SELECT hashtag, mentions, bucket_start FROM trending_hashtags_5m
        ORDER BY bucket_start DESC, mentions DESC LIMIT 50
    """)
    if not df_ht.empty:
        latest  = df_ht["bucket_start"].max()
        df_l    = df_ht[df_ht["bucket_start"] == latest].head(20)
        ch, si  = st.columns([3,1])
        with ch:
            st.bar_chart(df_l.set_index("hashtag")["mentions"], width="stretch", color=GOLD)
        with si:
            st.markdown(f"""
            <div style="background:{PANEL};border:1px solid {BORDER};border-radius:10px;padding:18px">
              <div style="font-size:9px;font-weight:700;letter-spacing:1.5px;
                          text-transform:uppercase;color:{MUTED};margin-bottom:4px">Window closed</div>
              <div style="font-size:12px;color:{TEXT};margin-bottom:16px">{latest}</div>
              <div style="font-size:9px;font-weight:700;letter-spacing:1.5px;
                          text-transform:uppercase;color:{MUTED};margin-bottom:4px">Unique hashtags</div>
              <div style="font-size:1.8rem;font-weight:800;color:{GOLD};margin-bottom:12px">{len(df_l)}</div>
              <div style="font-size:9px;font-weight:700;letter-spacing:1.5px;
                          text-transform:uppercase;color:{MUTED};margin-bottom:4px">Top hashtag</div>
              <div style="font-size:1.2rem;font-weight:700;color:{GOLD};margin-bottom:12px">
                #{df_l.iloc[0]['hashtag'] if not df_l.empty else '—'}
              </div>
              <div style="font-size:9px;font-weight:700;letter-spacing:1.5px;
                          text-transform:uppercase;color:{MUTED};margin-bottom:4px">Peak mentions</div>
              <div style="font-size:1.8rem;font-weight:800;color:{GOLD}">
                {int(df_l.iloc[0]['mentions']) if not df_l.empty else 0}
              </div>
            </div>""", unsafe_allow_html=True)
        with st.expander("Full table"):
            st.dataframe(df_l[["hashtag","mentions"]].rename(
                columns={"hashtag":"Hashtag","mentions":"Mentions"}),
                width="stretch", hide_index=True)
    else:
        st.info("No hashtag data yet.")

    section("1-Hour Rolling Trends")
    st.caption("HOP window (1h size, 5m slide) · captures sustained trends vs one-off spikes")

    df_hop = rw_query("""
        SELECT hashtag, mentions, window_start FROM trending_hashtags_1h_hop
        ORDER BY window_start DESC, mentions DESC LIMIT 20
    """)
    if not df_hop.empty:
        lh = df_hop["window_start"].max()
        st.bar_chart(df_hop[df_hop["window_start"]==lh].head(15)
                     .set_index("hashtag")["mentions"], width="stretch", color=PURPLE)
        st.caption(f"Window ending {lh}")
    else:
        st.info("No 1-hour hop data yet.")


# ═══════════════════════════════════════════════════════════════
#  TAB 3 — HISTORY
# ═══════════════════════════════════════════════════════════════
with tab_history:
    section("Post Volume — Apache Iceberg (Durable Layer)")
    st.caption("DuckDB iceberg_scan() · Parquet files in MinIO · ~60s snapshot cadence · test markers excluded")

    df_hist = duck_query_cached(f"""
        SELECT DATE_TRUNC('hour', event_time) AS hour,
               primary_lang AS lang, COUNT(*) AS posts
        FROM iceberg_scan('{ICEBERG_PATH}', allow_moved_paths=true)
        WHERE primary_lang IS NOT NULL AND primary_lang <> ''
          AND did NOT LIKE 'harness:%'
        GROUP BY 1,2 ORDER BY 1
    """)
    if not df_hist.empty:
        top_l = df_hist.groupby("lang")["posts"].sum().idxmax()
        h1,h2,h3,h4 = st.columns(4)
        with h1: kpi("Total posts",        f"{int(df_hist['posts'].sum()):,}", "Committed to Iceberg, test markers excluded")
        with h2: kpi("Languages",          f"{df_hist['lang'].nunique()}",     "Distinct ISO 639-1 codes")
        with h3: kpi("Hours of data",      f"{df_hist['hour'].nunique()}",     "Distinct hourly buckets")
        with h4: kpi("Dominant language",  top_l,                             "Highest overall post count")
        st.markdown("")
        top_langs = df_hist.groupby("lang")["posts"].sum().nlargest(8).index.tolist()
        df_piv = (df_hist[df_hist["lang"].isin(top_langs)]
                  .pivot_table(index="hour", columns="lang", values="posts", aggfunc="sum").fillna(0))
        st.line_chart(df_piv, width="stretch")
        st.caption(f"Top languages: {', '.join(top_langs)}")
        with st.expander("Raw hourly data"):
            st.dataframe(df_hist.rename(columns={"hour":"Hour","lang":"Language","posts":"Posts"}),
                         width="stretch", hide_index=True)
    else:
        st.info("No Iceberg data — check that the RisingWave sink has committed at least one snapshot.")


# ═══════════════════════════════════════════════════════════════
#  TAB 4 — PIPELINE
# ═══════════════════════════════════════════════════════════════
with tab_pipeline:

    section("Architecture")
    st.caption("End-to-end data flow — from the Bluesky firehose to this dashboard")

    # ── Pipeline nodes: rendered one card per column so Streamlit never
    #    mistakes the HTML for a Markdown code block ───────────────────────────
    pipeline = [
        ("01", "SOURCE",      "Jetstream",       "Bluesky public WebSocket firehose. ~3M posts/day.",          GOLD),
        ("02", "INGESTOR",    "Python Service",  "asyncio client. Batches events, produces to Redpanda.",      TEAL),
        ("03", "BROKER",      "Redpanda",        "Kafka-compatible broker. Topic: bsky.posts.",                "#e9c46a"),
        ("04", "STREAMING",   "RisingWave",      "Streaming SQL. 7 MVs, watermarked source, Iceberg sink.",   PURPLE),
        ("05", "STORAGE",     "Iceberg + MinIO", "Open table format on S3. Polaris catalog. ~60s commits.",   "#4cc9f0"),
        ("06", "BATCH",       "dbt-DuckDB",      "Hourly iceberg_scan(). 3 models: stg, fct, dim.",           GREEN),
        ("07", "ORCHESTRATE", "Dagster",         "Asset graph. Hourly dbt schedule. Observes all layers.",    "#f77f00"),
    ]

    # Row 1: ingestion side  (nodes 0-3)
    cols_a = st.columns([10, 1, 10, 1, 10, 1, 10])
    for ci, (ni, (step, layer, name, desc, color)) in enumerate(zip([0,2,4,6], pipeline[:4])):
        with cols_a[ni]:
            st.markdown(f"""<div style="background:{PANEL};border:1px solid {BORDER};border-top:3px solid {color};border-radius:10px;padding:16px 14px;height:100%"><div style="display:flex;justify-content:space-between;margin-bottom:10px"><span style="font-size:9px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:{MUTED}">{layer}</span><span style="font-size:9px;font-weight:800;color:{color};background:{BG};border:1px solid {BORDER};border-radius:4px;padding:2px 6px">{step}</span></div><div style="font-size:14px;font-weight:700;color:{color};margin-bottom:8px">{name}</div><div style="font-size:11px;color:{MUTED};line-height:1.5">{desc}</div></div>""".strip(), unsafe_allow_html=True)
    for ci in [1, 3, 5]:
        with cols_a[ci]:
            st.markdown(f"""<div style="display:flex;align-items:center;justify-content:center;height:100%;padding-top:20px;color:{BORDER};font-size:24px">&#8594;</div>""".strip(), unsafe_allow_html=True)

    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    # Row 2: storage + batch side  (nodes 4-6), centered under row 1
    _, ca, _x, cb, _y, cc, _ = st.columns([10, 10, 1, 10, 1, 10, 10])
    for col, (step, layer, name, desc, color) in zip([ca, cb, cc], pipeline[4:]):
        with col:
            st.markdown(f"""<div style="background:{PANEL};border:1px solid {BORDER};border-top:3px solid {color};border-radius:10px;padding:16px 14px"><div style="display:flex;justify-content:space-between;margin-bottom:10px"><span style="font-size:9px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:{MUTED}">{layer}</span><span style="font-size:9px;font-weight:800;color:{color};background:{BG};border:1px solid {BORDER};border-radius:4px;padding:2px 6px">{step}</span></div><div style="font-size:14px;font-weight:700;color:{color};margin-bottom:8px">{name}</div><div style="font-size:11px;color:{MUTED};line-height:1.5">{desc}</div></div>""".strip(), unsafe_allow_html=True)
    with _x:
        st.markdown(f"""<div style="display:flex;align-items:center;justify-content:center;height:100%;padding-top:20px;color:{BORDER};font-size:24px">&#8594;</div>""".strip(), unsafe_allow_html=True)
    with _y:
        st.markdown(f"""<div style="display:flex;align-items:center;justify-content:center;height:100%;padding-top:20px;color:{BORDER};font-size:24px">&#8594;</div>""".strip(), unsafe_allow_html=True)

    # ── Key results row ───────────────────────────────────────────────────────
    section("Key Results")
    r1, r2, r3 = st.columns(3)
    with r1:
        kpi("Latency p50 / p99", "54s / 100s", "Event-time to processing-time delta measured via RisingWave event_lateness_hist view")
    with r2:
        kpi("Exactly-once delivery", "PASS", "50,000 / 50,000 markers delivered · 0 duplicates · 0 missing across 3 chaos scenarios")
    with r3:
        kpi("Iceberg commit cadence", "~60s", "Two-phase commit between RisingWave and Iceberg · Polaris REST catalog")

    # ── Stack table ───────────────────────────────────────────────────────────
    section("Tech Stack")
    st.caption("Every component in the pipeline — what it does and why it was chosen")

    stack_data = {
        "Component": ["Bluesky Jetstream","Redpanda","RisingWave","Apache Iceberg",
                      "Apache Polaris","MinIO","dbt-DuckDB","Dagster","Streamlit"],
        "Layer":     ["Source","Broker","Streaming","Table Format",
                      "Catalog","Object Store","Batch","Orchestration","Serving"],
        "Role": [
            "WebSocket firehose — real-time public feed of all Bluesky posts",
            "Kafka-compatible broker — decouples ingestor from stream processor",
            "Streaming SQL engine — materialized views, watermarks, Iceberg sink",
            "Open table format — ACID transactions, schema evolution, time-travel",
            "REST catalog — manages Iceberg namespaces, tables, access control",
            "S3-compatible object store — stores all Parquet data files locally",
            "Batch transform layer — reads Iceberg via DuckDB, hourly aggregates",
            "Orchestrator — asset lineage graph, hourly dbt schedule, health checks",
            "Dashboard — live (RisingWave MVs) + historical (Iceberg) in one UI",
        ],
        "Why": [
            "Free, public, high-volume, real-world data — no synthetic generators",
            "Kafka API without ZooKeeper — simpler local dev setup",
            "Native Iceberg sink with two-phase commit — exactly-once guarantee",
            "Industry standard; works with DuckDB, Spark, Trino, Athena",
            "Official Iceberg REST catalog spec — vendor-neutral, swappable",
            "Drop-in S3 replacement — zero cloud cost for local development",
            "No Spark needed — DuckDB reads Parquet directly at query time",
            "Software-defined assets — lineage is explicit code, not implicit",
            "Fast iteration; cache_resource for shared DuckDB connection",
        ],
    }
    st.dataframe(
        pd.DataFrame(stack_data),
        column_config={
            "Component": st.column_config.TextColumn("Component", width="medium"),
            "Layer":     st.column_config.TextColumn("Layer",     width="small"),
            "Role":      st.column_config.TextColumn("Role",      width="large"),
            "Why":       st.column_config.TextColumn("Why",       width="large"),
        },
        width="stretch", hide_index=True,
    )


# ── Auto-refresh ──────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(3)
    st.rerun()
