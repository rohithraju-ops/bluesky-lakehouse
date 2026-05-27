"""
Bluesky Lakehouse Dashboard
Tab 1 (Live): posts/sec gauge, language area chart, spike table — from RisingWave MVs
Tab 2 (Trends): trending hashtag bar chart — from RisingWave MV
Tab 3 (Languages): historical post volume by language — from Iceberg via DuckDB
"""
import time

import duckdb
import pandas as pd
import psycopg
import streamlit as st

RW_DSN = "host=localhost port=4566 dbname=dev user=root"
ICEBERG_PATH = "s3://lakehouse/warehouse/bsky/posts"

st.set_page_config(page_title="Bluesky Lakehouse", layout="wide", page_icon="🦋")


@st.cache_resource
def get_duckdb():
    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("SET s3_region='us-east-1';")
    con.execute("SET s3_access_key_id='minio';")
    con.execute("SET s3_secret_access_key='minio123';")
    con.execute("SET s3_endpoint='localhost:9000';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    con.execute("SET unsafe_enable_version_guessing=true;")
    return con


def rw_query(sql: str) -> pd.DataFrame:
    try:
        with psycopg.connect(RW_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                cols = [d.name for d in cur.description]
                rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.error(f"RisingWave error: {e}")
        return pd.DataFrame()


def duck_query(sql: str) -> pd.DataFrame:
    try:
        con = get_duckdb()
        return con.execute(sql).df()
    except Exception as e:
        st.error(f"DuckDB error: {e}")
        return pd.DataFrame()


# ── Sidebar ──────────────────────────────────────────────────────────────────
st.sidebar.title("Bluesky Lakehouse")
st.sidebar.caption("Real-time + historical Bluesky post analytics")
auto_refresh = st.sidebar.checkbox("Auto-refresh (3s)", value=True)
st.sidebar.markdown("---")
st.sidebar.markdown("**Stack**")
st.sidebar.markdown("Redpanda · RisingWave · Iceberg · MinIO · dbt · Dagster")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_live, tab_trends, tab_languages = st.tabs(["📡 Live", "🔥 Trends", "🌍 Languages"])

# ─────────────────────────── TAB 1: LIVE ─────────────────────────────────────
with tab_live:
    st.header("Live Stream")
    col_rate, col_lang = st.columns([1, 3])

    # Posts per second (most recent window)
    df_pps = rw_query(
        "SELECT post_count FROM posts_per_second ORDER BY second_bucket DESC LIMIT 1"
    )
    with col_rate:
        rate = int(df_pps["post_count"].iloc[0]) if not df_pps.empty else 0
        st.metric("Posts / second", rate, help="Latest 1-second tumble window")

    # Posts per minute by language — last 20 windows, top 6 langs
    df_lang = rw_query(
        """
        SELECT window_start AS minute, primary_lang AS lang, post_count
        FROM posts_per_minute_by_lang
        WHERE primary_lang IS NOT NULL
          AND primary_lang <> ''
          AND window_start >= NOW() - INTERVAL '30 minutes'
        ORDER BY window_start
        """
    )
    with col_lang:
        if not df_lang.empty:
            top_langs = (
                df_lang.groupby("lang")["post_count"].sum()
                .nlargest(6).index.tolist()
            )
            df_pivot = (
                df_lang[df_lang["lang"].isin(top_langs)]
                .pivot_table(index="minute", columns="lang", values="post_count", aggfunc="sum")
                .fillna(0)
            )
            st.area_chart(df_pivot, use_container_width=True)
        else:
            st.info("No language data yet — waiting for RisingWave MV to populate.")

    # Spike detector table
    st.subheader("Language Spikes")
    df_spikes = rw_query(
        """
        SELECT lang, minute_bucket, minute_count, rolling_hr_avg,
               ROUND(minute_count::numeric / NULLIF(rolling_hr_avg, 0), 2) AS spike_ratio
        FROM lang_spike_detector
        ORDER BY minute_bucket DESC, spike_ratio DESC
        LIMIT 20
        """
    )
    if not df_spikes.empty:
        st.dataframe(df_spikes, use_container_width=True)
    else:
        st.info("No spikes detected yet.")

# ─────────────────────────── TAB 2: TRENDS ────────────────────────────────────
with tab_trends:
    st.header("Trending Hashtags (5-minute window)")
    df_ht = rw_query(
        """
        SELECT hashtag, mentions, bucket_start
        FROM trending_hashtags_5m
        ORDER BY bucket_start DESC, mentions DESC
        LIMIT 30
        """
    )
    if not df_ht.empty:
        # Take most recent bucket
        latest_bucket = df_ht["bucket_start"].max()
        df_latest = df_ht[df_ht["bucket_start"] == latest_bucket].head(15)
        st.caption(f"Window: {latest_bucket}")
        st.bar_chart(
            df_latest.set_index("hashtag")["mentions"],
            use_container_width=True,
        )
        with st.expander("Raw data"):
            st.dataframe(df_ht, use_container_width=True)
    else:
        st.info("No hashtag data yet — waiting for posts with #hashtags.")

# ─────────────────────────── TAB 3: LANGUAGES ─────────────────────────────────
with tab_languages:
    st.header("Historical Post Volume by Language (Iceberg)")
    df_hist = duck_query(
        f"""
        SELECT
            DATE_TRUNC('hour', event_time) AS hour,
            primary_lang AS lang,
            COUNT(*) AS posts
        FROM iceberg_scan('{ICEBERG_PATH}', allow_moved_paths = true)
        WHERE primary_lang IS NOT NULL
          AND primary_lang <> ''
          AND did NOT LIKE 'harness:%'
        GROUP BY 1, 2
        ORDER BY 1
        """
    )
    if not df_hist.empty:
        top_langs = (
            df_hist.groupby("lang")["posts"].sum()
            .nlargest(8).index.tolist()
        )
        df_pivot = (
            df_hist[df_hist["lang"].isin(top_langs)]
            .pivot_table(index="hour", columns="lang", values="posts", aggfunc="sum")
            .fillna(0)
        )
        st.line_chart(df_pivot, use_container_width=True)
        st.caption(f"Total posts in Iceberg: {df_hist['posts'].sum():,}")
    else:
        st.info("No Iceberg data yet — or DuckDB couldn't reach MinIO.")

# ── Auto-refresh ──────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(3)
    st.rerun()
