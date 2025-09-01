# app.py
import sqlite3
from datetime import datetime, timezone, timedelta
from urllib.parse import quote_plus

import feedparser
import pandas as pd
import streamlit as st

# --- STREAMLIT CONFIG ---
st.set_page_config(page_title="Family Office News Tracker", layout="wide")

# ---- CONFIG ----
DB_FILE = "family_office_news.db"
GOOGLE_NEWS_SUFFIX = "hl=en-PH&gl=PH&ceid=PH:en"

DEFAULT_FAMILY_OFFICES = [
    "Rockefeller Capital Management",
    "Cascade Investment",
    "Blue Pool Capital"
]

# Only fetch news from the past 2 months
NEWS_CUTOFF_DATE = datetime.now(timezone.utc) - timedelta(days=60)

# ---- DATABASE HELPERS ----
def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id INTEGER PRIMARY KEY,
                family_office TEXT,
                title TEXT,
                link TEXT UNIQUE,
                published TEXT,
                source TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_news_published ON news(published DESC)")
        conn.commit()

def save_article(family_office, title, link, published_iso, source):
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        try:
            c.execute("""
                INSERT INTO news (family_office, title, link, published, source)
                VALUES (?, ?, ?, ?, ?)
            """, (family_office, title, link, published_iso, source))
            conn.commit()
        except sqlite3.IntegrityError:
            pass  # skip duplicates

def clear_all_news():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("DELETE FROM news")
        conn.commit()

def fetch_all_news(limit=100):
    with sqlite3.connect(DB_FILE) as conn:
        df = pd.read_sql_query(
            "SELECT * FROM news WHERE published >= ? ORDER BY published DESC LIMIT ?",
            conn,
            params=(NEWS_CUTOFF_DATE.isoformat(), limit)
        )
    return df

# ---- UTILITIES ----
def parse_entry_published(entry):
    for key in ("published_parsed", "updated_parsed"):
        if entry.get(key):
            try:
                return datetime(*entry[key][:6], tzinfo=timezone.utc)
            except Exception:
                pass
    if entry.get("published"):
        try:
            return pd.to_datetime(entry["published"], utc=True).to_pydatetime()
        except Exception:
            pass
    return datetime.now(timezone.utc)

def df_with_clickable_links(df, link_col="link", link_label="Read Article"):
    df = df.copy()
    df["article"] = df[link_col].apply(
        lambda x: f'<a href="{x}" target="_blank">{link_label}</a>' if x else ""
    )
    cols = [c for c in ["family_office", "title", "article", "published", "source"] if c in df.columns]
    return df[cols]

# ---- FETCH ARTICLES ----
def fetch_news_google(family_offices, per_office=20):
    results = []
    for fo in family_offices:
        query = quote_plus(fo)
        feed_url = f"https://news.google.com/rss/search?q={query}&{GOOGLE_NEWS_SUFFIX}"
        feed = feedparser.parse(feed_url)
        if getattr(feed, "bozo", False):
            continue
        for entry in feed.entries[:per_office]:
            title = entry.get("title", "").strip()
            link = entry.get("link", "").strip()
            published_dt = parse_entry_published(entry)

            # Skip older articles
            if published_dt < NEWS_CUTOFF_DATE:
                continue

            published_iso = published_dt.astimezone(timezone.utc).replace(tzinfo=timezone.utc).isoformat()
            source = "Google News"
            save_article(fo, title, link, published_iso, source)
            results.append({
                "family_office": fo,
                "title": title,
                "link": link,
                "published": published_iso,
                "source": source
            })
    return results

# ---- INIT ----
init_db()

st.title("📡 Family Office News Tracker (Google News Edition)")

st.sidebar.header("Family Offices to Track")
st.sidebar.write(
    "Enter one per line, e.g.,\n- Rockefeller Capital Management\n- Cascade Investment\n- Blue Pool Capital"
)

family_offices_input = st.sidebar.text_area(
    "Names (one per line):",
    value="",
    height=180
)

col_btn1, col_btn2 = st.sidebar.columns(2)
fetch_clicked = col_btn1.button("Fetch News Now", use_container_width=True)
clear_clicked = col_btn2.button("Clear All News", use_container_width=True)

# ---- CLEAR ALL NEWS ----
if clear_clicked:
    clear_all_news()
    st.sidebar.success("All news cleared.")

# ---- FETCH NEWS ----
if fetch_clicked:
    offices_raw = family_offices_input.strip()
    if offices_raw:
        offices = [fo.strip() for fo in offices_raw.split("\n") if fo.strip()]
    else:
        offices = DEFAULT_FAMILY_OFFICES

    with st.spinner("Fetching latest news from Google News..."):
        fetch_news_google(offices)

# ---- DISPLAY NEWS ----
st.subheader("📰 News from the Last 2 Months")
df_all = fetch_all_news(limit=200)
if not df_all.empty:
    df_disp = df_with_clickable_links(df_all, "link", "Open")
    st.markdown(df_disp.to_html(escape=False, index=False), unsafe_allow_html=True)
    st.download_button(
        "⬇️ Download News (CSV)",
        df_all.to_csv(index=False),
        "google_news.csv",
        mime="text/csv",
    )
else:
    st.info("No news available from the past 2 months.")
