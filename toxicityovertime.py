import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from db import get_cursor
from utils import getlogger

logger = getlogger("toxicity_panel")
defaultdays = 30

def parsedate(s, default):
    if not s:
        return default
    try:
        return datetime.fromisoformat(s)
    except:
        return default
# this function will  gathers all distinct reddit subreddits and 4chan boards present in the
# database and it normalizes naming  so the UI can show a
# unified list of communities for cross platform toxicity comparisons
def getthecommunities():
    arr = []
    bords = []
    with get_cursor() as cur:
        cur.execute("""
            SELECT DISTINCT
              COALESCE(
                NULLIF(data->>'subreddit', ''),
                NULLIF(data->>'subreddit_name_prefixed', '')
              ) AS sub
            FROM reddit_posts
            WHERE (data->>'subreddit') IS NOT NULL OR (data->>'subreddit_name_prefixed') IS NOT NULL
        """)
        for row in cur.fetchall():
            v = row[0]
            if not v:
                continue
            if v.startswith("r/"):
                v = v[2:]
            arr.append(v.strip().lower())
        cur.execute("SELECT DISTINCT board_name FROM chan_posts;")
        for row in cur.fetchall():
            bords.append(row[0].strip().lower())
    return arr + bords

# in this function i am computing toxicity statistics for selected communities across reddit and
# fourchann and this will calculates the average toxicity metric and samples a distribution of values
# for histogram plotting by enabling side by side toxicity analysis between platforms
def gettoxicity(platforms, communities, metric="toxicityscore", start=None, end=None):
    maxdistintrows = 2000
    startdate = datetime.combine(parsedate(start, datetime.utcnow() - timedelta(days=defaultdays)), datetime.min.time())
    enddate = datetime.combine(parsedate(end, datetime.utcnow()), datetime.max.time())
    results = []
    with get_cursor() as cur:
        for comm in communities:
            name = comm.strip().lower()
            if not name:
                continue
            if "chan" in platforms:
                cur.execute("SELECT 1 FROM chan_posts WHERE LOWER(board_name) = %s LIMIT 1;", (name,))
                ischan = cur.fetchone() is not None
                if ischan:
                    cur.execute(f"""
                        SELECT AVG(COALESCE({metric},0))
                        FROM chan_posts
                        WHERE LOWER(board_name) = %s AND created_at BETWEEN %s AND %s
                    """, (name, startdate, enddate))
                    averagevalue = cur.fetchone()[0] or 0
                    cur.execute(f"""
                        SELECT {metric} FROM chan_posts
                        WHERE LOWER(board_name) = %s AND created_at BETWEEN %s AND %s AND {metric} IS NOT NULL
                        ORDER BY random() LIMIT {maxdistintrows}
                    """, (name, startdate, enddate))
                    distintrows = [r[0] for r in cur.fetchall()]
                    results.append({
                        "community": name,
                        "platform": "4chan",
                        "avg": averagevalue,
                        "distribution": distintrows
                    })
            if "reddit" in platforms:
                subreddit_name = name
                alt_prefixed = "r/" + subreddit_name if not subreddit_name.startswith("r/") else subreddit_name
                cur.execute(f"""
                    SELECT AVG(COALESCE({metric},0))
                    FROM reddit_posts
                    WHERE (LOWER(data->>'subreddit') = %s OR LOWER(data->>'subreddit_name_prefixed') = %s)
                      AND created_at BETWEEN %s AND %s
                """, (subreddit_name, alt_prefixed, startdate, enddate))
                averagevalue = cur.fetchone()[0] or 0
                cur.execute(f"""
                    SELECT {metric} FROM reddit_posts
                    WHERE (LOWER(data->>'subreddit') = %s OR LOWER(data->>'subreddit_name_prefixed') = %s)
                      AND created_at BETWEEN %s AND %s AND {metric} IS NOT NULL
                    ORDER BY random() LIMIT {maxdistintrows}
                """, (subreddit_name, alt_prefixed, startdate, enddate))
                distintrows = [r[0] for r in cur.fetchall()]
                results.append({
                    "community": name,
                    "platform": "Reddit",
                    "avg": averagevalue,
                    "distribution": distintrows
                })
    return results
# this will renders the entire toxicity comparison section in Streamlit by gathering
# user inputs such as date ranges, platforms, communities, toxicity metric and it will fetches toxicity
# data and visualizes averages and distributions to highlight how toxic each community is
def rendertoxicity(startdate=None, enddate=None):
    st.header("Toxicity Comparison")
    if startdate is None:
        startdate = st.sidebar.date_input("Start date", datetime.utcnow() - timedelta(days=defaultdays))
    if enddate is None:
        enddate = st.sidebar.date_input("End date", datetime.utcnow())
    platforms = st.sidebar.multiselect("Select platforms", ["chan", "reddit"], default=["chan", "reddit"])
    communities = st.sidebar.multiselect("Select boards/subreddits", getthecommunities())
    metric = st.selectbox(
        "Select toxicity metric",
        ["toxicityscore", "severetoxicityscore", "insultscore",
         "profanityscore", "identityattackscore", "threatscore", "unsubstantialscore"]
    )
    if not communities or not platforms:
        st.warning("Please select at least one platform and community.")
        return
    toxicitydata = gettoxicity(platforms, communities, metric,
                               start=startdate.isoformat(), end=enddate.isoformat())
    for entry in toxicitydata:
        st.markdown(" ")
        with st.container(border=True): 
            st.subheader(f"{entry['platform']} — {entry['community']}")
            if entry["avg"] is None or len(entry["distribution"]) == 0:
                st.info("No data available for this range.")
                continue
            dist = pd.Series(entry["distribution"])
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Median toxicity (50%)", f"{dist.quantile(0.5):.4f}")
                st.metric("Minimum observed", f"{dist.min():.4f}")
            with col2:
                st.metric("25% → 75% percentile",
                          f"{dist.quantile(0.25):.4f} → {dist.quantile(0.75):.4f}")
                st.metric("Maximum observed", f"{dist.max():.4f}")
            st.markdown("---")
            fig = px.histogram(
                dist,
                x=dist,
                nbins=35,
                opacity=0.85,
                title=f"{metric} distribution for {entry['community']} ({entry['platform']})",
            )
            if entry["platform"] == "Reddit":
                fig.update_traces(marker_color="#d62728")
            else:
                fig.update_traces(marker_color="#57B9FA")
            fig.update_layout(
                bargap=0.05,
                margin=dict(l=20, r=20, t=60, b=20),
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)
