import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from db import get_cursor
from utils import getlogger

logger = getlogger("ai_topic_panel")
defaultdays = 30
defaulttopics = ["ChatGPT", "Claude", "Gemini", "LLaMA"]

def parsedate(s, default):
    if not s:
        return default
    try:
        return datetime.fromisoformat(s)
    except:
        return default

# this function will retrieves time bucketed counts and toxicity averages for each ai topic
# across chan and redit by  scaning post content for keyword matches and aggregates
# activity over the selected date window returning structured series for visualization
def getaitopics(topics=None, platform="both", bucket="day", start=None, end=None):
    enddate = parsedate(end, datetime.utcnow())
    startdate = parsedate(start, enddate - timedelta(days=defaultdays))
    results = []
    for topic in topics or defaulttopics:
        fourchanpoints, redditpoints = [], []
        fourchtoxicityraw, reddittoxicityraw = [], []
        with get_cursor() as cur:
            if platform in ("chan", "both"):
                cur.execute(f"""
                    SELECT date_trunc('{bucket}', created_at) AS t, COUNT(*)::int AS count, AVG(toxicityscore) AS averagetoxicity
                    FROM chan_posts
                    WHERE content ILIKE %s AND created_at BETWEEN %s AND %s
                    GROUP BY 1
                    ORDER BY 1;
                """, (f"%{topic}%", startdate, enddate))
                rows = cur.fetchall()
                fourchanpoints = [{"t": r[0], "count": r[1], "averagetoxicity": float(r[2]) if r[2] else None} for r in rows]
                cur.execute("""
                    SELECT toxicityscore
                    FROM chan_posts
                    WHERE content ILIKE %s AND created_at BETWEEN %s AND %s AND toxicityscore IS NOT NULL;
                """, (f"%{topic}%", startdate, enddate))
                fourchtoxicityraw = [float(r[0]) for r in cur.fetchall()]
            if platform in ("reddit", "both"):
                cur.execute(f"""
                    SELECT date_trunc('{bucket}', created_at) AS t, COUNT(*)::int AS count, AVG(toxicityscore) AS averagetoxicity
                    FROM reddit_posts
                    WHERE ((data->>'body') ILIKE %s OR (data->>'title') ILIKE %s OR (data->>'selftext') ILIKE %s)
                      AND created_at BETWEEN %s AND %s
                    GROUP BY 1
                    ORDER BY 1;
                """, (f"%{topic}%", f"%{topic}%", f"%{topic}%", startdate, enddate))
                rows = cur.fetchall()
                redditpoints = [{"t": r[0], "count": r[1], "averagetoxicity": float(r[2]) if r[2] else None} for r in rows]
                cur.execute("""
                    SELECT toxicityscore
                    FROM reddit_posts
                    WHERE ((data->>'body') ILIKE %s OR (data->>'title') ILIKE %s OR (data->>'selftext') ILIKE %s)
                      AND created_at BETWEEN %s AND %s AND toxicityscore IS NOT NULL;
                """, (f"%{topic}%", f"%{topic}%", f"%{topic}%", startdate, enddate))
                reddittoxicityraw = [float(r[0]) for r in cur.fetchall()]
        results.append({"topic": topic, "chan": fourchanpoints, "reddit": redditpoints, 
                       "chantoxraw": fourchtoxicityraw, "reddittoxraw": reddittoxicityraw})
    return results

# function builds a summary table for all ai topic activity, it computes total post
# volume, average toxicity, maximum toxicity, and median toxicity for each platforms topic
# pair and producing a compact overview suitable for tabular display in the dashboard
def getsummarytable(toicdata):
    rows = []
    for topic in toicdata:
        for platformname, datapoints in [("4chan", topic["chan"]), ("Reddit", topic["reddit"])]:
            if datapoints:
                df = pd.DataFrame(datapoints)
                totalnumberofposts = df["count"].sum()
                averagetoxicity = df["averagetoxicity"].mean()
                maximumtoxicity = df["averagetoxicity"].max()
                toxocitymedian = df["averagetoxicity"].median()
                rows.append({
                    "Topic": topic["topic"],
                    "Platform": platformname,
                    "Total Posts": totalnumberofposts,
                    "Average Toxicity": round(averagetoxicity, 4) if averagetoxicity else None,
                    "Max Toxicity": round(maximumtoxicity, 4) if maximumtoxicity else None,
                    "Median Toxicity": round(toxocitymedian, 4) if toxocitymedian else None
                })
    return pd.DataFrame(rows)

#  it will renders the complete ai topic analysis panel in streamlit by  gathering
# user selections and  fetches topic-level activity and  plots post volume and toxicity trends
# and displays summary statistics and distribution charts for both Reddit and 4chan
def renderaitopic(startdate, enddate, platform="both"):
    st.header("Ai topic frequency analysis")
    selectedtopic = st.sidebar.multiselect("Select ai topics", defaulttopics, default=defaulttopics)
    selectedtopicmetric = st.sidebar.selectbox("Toxicity metric", ["averagetoxicity"])
    if not selectedtopic:
        st.warning("Please select at least one topic.")
        return
    toicdata = getaitopics(selectedtopic, platform, bucket="day",
                               start=startdate.isoformat(), end=enddate.isoformat())
    for topic in toicdata:
        st.subheader(f"Topic: {topic['topic']}")
        cdata = []
        if topic["chan"]:
            chandataframe = pd.DataFrame(topic["chan"])
            chandataframe["platform"] = "4chan"
            cdata.append(chandataframe)
        if topic["reddit"]:
            dataframreddit = pd.DataFrame(topic["reddit"])
            dataframreddit["platform"] = "Reddit"
            cdata.append(dataframreddit)
        if cdata:
            dataframrofall = pd.concat(cdata, ignore_index=True)

            totalcountfigure = px.line(
                dataframrofall,
                x="t",
                y="count",
                color="platform",
                title=f"{topic['topic']} - Combined post count",
                color_discrete_map={"4chan": "#66c2a5", "Reddit": "#d62728"}
            )
            st.plotly_chart(totalcountfigure, use_container_width=True)
            chanandreddittoxicfigure = px.line(
                dataframrofall,
                x="t",
                y="averagetoxicity",
                color="platform",
                title=f"{topic['topic']} - Combined average toxicity",
                color_discrete_map={"4chan": "#66c2a5", "Reddit": "#d62728"}
            )
            st.plotly_chart(chanandreddittoxicfigure, use_container_width=True)
    st.subheader("Summary statistics")
    summarytable = getsummarytable(toicdata)
    st.dataframe(summarytable, use_container_width=True)
    st.subheader("Toxicity distribution by topic and platform")
    alldata = []
    for topic in toicdata:
        if topic.get("chantoxraw") and len(topic["chantoxraw"]) > 0:
            avg = sum(topic["chantoxraw"]) / len(topic["chantoxraw"])
            alldata.append({
                "topic": topic["topic"],
                "platform": "4chan",
                "toxicity": avg
            })
        if topic.get("reddittoxraw") and len(topic["reddittoxraw"]) > 0:
            avg = sum(topic["reddittoxraw"]) / len(topic["reddittoxraw"])
            alldata.append({
                "topic": topic["topic"],
                "platform": "Reddit",
                "toxicity": avg
            })
    if alldata:
        dfall = pd.DataFrame(alldata)
        dfall["topicplatform"] = dfall["topic"] + " - " + dfall["platform"]
        figdist = px.bar(dfall, x="topicplatform", y="toxicity",
                          title="Toxicity distribution by topic and platform",
                          labels={"topicplatform": "Topic - Platform", "toxicity": "Average Toxicity"},
                          color="platform",
                          color_discrete_map={"4chan": "#66c2a5", "Reddit": "#d62728"})
        figdist.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(figdist, use_container_width=True)
