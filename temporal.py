import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from db import get_cursor
from utils import getlogger

logger = getlogger("temporal_panel")
defaultdays = 30
# this function safely parses a date string into a datetime object, if the input is
# missing or invalid it falls back to either the provided default or the current
# utc timings ensuring all downstream temporal functions receive valid timestamps
def parsedate(s, default=None):
    if not s:
        return default or datetime.utcnow()
    try:
        return datetime.fromisoformat(s)
    except:
        return default or datetime.utcnow()

# in this function we retrieves time bucketed post activity for both 4chan and reddit
# It will then groups posts by the chosen bucket  and calculates post counts and average
# toxicity scores and it will return a structured series used for time-series charts
def getpostspertime(bucket="day", start=None, end=None):
    enddate = parsedate(end, datetime.utcnow())
    startdate = parsedate(start, enddate - timedelta(days=defaultdays))
    platformdatalist = []
    with get_cursor() as cur:
        sql = f"""
            SELECT date_trunc('{bucket}', created_at) AS bucket_ts,
                   COUNT(*)::int AS cnt,
                   AVG(toxicityscore)::float AS avg_tox
            FROM chan_posts
            WHERE board_name = 'g'
            AND created_at BETWEEN %s AND %s
            GROUP BY bucket_ts
            ORDER BY bucket_ts;
        """
        cur.execute(sql, (startdate, enddate))
        datarows = cur.fetchall()
        platformdatalist.append({
            "platform": "4chan",
            "points": [{"t": r[0], "count": r[1], "avg_tox": r[2] or 0} for r in datarows]
        })
    with get_cursor() as cur:
        sql = f"""
            SELECT date_trunc('{bucket}', created_at) AS bucket_ts,
                   COUNT(*)::int AS cnt,
                   AVG(COALESCE(toxicityscore,0))::float AS avg_tox
            FROM reddit_posts
            WHERE created_at BETWEEN %s AND %s
            GROUP BY bucket_ts
            ORDER BY bucket_ts;
        """
        cur.execute(sql, (startdate, enddate))
        datarows = cur.fetchall()
        platformdatalist.append({
            "platform": "Reddit",
            "points": [{"t": r[0], "count": r[1], "avg_tox": r[2] or 0} for r in datarows]
        })
    return {"start": startdate, "end": enddate, "bucket": bucket, "series": platformdatalist}

#  function will returns a simple summary of total posts within the selected date range
def gettemporalsummary(start=None, end=None):
    enddate = parsedate(end, datetime.utcnow())
    startdate = parsedate(start, enddate - timedelta(days=defaultdays))
    sumarytable = {}
    with get_cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM chan_posts WHERE board_name = 'g' AND created_at BETWEEN %s AND %s", (startdate, enddate))
        sumarytable["4chan total posts"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM reddit_posts WHERE created_at BETWEEN %s AND %s", (startdate, enddate))
        sumarytable["Reddit total posts"] = cur.fetchone()[0]
    return sumarytable

# this function computes posting frequency for each day of the week across both the platforms and 
# it analyze weekday vs weekend behavior by returning counts grouped by date of the week and 
# which is then used to build comparative bar charts
def weekdayvsweekendstats(start=None, end=None):
    enddate = parsedate(end, datetime.utcnow())
    startdate = parsedate(start, enddate - timedelta(days=defaultdays))
    result = []
    with get_cursor() as cur:
        cur.execute("""
            SELECT EXTRACT(DOW FROM created_at)::int AS weekday,
                   COUNT(*)::int AS cnt
            FROM chan_posts
            WHERE board_name = 'g'
            AND created_at BETWEEN %s AND %s
            GROUP BY weekday
            ORDER BY weekday;
        """, (startdate, enddate))
        dataframe = pd.DataFrame(cur.fetchall(), columns=["weekday", "count"])
        dataframe["platform"] = "4chan"
        result.append(dataframe)
        cur.execute("""
            SELECT EXTRACT(DOW FROM created_at)::int AS weekday,
                   COUNT(*)::int AS cnt
            FROM reddit_posts
            WHERE created_at BETWEEN %s AND %s
            GROUP BY weekday
            ORDER BY weekday;
        """, (startdate, enddate))
        dataframe = pd.DataFrame(cur.fetchall(), columns=["weekday", "count"])
        dataframe["platform"] = "Reddit"
        result.append(dataframe)
    return pd.concat(result) if result else pd.DataFrame()

# in this function i am  calculatiing how average post length changes over time for 4chan and reddit
# it then aggregates daily averages and returns combined data 
def postlengthovertime(start=None, end=None):
    enddate = parsedate(end, datetime.utcnow())
    startdate = parsedate(start, enddate - timedelta(days=defaultdays))
    platformdatalist = []
    with get_cursor() as cur:
        cur.execute("""
            SELECT date_trunc('day', created_at) AS day,
                   AVG(LENGTH(content))::float AS avg_len
            FROM chan_posts
            WHERE board_name = 'g'
            AND created_at BETWEEN %s AND %s
            GROUP BY day
            ORDER BY day;
        """, (startdate, enddate))
        dataframe = pd.DataFrame(cur.fetchall(), columns=["day", "avg_len"])
        dataframe["platform"] = "4chan"
        platformdatalist.append(dataframe)
        cur.execute("""
            SELECT date_trunc('day', created_at) AS day,
                   AVG(
                       LENGTH(
                           COALESCE(data->>'body','') ||
                           COALESCE(data->>'title','') ||
                           COALESCE(data->>'selftext','')
                       )
                   )::float AS avg_len
            FROM reddit_posts
            WHERE created_at BETWEEN %s AND %s
            GROUP BY day
            ORDER BY day;
        """, (startdate, enddate))
        dataframe = pd.DataFrame(cur.fetchall(), columns=["day", "avg_len"])
        dataframe["platform"] = "Reddit"
        platformdatalist.append(dataframe)
    return pd.concat(platformdatalist) if platformdatalist else pd.DataFrame()

# this function returns the top authors by post count for a selected date range
# it queries both fourchans and reddit and aggregates counts and will returns dataframes for each platform
def gettopauthors(start=None, end=None, limit=20):
    enddate = parsedate(end, datetime.utcnow())
    startdate = parsedate(start, enddate - timedelta(days=defaultdays))
    results = {}
    with get_cursor() as cur:
        cur.execute("""
            SELECT author_name, COUNT(*)::int AS cnt
            FROM chan_posts
            WHERE board_name = 'g' AND created_at BETWEEN %s AND %s
            GROUP BY author_name
            ORDER BY cnt DESC
            LIMIT %s;
        """, (startdate, enddate, limit))
        rows = cur.fetchall()
        results["4chan"] = pd.DataFrame(rows, columns=["author_name", "count"])
    with get_cursor() as cur:
        cur.execute("""
            SELECT data->>'author' AS author_name, COUNT(*)::int AS cnt
            FROM reddit_posts
            WHERE created_at BETWEEN %s AND %s
            GROUP BY author_name
            ORDER BY cnt DESC
            LIMIT %s;
        """, (startdate, enddate, limit))
        rows = cur.fetchall()
        results["Reddit"] = pd.DataFrame(rows, columns=["author_name", "count"])
    return results

# this function computes posting patterns by hour for the top authors
# it will tell when the most active authors post happened by aggregated per hour of the day
def authortimepattern(start=None, end=None, topn=10):
    enddate = parsedate(end, datetime.utcnow())
    startdate = parsedate(start, enddate - timedelta(days=defaultdays))
    results = {}
    with get_cursor() as cur:
        cur.execute("""
            SELECT author_name, DATE_PART('hour', created_at)::int AS hour, COUNT(*)::int AS cnt
            FROM chan_posts
            WHERE board_name = 'g' AND created_at BETWEEN %s AND %s
            AND author_name != 'Anonymous'
            GROUP BY author_name, hour
            ORDER BY COUNT(*) DESC;
        """, (startdate, enddate))
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=["author_name", "hour", "count"])
        topauthors = df.groupby("author_name")["count"].sum().sort_values(ascending=False).head(topn).index.tolist()
        df = df[df["author_name"].isin(topauthors)]
        pivot = df.pivot_table(index="author_name", columns="hour", values="count", fill_value=0)
        results["4chan"] = pivot.reindex(index=topauthors).fillna(0)
    with get_cursor() as cur:
        cur.execute("""
            SELECT data->>'author' AS author_name, DATE_PART('hour', created_at)::int AS hour, COUNT(*)::int AS cnt
            FROM reddit_posts
            WHERE created_at BETWEEN %s AND %s
            AND data->>'author' != 'AutoModerator'
            GROUP BY author_name, hour
            ORDER BY COUNT(*) DESC;
        """, (startdate, enddate))
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=["author_name", "hour", "count"])
        topauthors = df.groupby("author_name")["count"].sum().sort_values(ascending=False).head(topn).index.tolist()
        df = df[df["author_name"].isin(topauthors)]
        pivot = df.pivot_table(index="author_name", columns="hour", values="count", fill_value=0)
        results["Reddit"] = pivot.reindex(index=topauthors).fillna(0)
    return results

# this function calculates average post length by author for a given date range
def averagepostlenghtbyauthor(start=None, end=None, limit=20):
    enddate = parsedate(end, datetime.utcnow())
    startdate = parsedate(start, enddate - timedelta(days=defaultdays))
    results = {}
    with get_cursor() as cur:
        cur.execute("""
            SELECT author_name, AVG(LENGTH(content))::float AS avg_len, COUNT(*)::int AS cnt
            FROM chan_posts
            WHERE board_name = 'g' AND created_at BETWEEN %s AND %s
            GROUP BY author_name
            ORDER BY avg_len DESC
            LIMIT %s;
        """, (startdate, enddate, limit))
        rows = cur.fetchall()
        results["4chan"] = pd.DataFrame(rows, columns=["author_name", "avg_len", "count"])
    with get_cursor() as cur:
        cur.execute("""
            SELECT data->>'author' AS author_name,
                   AVG(LENGTH(COALESCE(data->>'body','') || COALESCE(data->>'title','') || COALESCE(data->>'selftext','')))::float AS avg_len,
                   COUNT(*)::int AS cnt
            FROM reddit_posts
            WHERE created_at BETWEEN %s AND %s
            GROUP BY author_name
            ORDER BY avg_len DESC
            LIMIT %s;
        """, (startdate, enddate, limit))
        rows = cur.fetchall()
        results["Reddit"] = pd.DataFrame(rows, columns=["author_name", "avg_len", "count"])
    return results

# this function renders the entire temporal analytics panel 
# it coordinates all temporal functions and allows selection of day/week option and 
# optionally displays top authors and their  activity patterns and avg post length
# and plots the  interactive line/bar charts to visualize posting patterns 
# weekday vs weekend behavior and post length trends
def rendertemporal(startdate=None, enddate=None):
    st.header("Temporal activity")
    bucket = st.sidebar.selectbox("Aggregation", ["day", "week"], index=0)
    getothers = st.sidebar.selectbox("Get others", ["None", "Top authors", "Author time patterns", "Avg post length by author"], index=0)
    data = getpostspertime(bucket=bucket, start=startdate.isoformat(), end=enddate.isoformat())
    for s in data["series"]:
        dataframe = pd.DataFrame(s["points"])
        if dataframe.empty:
            st.warning(f"their is no data for {s['platform']}")
            continue
        figure = px.line(dataframe, x="t", y="count", title=f"{s['platform']} Posts over time")
        if s['platform'] == "Reddit":
            figure.update_traces(line_color="#ff6b6b")
        else:
            figure.update_traces(line_color="#57B9FA")
        st.plotly_chart(figure, use_container_width=True)
    st.subheader("Average toxicity over time")
    dataframeall = pd.concat([pd.DataFrame(s["points"]).assign(platform=s["platform"]) for s in data["series"] if s["points"]], ignore_index=True)
    if not dataframeall.empty:
        toxicityfigure = px.line(dataframeall, x="t", y="avg_tox", color="platform", title="Avg toxicity over time",
                                 color_discrete_map={"4chan": "#57B9FA", "Reddit": "#ff6b6b"})
        st.plotly_chart(toxicityfigure, use_container_width=True)

    st.subheader("sumary table")
    sumarytable = gettemporalsummary(start=startdate.isoformat(), end=enddate.isoformat())
    st.json(sumarytable)
    st.subheader("Weekday and Weekend activity")
    weekdaydataframe = weekdayvsweekendstats(start=startdate.isoformat(), end=enddate.isoformat())

    if not weekdaydataframe.empty:
        weekdaydataframe["day_name"] = weekdaydataframe["weekday"].apply(lambda x: ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"][int(x)])
        figure = px.bar(weekdaydataframe, x="day_name", y="count", color="platform",
                     barmode="group", title="Posts by weekday",
                     color_discrete_map={"4chan": "#57B9FA", "Reddit": "#ff6b6b"})
        st.plotly_chart(figure, use_container_width=True)
    st.subheader("Post Length over time")
    lengthofdataframe = postlengthovertime(start=startdate.isoformat(), end=enddate.isoformat())
    if not lengthofdataframe.empty:
        figure = px.line(lengthofdataframe, x="day", y="avg_len", color="platform",
                      title="Average post length over time",
                      color_discrete_map={"4chan": "#57B9FA", "Reddit": "#ff6b6b"})
        st.plotly_chart(figure, use_container_width=True)

    if getothers == "Top authors":
        st.subheader("Top Frequent Authors (by post count)")
        tops = gettopauthors(start=startdate.isoformat(), end=enddate.isoformat(), limit=30)
        column1, column2 = st.columns(2)
        with column1:
            st.markdown("#### 4chan top authors (/g/)")
            df4 = tops.get("4chan", pd.DataFrame(columns=["author_name", "count"]))
            if not df4.empty:
                st.dataframe(df4)
                df4_chart = df4[df4["author_name"] != "Anonymous"].head(20)
                if not df4_chart.empty:
                    figurefour = px.bar(df4_chart, x="author_name", y="count", title="4chan top 20 authors (excluding Anonymous)")
                    maximumvalue = df4_chart["count"].max()
                    figurefour.update_layout(xaxis_tickangle=-45, height=500, yaxis=dict(range=[0, maximumvalue * 1.1]))
                    st.plotly_chart(figurefour, use_container_width=True)
                else:
                    st.write("no data to chart")
            else:
                st.write("no data")
        with column2:
            st.markdown("#### Reddit top authors")
            dfr = tops.get("Reddit", pd.DataFrame(columns=["author_name", "count"]))
            if not dfr.empty:
                st.dataframe(dfr)
                dfr_chart = dfr[dfr["author_name"] != "AutoModerator"].head(20)
                if not dfr_chart.empty:
                    figure = px.bar(dfr_chart, x="author_name", y="count", title="Reddit top 20 authors (excluding AutoModerator)")
                    maximumvalue = dfr_chart["count"].max()
                    figure.update_layout(xaxis_tickangle=-45, height=500, yaxis=dict(range=[0, maximumvalue * 1.1]))
                    st.plotly_chart(figure, use_container_width=True)
                else:
                    st.write("no data to chart")
            else:
                st.write("no data")

    if getothers == "Author time patterns":
        st.subheader("Author Posting Time Patterns (top authors)")
        pats = authortimepattern(start=startdate.isoformat(), end=enddate.isoformat(), topn=10)
        column1, column2 = st.columns(2)
        with column1:
            st.markdown("#### 4chan hourly activity (top authors)")
            plotfour = pats.get("4chan")
            if plotfour is not None and not plotfour.empty:
                plotfoursimple = plotfour.reset_index()
                plotfoursimple = plotfoursimple.melt(id_vars=['author_name'], var_name='hour', value_name='posts')
                plotfoursimple = plotfoursimple[plotfoursimple['posts'] > 0]
                plotfoursimple = plotfoursimple.sort_values(['author_name', 'posts'], ascending=[True, False])
                st.dataframe(plotfoursimple, height=400)
                figurefour = px.bar(plotfoursimple, x="hour", y="posts", color="author_name", 
                             title="4chan posting activity by hour",
                             labels={"hour": "Hour of Day", "posts": "Number of Posts"})
                figurefour.update_layout(height=500, xaxis=dict(tickmode='linear', tick0=0, dtick=2))
                st.plotly_chart(figurefour, use_container_width=True)
            else:
                st.write("no data")
        with column2:
            st.markdown("#### Reddit hourly activity (top authors)")
            pr = pats.get("Reddit")
            if pr is not None and not pr.empty:
                prsimple = pr.reset_index()
                prsimple = prsimple.melt(id_vars=['author_name'], var_name='hour', value_name='posts')
                prsimple = prsimple[prsimple['posts'] > 0]
                prsimple = prsimple.sort_values(['author_name', 'posts'], ascending=[True, False])
                st.dataframe(prsimple, height=400)
                figure = px.bar(prsimple, x="hour", y="posts", color="author_name",
                             title="Reddit posting activity by hour",
                             labels={"hour": "Hour of Day", "posts": "Number of Posts"})
                figure.update_layout(height=500, xaxis=dict(tickmode='linear', tick0=0, dtick=2))
                st.plotly_chart(figure, use_container_width=True)
            else:
                st.write("no data")

    if getothers == "Avg post length by author":
        st.subheader("Average Post Length by Author")
        averages = averagepostlenghtbyauthor(start=startdate.isoformat(), end=enddate.isoformat(), limit=30)
        column1, column2 = st.columns(2)
        with column1:
            st.markdown("#### 4chan avg length")
            dataa = averages.get("4chan", pd.DataFrame(columns=["author_name", "avg_len", "count"]))
            if not dataa.empty:
                st.dataframe(dataa)
                figurefour = px.bar(dataa.head(20), x="author_name", y="avg_len", title="4chan avg post length (top 20 authors)")
                figurefour.update_layout(xaxis_tickangle=-45, height=500)
                st.plotly_chart(figurefour, use_container_width=True)
            else:
                st.write("no data")
        with column2:
            st.markdown("#### Reddit avg length")
            ar = averages.get("Reddit", pd.DataFrame(columns=["author_name", "avg_len", "count"]))
            if not ar.empty:
                st.dataframe(ar)
                figure = px.bar(ar.head(20), x="author_name", y="avg_len", title="Reddit avg post length (top 20 authors)")
                figure.update_layout(xaxis_tickangle=-45, height=500)
                st.plotly_chart(figure, use_container_width=True)
            else:
                st.write("no data")