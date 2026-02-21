import streamlit as st
from datetime import datetime, timedelta
from temporal import rendertemporal
from toxicityovertime import rendertoxicity
from aitopicanalysis import renderaitopic

defaultdays = 30
st.set_page_config(page_title="Dashboard", layout="wide")
# this is the sidebar
st.sidebar.title("Dashboard")
panel = st.sidebar.selectbox(
    "Choose panel",
    ["Temporal Activity", "Toxicity Over Time", "AI Topic Toxicity"]
)
#filters to choose the sart date adn end date
startdate = st.sidebar.date_input(
    "Start date",
    datetime.utcnow() - timedelta(days=defaultdays)
)
enddate = st.sidebar.date_input("End date", datetime.utcnow())
# all side panels for the interactivity
if panel == "Temporal Activity":
    rendertemporal(startdate=startdate, enddate=enddate)
elif panel == "Toxicity Over Time":
    rendertoxicity(startdate=startdate, enddate=enddate)
elif panel == "AI Topic Toxicity":
    renderaitopic(startdate, enddate)
    