import os
import sys

import numpy as np

sys.path.append("/Users/grzegorznaporowski/Desktop/Portfolio")


from air_pollution.src.utils.streamlit_utils.background import Background
from air_pollution.src.utils.streamlit_utils.sidebar import Sidebar
from air_pollution.src.utils.sql_utils.connect_to_db import SQLManagement
from air_pollution.src.utils.streamlit_utils.choropleth_map import Choropleth
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

#Creating necessary objects

BACKGROUND = Background()
SIDEBAR = Sidebar(msg="Navigation Panel",options=("About", "Visualisation", "Statistics"))
DATA = SQLManagement()
CHORO = Choropleth()


#Creating background and Sidebar
st.set_page_config(layout="wide", initial_sidebar_state="expanded")
st.markdown(BACKGROUND.use_css(), unsafe_allow_html=True)
side = SIDEBAR.create_sidebar()

#Displaying different things within different sidebar options
if side == "About":
    dir_path = os.path.dirname(__file__)
    path = os.path.abspath(os.path.join(dir_path, "..", "utils", "streamlit_utils", "about.txt"))
    st.header("Air Pollution Dashboard")
    with open(path, "r") as about:
        st.write(about.read())

if side == "Visualisation":
    df = DATA.read_data_from_sql(mode="view")
    groupped_data = df.groupby('voivodeship')['pm10'].mean().reset_index()
    col1, col2 = st.columns(2)
    with col1:
        CHORO.create_choropleth_map(
            data_frame=groupped_data,
            loc="voivodeship",
            feat_key="properties.nazwa",
            color="pm10"
        )






