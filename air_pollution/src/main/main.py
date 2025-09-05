import os
import sys

import numpy as np

sys.path.append("/Users/grzegorznaporowski/Desktop/Portfolio")


from air_pollution.src.utils.streamlit_utils.background import Background
from air_pollution.src.utils.streamlit_utils.sidebar import Sidebar
from air_pollution.src.utils.sql_utils.connect_to_db import SQLManagement
from air_pollution.src.utils.streamlit_utils.plots import CreatePlot
from air_pollution.src.utils.streamlit_utils.stats import Stats
from air_pollution.src.utils.streamlit_utils.choropleth_map import Choropleth
from air_pollution.src.utils.streamlit_utils.maps import CreateMaps
import plotly.express as px
import streamlit as st

#Creating necessary objects

BACKGROUND = Background()
SIDEBAR = Sidebar(msg="Navigation Panel",options=("About", "Maps", "Plots", "Statistics"))
DATA = SQLManagement()
CHORO = Choropleth()



#Creating background and Sidebar
#st.set_page_config(layout="wide", initial_sidebar_state="expanded")
side = SIDEBAR.create_sidebar()

#Displaying different things within different sidebar options
if side == "About":
    dir_path = os.path.dirname(__file__)
    path = os.path.abspath(os.path.join(dir_path, "..", "utils", "streamlit_utils", "about.txt"))
    st.header("Air Pollution Dashboard")
    with open(path, "r") as about:
        st.text(about.read())

if side == "Maps":
    df = DATA.read_data_from_sql(mode="view")
    min_date = df['datetime'].min()
    max_date = df['datetime'].max()

    col1, col2, col3 = st.columns(3)
    with col1:
        RANGE_ = st.slider("Select a date range: ", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    with col2:
        AGG_OPTION = st.selectbox("What aggregation you want to perform?",("Mean", "Median"))
    with col3:
        GROUP_OPTION = st.selectbox("Show data groupped by:", ("Voivodeship", "City"))

    if GROUP_OPTION == "Voivodeship":
        CreateMaps(mode=AGG_OPTION, date_slider=RANGE_).create_maps()
    else:
        CreateMaps(mode=AGG_OPTION, date_slider=RANGE_).create_scatter_map()

elif side == "Plots":
    PLOTS = CreatePlot()
    PLOTS.create_histogram()
    PLOTS.create_heatmap()
    PLOTS.create_line_plot()
else:
    STATS = Stats()
    STATS.calculate_stats()





