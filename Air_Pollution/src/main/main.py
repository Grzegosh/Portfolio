import os
import sys
sys.path.append("/Users/grzegorznaporowski/Desktop/Portfolio")


from air_Pollution.src.utils.streamlit_utils.background import Background
from air_Pollution.src.utils.streamlit_utils.sidebar import Sidebar
from air_Pollution.src.utils.sql_utils.connect_to_db import SQLManagement
import streamlit as st
import pandas as pd

#Creating necessary objects

BACKGROUND = Background()
SIDEBAR = Sidebar(msg="Navigation Panel",options=("About", "Visualisation", "Statistics"))
DATA = SQLManagement()

#Creating background and Sidebar
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
    df = DATA.read_data_from_sql()
    groupped_data = pd.DataFrame(df.groupby(['lat','lon'])['pm10'].median().reset_index())
    st.map(groupped_data, latitude="lat", longitude="lon", size="pm10")s





