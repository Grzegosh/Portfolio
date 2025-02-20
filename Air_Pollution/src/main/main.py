import os
import sys

import numpy as np

sys.path.append("/Users/grzegorznaporowski/Desktop/Portfolio")


from air_pollution.src.utils.streamlit_utils.background import Background
from air_pollution.src.utils.streamlit_utils.sidebar import Sidebar
from air_pollution.src.utils.sql_utils.connect_to_db import SQLManagement
from air_pollution.src.utils.streamlit_utils.choropleth_map import Choropleth
from air_pollution.src.utils.streamlit_utils.maps import CreateMaps
import streamlit as st
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

#Creating necessary objects

BACKGROUND = Background()
SIDEBAR = Sidebar(msg="Navigation Panel",options=("About", "Maps", "Statistics"))
DATA = SQLManagement()
CHORO = Choropleth()
SCALER = MinMaxScaler()


#Creating background and Sidebar
st.set_page_config(layout="wide", initial_sidebar_state="expanded")
side = SIDEBAR.create_sidebar()

#Displaying different things within different sidebar options
if side == "About":
    dir_path = os.path.dirname(__file__)
    path = os.path.abspath(os.path.join(dir_path, "..", "utils", "streamlit_utils", "about.txt"))
    st.header("Air Pollution Dashboard")
    with open(path, "r") as about:
        st.text(about.read())

if side == "Maps":
    pollutants = ['o3', 'pm10', 'pm25', 'so2']
    df = DATA.read_data_from_sql(mode="view")

    min_date = df['datetime'].min()
    max_date = df['datetime'].max()

    col1, col2 = st.columns(2)
    with col1:
        RANGE_ = st.slider("Select a date range: ", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    with col2:
        AGG_OPTION = st.selectbox("What aggregation you want to perform?",("Mean", "Median"))

    if AGG_OPTION == "Mean":
        agg_dict = {pollutant: 'mean' for pollutant in pollutants}
        df = df[(df['datetime']>=RANGE_[0])&(df['datetime']<=RANGE_[1])]
        if len(df) == 0:
            st.error("Pick a greater timerange! It seem's like we don't have data for that timerange.")

        groupped_data = df.groupby('voivodeship').agg(agg_dict).reset_index()
        groupped_data['score'] = groupped_data[pollutants].mean(axis=1)
    else:
        agg_dict = {
            pollutant: lambda x: x[x != 0].median() if x[x != 0].count() > 0 else 0
            for pollutant in pollutants
        }
        groupped_data = df.groupby('voivodeship').agg(agg_dict).reset_index()
        groupped_data['score'] = groupped_data[pollutants].mean(axis=1)



    col3, col4 = st.columns(2)

    with col3:
        fig = CHORO.create_choropleth_map(
            data_frame=groupped_data,
            loc='voivodeship',
            feat_key='properties.nazwa',
            color='o3',
            title=f"{AGG_OPTION} value of o3 between {RANGE_[0]} and {RANGE_[1]}"
        )

    with col4:
        fig = CHORO.create_choropleth_map(
            data_frame=groupped_data,
            loc='voivodeship',
            feat_key='properties.nazwa',
            color='so2',
            title=f"{AGG_OPTION} value of so2 between {RANGE_[0]} and {RANGE_[1]}"
        )

    col5, col6 = st.columns(2)

    with col5:
        fig = CHORO.create_choropleth_map(
            data_frame=groupped_data,
            loc='voivodeship',
            feat_key='properties.nazwa',
            color='pm10',
            title=f"{AGG_OPTION} value of pm10 between {RANGE_[0]} and {RANGE_[1]}"
        )

    with col6:
        fig = CHORO.create_choropleth_map(
            data_frame=groupped_data,
            loc='voivodeship',
            feat_key='properties.nazwa',
            color='pm25',
            title=f"{AGG_OPTION} value of pm25 between {RANGE_[0]} and {RANGE_[1]}"
        )

    CHORO.create_choropleth_map(
        data_frame=groupped_data,
        loc='voivodeship',
        feat_key='properties.nazwa',
        color='score',
        title=f"General score of air pollution in Poland based on {AGG_OPTION} metric. (Higher score means greater pollution.)"
    )
    CreateMaps(mode="mean").create_maps()





