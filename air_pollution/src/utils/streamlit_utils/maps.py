import os
import sys

import numpy as np
import pandas as pd

sys.path.append("/Users/grzegorznaporowski/Desktop/Portfolio")

from air_pollution.src.utils.streamlit_utils.choropleth_map import Choropleth
from air_pollution.src.utils.sql_utils.connect_to_db import SQLManagement
import streamlit as st
import plotly.express as px
class CreateMaps:
    def __init__(self, n_maps: int = 4, group_by: bool = True, mode="mean", date_slider=None):
        self.n_maps = n_maps
        self.groupby = group_by
        self.data_frame = SQLManagement().read_data_from_sql(mode="view")
        self.pollutants = ['o3', 'pm10', 'pm25', 'so2']
        self.mean_agg = {pollutant:'mean' for pollutant in self.pollutants}
        self.median_agg = {
            pollutant: lambda x: x[x != 0].median() if x[x != 0].count() > 0 else 0
            for pollutant in self.pollutants
        }
        self.mode = mode
        self.cols = st.columns(4)
        self.map_creator = Choropleth()
        self.date_min = self.data_frame['datetime'].min()
        self.date_slider = date_slider


    def create_maps(self):
        if self.mode not in ['Mean', 'Median']:
            raise ValueError("mode should be set to 'Mean' or 'Median'!")
        self.data_frame = self.data_frame[(self.data_frame['datetime']>=self.date_slider[0])&
                                          (self.data_frame['datetime']<=self.date_slider[1])]
        if self.groupby:
            self.data_frame = self.data_frame.groupby(
                'voivodeship'
            )
            if self.mode == "Mean":
                self.data_frame = self.data_frame.agg(self.mean_agg).reset_index()
                self.data_frame['score'] = self.data_frame[self.pollutants].mean(axis=1)
            else:
                self.data_frame = self.data_frame.agg(self.median_agg).reset_index()
                self.data_frame['score'] = self.data_frame[self.pollutants].mean(axis=1)

        for i in range(len(self.pollutants)):
            self.map_creator.create_choropleth_map(
                data_frame=self.data_frame,
                loc='voivodeship',
                feat_key='properties.nazwa',
                color = self.pollutants[i],
                title=f"{self.mode} value of {self.pollutants[i]} between {self.date_slider[0]} and {self.date_slider[1]}"
            )

        self.map_creator.create_choropleth_map(
            data_frame=self.data_frame,
            loc='voivodeship',
            feat_key='properties.nazwa',
            color='score',
            title=f"General score of air pollution in Poland based on {self.mode} metric. (Higher score means greater pollution.)"
        )


    def create_scatter_map(self):
        if self.mode not in ['Mean', 'Median']:
            raise ValueError("mode should be set to 'Mean' or 'Median'!")

        agg_opt = st.selectbox("What pollutant are you intrested in?", self.pollutants)
        self.data_frame = self.data_frame[(self.data_frame['datetime']>=self.date_slider[0])&
                                          (self.data_frame['datetime']<=self.date_slider[1])]

        if self.groupby:
            self.data_frame = self.data_frame.groupby(
                ["city", "lat", "lon"]
            )
            if self.mode == "Mean":
                self.data_frame = self.data_frame.agg(self.mean_agg).reset_index()
                self.data_frame['score'] = self.data_frame[self.pollutants].mean(axis=1)
                fig = px.scatter_map(
                    data_frame=self.data_frame,
                    lat="lat",
                    lon="lon",
                    hover_name="city",
                    size=agg_opt,
                    title=f"{self.mode} value of {agg_opt} between {self.date_slider[0]} and {self.date_slider[1]}",
                    zoom=4
                )

                fig2 = px.scatter_map(
                    data_frame=self.data_frame,
                    lat="lat",
                    lon="lon",
                    hover_name="city",
                    size="score",
                    title=f"General score of air pollution in Poland based on {self.mode} metric. (Higher score means greater pollution.)",
                    zoom=4

                )
                return st.plotly_chart(fig), st.plotly_chart(fig2)
            else:
                self.data_frame = self.data_frame.agg(self.median_agg).reset_index()
                self.data_frame['score'] = self.data_frame[self.pollutants].mean(axis=1)
                fig = px.scatter_map(
                    data_frame=self.data_frame,
                    lat="lat",
                    lon="lon",
                    hover_name="city",
                    size=agg_opt,
                    title=f"{self.mode} value of {agg_opt} between {self.date_slider[0]} and {self.date_slider[1]}",
                    zoom=4
                )
                fig2 = px.scatter_map(
                    data_frame=self.data_frame,
                    lat="lat",
                    lon="lon",
                    hover_name="city",
                    size="score",
                    title=f"General score of air pollution in Poland based on {self.mode} metric. (Higher score means greater pollution.)",
                    zoom=4

                )
                return st.plotly_chart(fig), st.plotly_chart(fig2)







