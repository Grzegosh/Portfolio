import os
import sys

import numpy as np
import pandas as pd

sys.path.append("/Users/grzegorznaporowski/Desktop/Portfolio")

from air_pollution.src.utils.streamlit_utils.choropleth_map import Choropleth
from air_pollution.src.utils.sql_utils.connect_to_db import SQLManagement
import streamlit as st

class CreateMaps:
    def __init__(self, n_maps: int = 4, group_by: bool = True, mode="mean"):
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


    def create_maps(self):
        if self.mode not in ['mean', 'median']:
            raise ValueError("mode should be set to 'mean' or 'median'!")
        if self.groupby:
            self.data_frame = self.data_frame.groupby(
                'voivodeship'
            )
            if self.mode == "mean":
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
                color = self.pollutants[i]
            )

        self.map_creator.create_choropleth_map(
            data_frame=self.data_frame,
            loc='voivodeship',
            feat_key='properties.nazwa',
            color='score'
        )








