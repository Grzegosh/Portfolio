import sys
sys.path.append("/Users/grzegorznaporowski/Desktop/Portfolio")
from air_pollution.src.utils.sql_utils.connect_to_db import SQLManagement
from air_pollution.src.utils.streamlit_utils.maps import CreateMaps
import streamlit as st
import pandas as pd
import plotly.express as px

class CreatePlot(CreateMaps):
    def __init__(self, date_slider=None):
        super().__init__(date_slider=date_slider)
        del self.map_creator
        del self.cols
        del self.groupby
        del self.mode

    def create_histogram(self):
        col1, col2 = st.columns(2)
        col3, col4 = st.columns(2)
        for i,pollutant in enumerate(self.pollutants):
            data = self.data_frame[self.data_frame[pollutant]>0]
            data = data[(data['datetime']>=self.date_slider[0])&
                   (data['datetime']<=self.date_slider[1])]
            fig = px.histogram(
                data_frame=data,
                x=pollutant,
                height=400,
                title=f"Distribution of {pollutant}",
                nbins=100
            )

            if i == 0:
                with col1:
                    st.plotly_chart(fig, key=f"chart_{pollutant}_col1")
            elif i == 1:
                with col2:
                    st.plotly_chart(fig, key=f"chart_{pollutant}_col2")
            elif i == 2:
                with col3:
                    st.plotly_chart(fig, key=f"chart_{pollutant}_col3")
            else:
                with col4:
                    st.plotly_chart(fig, key=f"chart_{pollutant}_col4")


    def create_heatmap(self):
        data = self.data_frame[(self.data_frame['datetime']>=self.date_slider[0])&
                               (self.data_frame['datetime']<=self.date_slider[1])]

        groupped_date = data.groupby('datetime').agg(self.mean_agg).reset_index()
        corr = groupped_date[self.pollutants].corr()
        fig = px.imshow(
            round(corr,3),
            text_auto=True,
            title="Correlation between pollutants."
        )
        return st.plotly_chart(fig, key=f"chart_heatmap", use_container_width=True)






