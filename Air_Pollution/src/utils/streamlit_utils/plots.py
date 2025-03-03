import sys
sys.path.append("/Users/grzegorznaporowski/Desktop/Portfolio")
from air_pollution.src.utils.sql_utils.connect_to_db import SQLManagement
from air_pollution.src.utils.streamlit_utils.maps import CreateMaps
import streamlit as st
import pandas as pd
import plotly.express as px
from sklearn.preprocessing import MinMaxScaler, StandardScaler


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
        data = self.data_frame[(self.data_frame['datetime'] >= self.date_slider[0]) &
                    (self.data_frame['datetime'] <= self.date_slider[1])]
        for i,pollutant in enumerate(self.pollutants):
            filltered_data = data[data[pollutant]>0]
            fig = px.histogram(
                data_frame=filltered_data,
                x=pollutant,
                height=400,
                title=f"Distribution of {pollutant}",
                nbins=250
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

    def create_line_plot(self):

        SCALER = StandardScaler()





        data = self.data_frame[(self.data_frame['datetime'] >= self.date_slider[0]) &
                               (self.data_frame['datetime'] <= self.date_slider[1])]


        dfs = []
        for pollutant in self.pollutants:
            filtered_df = data[data[pollutant]>0].copy()
            filtered_df[pollutant] = SCALER.fit_transform(filtered_df[[pollutant]])
            dfs.append(filtered_df)

        normalized_data = pd.concat(dfs)



        df_melted = normalized_data.melt(
            id_vars=['datetime'],
            value_vars=self.pollutants,
            var_name='pollutant',
            value_name='value'
        )

        df_melted_filtered = df_melted[df_melted['value']>0]

        df_melted_filtered_groupped = df_melted_filtered.groupby(['datetime', 'pollutant'])\
                                      .mean('value').reset_index()

        fig = px.line(
            df_melted_filtered_groupped,
            x='datetime',
            y='value',
            color='pollutant',
            title="Normalized pollutants value by date. (The lower value the better)",
            markers=True
        )

        fig.update_layout(
            xaxis_title='Date',
            yaxis_title='Normalized Value',
        )
        return st.plotly_chart(fig, key=f"chart_line_plot", use_container_width=True)






