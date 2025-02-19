import sys
sys.path.append("/Users/grzegorznaporowski/Desktop/Portfolio")

from air_pollution.src.utils.geojson import GetGeo
import plotly.express as px
import streamlit as st


class Choropleth:
    def __init__(self):
        self.geojson = GetGeo().get_geo_data()


    def create_choropleth_map(self, data_frame, loc, feat_key, color, title=None):
        """
        Function that creates a choropleth map based on a particular dataframe.
        :param data_frame: DataFrame from which we want to create a choropleth map.
        :param loc: DataFrame column name that contains the exact same names that properties key in geojson dict.
        :param feat_key: Path to field in GeoJSON feature object with which to match the values
        :param color: Either a name of a column in `data_frame`, or a pandas Series or
        :param title: Title of the map.
        array_like object.
        Plotly choropleth map.
        """
        fig = px.choropleth(
            data_frame=data_frame,
            geojson=self.geojson,
            locations=loc,
            featureidkey=feat_key,
            color = color,
            color_continuous_scale="RdYlGn_r",
            fitbounds="locations",
            basemap_visible=False,
            title=title
        )
        fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
            geo=dict(
                bgcolor='rgba(0,0,0,0)'
            )

        )


        return st.plotly_chart(fig, use_container_width=True)