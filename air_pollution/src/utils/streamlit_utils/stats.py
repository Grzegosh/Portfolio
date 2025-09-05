import sys

from posthog import poll_interval

sys.path.append("/Users/grzegorznaporowski/Desktop/Portfolio")


from air_pollution.src.utils.streamlit_utils.plots import CreatePlot
from sklearn.preprocessing import StandardScaler
import streamlit as st
import pandas as pd

class Stats(CreatePlot):
    def __init__(self):
        super().__init__()
        self.scaler = StandardScaler()

    def calculate_stats(self):
        data = self.data_frame[(self.data_frame['datetime'] >= self.date_slider[0]) &
                               (self.data_frame['datetime'] <= self.date_slider[1])]

        if self.spec == "Voivodeship":
            data = data[data['voivodeship']==self.filter]
        elif self.spec == "City":
            data = data[data['city']==self.filter]
        else:
            pass



        st.header("Basic descriptive statistics.")
        st.write(data[self.pollutants].describe().loc[['mean', 'std','max']])
        standarized_data = data.copy()
        for pollutant in self.pollutants:
            standarized_data[pollutant] = self.scaler.fit_transform(standarized_data[[pollutant]])
        standarized_data['score'] = standarized_data[self.pollutants].mean(axis=1)
        if self.spec == "Voivodeship":
            grouped_data = standarized_data.groupby('city')['score'].mean().reset_index()
            st.write(grouped_data)

