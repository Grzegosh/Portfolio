import os
import requests
import pandas as pd 
from src.configuration import Configuration
import datetime
import time

class DataFetcher:
    def __init__(self, config: Configuration) -> None:
        self.config = config

    def fetch_data(self, url: str, params: dict = {}) -> pd.DataFrame:
        """
        Fetch data from a given URL and return it as a pandas DataFrame.

        Args:
            url (str): The API endpoint to fetch data from.
            params (dict): Optional query parameters for the request.
        """
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()
        return pd.DataFrame(data)
    
    # Funtions to fetch specific datasets

    def fetch_drivers(self) -> pd.DataFrame:
        frame = self.fetch_data(self.config.drivers_url)
        needed_columns = ['session_key', 'driver_number', 'first_name', 'last_name', 'full_name', 'name_acronym',
                          'team_name']
        return frame[needed_columns]
    
    def fetch_pit_stops(self) -> pd.DataFrame:
        frame = self.fetch_data(self.config.pit_url)
        needed_columns = ['session_key', 'pit_duration', 'driver_number']
        whole_df = frame[needed_columns]
        return whole_df[whole_df['pit_duration'].notnull()]
    
    def fetch_sessions(self) -> pd.DataFrame:
        current_season_start = datetime.datetime.strptime(self.config.season_start_date, '%Y-%m-%d').date()
        frame = self.fetch_data(self.config.sessions_url)
        frame['date_start'] = pd.to_datetime(frame['date_start']).dt.date
        frame['date_end'] = pd.to_datetime(frame['date_end']).dt.date
        frame['is_current_season'] = frame['date_start'].apply(lambda d: 1 if current_season_start <= d else 0)
        needed_columns = ['session_key', 'location','date_start', 'date_end', 'session_name', 'country_code',
                          'country_name', 'year', 'is_current_season']
        return frame[needed_columns]
        
    
    def fetch_starting_grid(self) -> pd.DataFrame:
        frame = self.fetch_data(self.config.starting_grid_url)
        needed_columns = ['position','driver_number','lap_duration','session_key']
        return frame[needed_columns]
    
    def fetch_overtakes(self) -> pd.DataFrame:
        frame = self.fetch_data(self.config.overtakes_url)
        needed_columns = ['session_key', 'overtaking_driver_number', 'overtaken_driver_number', 'date', 'position']
        all_data =  frame[needed_columns]
        all_data['date'] = pd.to_datetime(all_data['date'], format='mixed').dt.date
        return all_data

    def fetch_session_results(self) -> pd.DataFrame:
        return self.fetch_data(self.config.session_results_url)
    
    def fetch_laps(self, driver_number: int) -> pd.DataFrame:
        return self.fetch_data(f"{self.config.laps_url}?driver_number={driver_number}")
    
    def fetch_weather(self, race_type: str) -> pd.DataFrame:
        sessions = self.fetch_sessions()
        frame = []
        if race_type == "Race":
            filtered_sessions = sessions[sessions['session_name'] == 'Race']
            unique_session_keys = list(filtered_sessions['session_key'].unique())
            for key in unique_session_keys:
                data = self.fetch_data(f"{self.config.weather_url}?session_key={key}")
                time.sleep(5)
                frame.append(data)
            # Agregate Data
            merged_frame = pd.concat(frame, ignore_index=True)
            groupped_frame = (
                merged_frame.groupby("session_key")
                .agg(
                    wind_direction_race_avg = ("wind_direction", "mean"),
                    wind_speed_race_avg = ("wind_speed", "mean"),
                    has_rainfall_race = ("rainfall", "max"),
                    track_temperature_race_avg = ("track_temperature", "mean"),
                    air_temperature_race_avg = ("air_temperature", "mean"),
                    humidity_race_avg = ("humidity", "mean"),
                    pressure_race_avg = ("pressure", "mean")

                )
            )
            return groupped_frame.reset_index()


        elif race_type == "Qualifying":
            filtered_sessions = sessions[sessions['session_name'] == 'Qualifying']
            unique_session_keys = list(filtered_sessions['session_key'].unique())
            for key in unique_session_keys:
                data = self.fetch_data(f"{self.config.weather_url}?session_key={key}")
                time.sleep(5)
                frame.append(data)
            # Agregate Data
            merged_frame = pd.concat(frame, ignore_index=True)
            groupped_frame = (
                merged_frame.groupby("session_key")
                .agg(
                    wind_direction_qualifying_avg = ("wind_direction", "mean"),
                    wind_speed_qualifying_avg = ("wind_speed", "mean"),
                    has_rainfall_qualifying = ("rainfall", "max"),
                    track_temperature_qualifying_avg = ("track_temperature", "mean"),
                    air_temperature_qualifying_avg = ("air_temperature", "mean"),
                    humidity_qualifying_avg = ("humidity", "mean"),
                    pressure_qualifying_avg = ("pressure", "mean")
                )
            )
            return groupped_frame.reset_index()
        else:
            raise ValueError(f"Race type should be 'Race' or 'Qualifying', got: {race_type}")
        
 
        
