import os
import requests
import pandas as pd 
from src.configuration import Configuration
import datetime

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