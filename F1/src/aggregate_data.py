from src.configuration import Configuration
from src.fetch_data import DataFetcher
import datetime
import pandas as pd
import numpy as np
import time

class DataAggregator:
    def __init__(self, config: Configuration, fetcher: DataFetcher) -> None:
        self.config = Configuration('F1/src/config.cfg')
        self.fetcher = DataFetcher(self.config)

    def fact_driver_overtakes(self) -> pd.DataFrame:
        """
        Aggregates overtaking statistics for the current season.
        """
        overtakes_df = self.fetcher.fetch_overtakes()
        time.sleep(5)  # To avoid hitting API rate limits
        drivers_df = self.fetcher.fetch_drivers()
        drivers_df = drivers_df[['session_key', 'driver_number', 'team_name', 'full_name']]
        # time.sleep(10)
        # sessions_with_overtakes = ['Race', 'Sprint'] # Only sessions with races and overtakes.
        # sessions_df = self.fetcher.fetch_sessions()
        # sessions_df = sessions_df[sessions_df['session_name'].isin(sessions_with_overtakes)]
        # sessions_df = sessions_df[['session_key', 'location', 'date_start']]
        
        

