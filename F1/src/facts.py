from src.configuration import Configuration
from src.fetch_data import DataFetcher
import pandas as pd


class Facts:
    def __init__(self, config: Configuration, fetcher: DataFetcher) -> None:
        self.config = config
        self.fetcher = DataFetcher(self.config)

    def fact_driver_overtakes(self) -> pd.DataFrame:
        """
        Returns information about driver overtakes.
        """
        overtakes_df = self.fetcher.fetch_overtakes()
        return overtakes_df[["session_key","overtaking_driver_number","overtaken_driver_number"]]
    

    def fact_session_results(self) -> pd.DataFrame:
        """
        Returns information about particular sessions result.
        """
        session_results = self.fetcher.fetch_session_results()
        return session_results
    
    def fact_starting_grid(self) -> pd.DataFrame:
        """
        Provides the starting grid for the upcoming race.
        """
        starting_grid = self.fetcher.fetch_starting_grid()
        return starting_grid
    
    def fact_pit_stops(self) -> pd.DataFrame:
        pits = self.fetcher.fetch_pit_stops()
        return pits
    
    def fact_laps(self, driver_number: int) -> pd.DataFrame:
        """
        Provides detailed information about individual laps.
        __________________
        driver_number:
            Nmber of particular driver.

        """
        laps = self.fetcher.fetch_laps(driver_number)
        return laps