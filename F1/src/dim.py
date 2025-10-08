from src.configuration import Configuration
from src.fetch_data import DataFetcher
import pandas as pd


class Dims:
    def __init__(self, config: Configuration, fetcher: DataFetcher) -> None:
        self.config = Configuration('F1/src/config.cfg')
        self.fetcher = DataFetcher(self.config)

    def dim_driver_number(self) -> pd.DataFrame:
        """
        Maps driver number to his number.
        """
        drivers_df = self.fetcher.fetch_drivers()
        return drivers_df[["session_key", "driver_number", "full_name"]]
    
    def dim_driver_team(self) -> pd.DataFrame:
        """
        Maps driver number to his team.
        """
        drivers_df = self.fetcher.fetch_drivers()
        return drivers_df[["session_key", "driver_number", "team_name"]]
    
    def dim_sessions(self) -> pd.DataFrame:
        """
        Show all of the information about races and qualification events.
        """
        session_df = self.fetcher.fetch_sessions()
        return session_df
