from src.configuration import Configuration
from src.fetch_data import DataFetcher
import pandas as pd


class Dims:
    def __init__(self, config: Configuration, fetcher: DataFetcher) -> None:
        self.config = config
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
    
    def dim_sessions(self, race = 'All') -> pd.DataFrame:
        """
        Show all of the information about races and qualification events.
        ________________________
        params:
            race -> Type of sessions we want to return.
        """
        if race not in ("All", "Race", "Qualifying"):
            raise ValueError(f"Race variable should be 'All','Race', or 'Qualifying' got: {race}")
        if race == 'All':
            session_df = self.fetcher.fetch_sessions()
            session_df['key'] = [x + str(y) for x,y in zip(session_df["location"], pd.to_datetime(session_df["date_start"]).dt.year)]
            return session_df
        
        if race == 'Race':
            session_df = self.fetcher.fetch_sessions()
            session_df = session_df[session_df['session_name'] == race]
            session_df['key'] = [x + str(y) for x,y in zip(session_df["location"], pd.to_datetime(session_df["date_start"]).dt.year)]
            return session_df
        
        if race == 'Qualifying':
            session_df = self.fetcher.fetch_sessions()
            session_df = session_df[session_df['session_name'] == race]
            session_df['key'] = [x + str(y) for x,y in zip(session_df["location"], pd.to_datetime(session_df["date_start"]).dt.year)]
            return session_df
