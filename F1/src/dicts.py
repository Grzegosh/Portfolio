from src.configuration import Configuration
from src.fetch_data import DataFetcher
import pandas as pd
from datetime import date



class Dicts:
    def __init__(self, config: Configuration, fetcher: DataFetcher) -> None:
        self.config = config
        self.fetcher = DataFetcher(self.config)

    def dict_world_champions(self) -> pd.DataFrame:
        """
        Dict that provides information about how many world champion title particular driver has.
        """
        data = [
        (44, 7),  # Hamilton
        (14, 2),  # Alonso
        (1, 4),   # Verstappen od 2025
        ]
        columns = ["driver_number", "titles_count"]
        return pd.DataFrame(data, columns=columns)