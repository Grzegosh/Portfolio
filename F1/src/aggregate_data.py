from src.configuration import Configuration
from src.fetch_data import DataFetcher
import pandas as pd
import numpy as np


class DataAggregator:
    def __init__(self, config: Configuration, fetcher: DataFetcher) -> None:
        self.config = Configuration('F1/src/config.cfg')
        self.fetcher = DataFetcher(self.config)

        
       

        

