from src.configuration import Configuration
from src.fetch_data import DataFetcher
import pandas as pd
import numpy as np
from src.facts import Facts
from src.dim import Dims
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f 


class DataAggregator:
    def __init__(self, config: Configuration, fetcher: DataFetcher) -> None:
        self.config = Configuration('F1/src/config.cfg')
        self.fetcher = DataFetcher(self.config)
        self.facts = Facts(self.config, self.fetcher)
        self.dims = Dims(self.config, self.fetcher)
        self.spark = SparkSession.builder.appName("session").master("local").getOrCreate()

    def get_last_races_result(self, n_races: int, race_type: str) -> pd.DataFrame:
        """
        For every driver and sessions returns last n_races result.
        _______________________________________
        Params:
            n_races -> Number of races from which we will take results.
            race_type -> Race or Qualifying.
        """
        fact_session_results = self.facts.fact_session_results()
        dim_sessions = self.dims.dim_sessions()
        dim_sessions_race = dim_sessions[dim_sessions["session_name"] == race_type]

        fact_session_results_races = fact_session_results.merge(
            dim_sessions_race,
            on="session_key",
            how="inner"
        )[['driver_number','position','session_key','date_end', 'key']]

        fact_session_results_races['position'] = fact_session_results_races['position'].fillna(value=21) # For disqualified racers
        fact_session_results_races = fact_session_results_races.sort_values(['date_end','driver_number'], ascending=False)
        spark_fact = self.spark.createDataFrame(fact_session_results_races)
        window = Window.partitionBy("driver_number").orderBy(f.col("date_end")).rowsBetween(-n_races, -1)
        df_with_last5 = spark_fact.withColumn(
        "last_5_positions",
        f.collect_list("position").over(window)
        )
        for i in range(n_races):
            df_with_last5 = df_with_last5.withColumn(
            f"last_{str(race_type).lower()}_pos_{i+1}",
            f.expr(f"element_at(last_5_positions, {i+1})")
        )
        df_with_last5_pd = df_with_last5.toPandas()
        race_columns = [col for col in df_with_last5_pd.columns if str(race_type).lower() in col]
        other_cols = ['driver_number','position','session_key','date_end','key']
        [df_with_last5_pd[col].fillna(value=99,inplace=True) for col in race_columns]
        joined_cols = other_cols + race_columns
        return df_with_last5_pd[joined_cols]
        




        
    

        

