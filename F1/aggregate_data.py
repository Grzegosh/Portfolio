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
        self.config = config
        self.fetcher = fetcher
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
        
    def get_racer_team_points(self, racer_team = "driver") -> pd.DataFrame:
        """
        Returns number of points gathered throughout the whole season.
        If racer_team = "driver" it will return the number of points for each driver.
        If racer_team = "team" it will return the number of points for the team.
        _________________________
        params:
            racer_team -> "driver" or "team".
        """
        if racer_team not in ("driver", "team"):
            raise ValueError(f"The racer_team should be equal to 'driver' or 'team' got:{racer_team}")
        
        #Data
        dim_session = self.dims.dim_sessions(race='Race')
        dim_driver_team = self.dims.dim_driver_team()
        fact_session_result = self.facts.fact_session_results()

        # Fill empty values
        fact_session_result['position'] = fact_session_result['position'].fillna(value=21)

        # Join results

        joined_results = fact_session_result.merge(
            dim_session,
            on = "session_key",
            how="inner"
        ).merge(
            dim_driver_team,
            on = ['driver_number','session_key'],
            how = "inner"
        )

        if racer_team == "driver":
            needed_cols = ["session_key","key","year","driver_number","date_end", "points"]
            joined_results = joined_results[needed_cols]
            joined_results_driver_spark = self.spark.createDataFrame(joined_results)
            window = Window.partitionBy("driver_number","year").orderBy("date_end")
            joined_results_driver_spark = joined_results_driver_spark.withColumn(
                "points_gained",
                f.sum("points").over(window)
            )
            joined_results_driver_pandas = joined_results_driver_spark.toPandas()

            return joined_results_driver_pandas
        
        else:
            needed_cols = ["session_key","key","year","driver_number","team_name","date_end", "points"]
            joined_results = joined_results[needed_cols]
            joined_results_team_spark = self.spark.createDataFrame(joined_results)
            window = Window.partitionBy("team_name","year").orderBy("date_end")
            joined_results_team_spark = joined_results_team_spark.withColumn(
                "points_gained",
                f.sum("points").over(window)
            )
            joined_results_team_pandas = joined_results_team_spark.toPandas()
            return joined_results_team_pandas

        
    

        

