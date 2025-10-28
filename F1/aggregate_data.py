from src.configuration import Configuration
from src.fetch_data import DataFetcher
import pandas as pd
import time
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
        
    def get_last_races_result(self, n_races: int, race_type: str, measure: str = "position") -> pd.DataFrame:
        """
        For every driver and session returns last n_races results or statistics.

        _______________________________________
        Params:
            n_races -> Number of races from which we will take results.
            race_type -> 'Race' or 'Qualifying'.
            measure -> One of:
                - "position" -> returns last n race positions (default)
                - "avg" -> returns average position from last n races
                - "std" -> returns standard deviation of positions from last n races
        """
        fact_session_results = self.facts.fact_session_results()
        dim_sessions = self.dims.dim_sessions()
        dim_sessions_race = dim_sessions[dim_sessions["session_name"] == race_type]
        fact_session_results_races = fact_session_results.merge(
            dim_sessions_race,
            on="session_key",
            how="inner"
        )[['driver_number', 'position', 'session_key', 'date_end', 'key']]
        fact_session_results_races['position'] = fact_session_results_races['position'].fillna(value=21)
        fact_session_results_races = fact_session_results_races.sort_values(['date_end', 'driver_number'], ascending=False)
        spark_fact = self.spark.createDataFrame(fact_session_results_races)
        window = Window.partitionBy("driver_number").orderBy(f.col("date_end")).rowsBetween(-n_races, -1)
        df_with_lastn = spark_fact.withColumn(
            "last_n_positions",
            f.collect_list("position").over(window)
        )
        if measure == "position":
            for i in range(n_races):
                df_with_lastn = df_with_lastn.withColumn(
                    f"last_{str(race_type).lower()}_pos_{i+1}",
                    f.expr(f"element_at(last_n_positions, {i+1})")
                )
            df_pd = df_with_lastn.toPandas()
            race_columns = [col for col in df_pd.columns if str(race_type).lower() in col]
            other_cols = ['driver_number', 'position', 'key']
            joined_cols = other_cols + race_columns
            df_pd.dropna(subset=race_columns, inplace=True)
            return df_pd[joined_cols]

        elif measure == "avg":
            df_with_lastn = df_with_lastn.withColumn(
                f"avg_last_{n_races}_{str(race_type).lower()}",
                f.expr(f"aggregate(last_n_positions, 0D, (acc, x) -> acc + x, acc -> acc / size(last_n_positions))")
            )
            df_pd = df_with_lastn.select(
                "driver_number", "key",
                f"avg_last_{n_races}_{str(race_type).lower()}"
            ).toPandas()
            df_pd.dropna(inplace=True)
            return df_pd
        elif measure == "std":
            df_with_lastn = df_with_lastn.withColumn(
                f"std_last_{n_races}_{str(race_type).lower()}",
                f.expr(
                    "sqrt(aggregate(last_n_positions, 0D, "
                    "(acc, x) -> acc + pow(x - aggregate(last_n_positions, 0D, (a,b) -> a+b, a -> a / size(last_n_positions)), 2), "
                    "acc -> acc / size(last_n_positions)))"
                )
            )
            df_pd =  df_with_lastn.select("driver_number", "key",
                                        f"std_last_{n_races}_{str(race_type).lower()}").toPandas()
            df_pd.dropna(inplace=True)
            return df_pd

        else:
            raise ValueError("Invalid 'measure' parameter. Choose from: 'position', 'avg', 'std'.")
    
        
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
        dim_session = self.dims.dim_sessions(race='Race')
        dim_driver_team = self.dims.dim_driver_team()
        fact_session_result = self.facts.fact_session_results()
        fact_session_result['position'] = fact_session_result['position'].fillna(value=21)
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

            return joined_results_driver_pandas[["session_key","driver_number","points_gained","key"]]
        
        else:
            needed_cols = ["session_key","key","year","driver_number","team_name","date_end", "points"]
            joined_results = joined_results[needed_cols]
            joined_results_team_spark = self.spark.createDataFrame(joined_results)
            window = Window.partitionBy("team_name","year").orderBy("date_end")
            joined_results_team_spark = joined_results_team_spark.withColumn(
                "team_points_gained",
                f.sum("points").over(window)
            )
            joined_results_team_pandas = joined_results_team_spark.toPandas()
            return joined_results_team_pandas[["key","driver_number","team_name","team_points_gained"]]

        
    def calculate_gap_to_teammate(self) -> pd.DataFrame:
        """
        Function that calculate points difference between drivers in the same team per session.
        """
        racer_team_points = self.get_racer_team_points()
        dim_driver_team = self.dims.dim_driver_team()
        join_result = racer_team_points.merge(
            dim_driver_team,
            on=["driver_number","session_key"]
        )
        time.sleep(10)

        merged = join_result.merge(
            join_result,
            on=["session_key","team_name"],
            suffixes=["","_other"]
        )
        merged_filtered = merged[merged["driver_number"] != merged["driver_number_other"]]
        merged_filtered["points_gap_to_teammate"] = merged_filtered["points_gained"] - merged_filtered["points_gained_other"]
        return merged_filtered[["key","driver_number","points_gap_to_teammate"]]


    def calculate_gap_to_leader(self):
        """
        Calculates points gap to the leader per session.
        """
        racer_points = self.get_racer_team_points(racer_team="driver")

        # Calculate max points per session

        max_per_session = racer_points.groupby(
            "key"
        ).agg(
            {'points_gained':'max'}
        ).reset_index()

        # Join to racer_points DataFrame

        merged_results = racer_points.merge(
            max_per_session,
            on="key",
            how="inner",
            suffixes=["_racer","_total"]
        )

        # Calculate the difference to leader

        merged_results["gap_to_leader"] = merged_results["points_gained_total"] - merged_results["points_gained_racer"]

        return merged_results[["key","driver_number","gap_to_leader"]]
    
    def calculate_pit_stop_efficiency(self) -> pd.DataFrame:
        """
        Calculates average pit stop time, and number of pit stops per driver per session.
        """
        needed_cols = ["key", "session_key"]
        fact_pit_stops = self.facts.fact_pit_stops()
        dim_session = self.dims.dim_sessions()
        dim_session = dim_session[dim_session["session_name"]=="Race"]
        dim_session = dim_session[needed_cols]
        merged = dim_session.merge(
        fact_pit_stops,
        on = "session_key"
        )
        data = merged.groupby(["key", "driver_number"]).agg(
        pit_duration_avg=("pit_duration", "mean"),
        pit_stop_count=("driver_number", "count")
        ).reset_index()
        return data
    

    ## TODO: Dodać informacje o numerze wyscigu w sezonie oraz gap to best team





    

        

