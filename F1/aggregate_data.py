from src.configuration import Configuration
from src.fetch_data import DataFetcher
import pandas as pd
import time
from src.facts import Facts
from src.dim import Dims
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f 
from datetime import timedelta


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
        if measure not in ("position", "avg", "std"):
            raise ValueError(f"Measure should be one of 'position','avg','std' got: {measure}")
        
        if race_type not in ("Race", "Qualifying"):
            raise ValueError(f"Race variable should be 'Race', or 'Qualifying' got: {race_type}")
        
        
        if race_type == "Race":
            def race_merged_data():     
                fact_session_results = self.facts.fact_session_results()
                dim_session_quali = self.dims.dim_sessions('Qualifying')
                dim_session_race = self.dims.dim_sessions('Race')
                dim_session_quali["date_end"] = pd.to_datetime(dim_session_quali["date_end"])
                dim_session_race["date_end"]   = pd.to_datetime(dim_session_race["date_end"])
                dim_session_quali['race_date'] = dim_session_quali['date_end'].dt.date + timedelta(days=1)
                def last_races_before(date):
                    prev = race_dates[race_dates < date]
                    return [d.date() for d in prev.tail(n_races)]
                race_dates = dim_session_race.sort_values("date_end")["date_end"].reset_index(drop=True)
                dim_session_quali["last5_race_dates"] = dim_session_quali["date_end"].apply(last_races_before)
                dim_sessions_quali_exploded = dim_session_quali.explode('last5_race_dates')
                dim_sessions_quali_exploded['last5_race_dates'] = pd.to_datetime(dim_sessions_quali_exploded['last5_race_dates'])
                merged = dim_sessions_quali_exploded.merge(
                    dim_session_race,
                    left_on='last5_race_dates',
                    right_on='date_end'
                ).merge(
                    fact_session_results,
                    left_on='session_key_y',
                    right_on='session_key'
                )
                return merged

            
            if measure == "position":
                race_merged_data = race_merged_data()
                race_merged_data = race_merged_data.groupby(
                    ['session_key_x', 'driver_number','key_x']
                    )['position'].apply(list).reset_index()

                for i in range(n_races):
                    col_name = f'last_{i+1}_race_position'
                    race_merged_data[col_name] = race_merged_data['position'].apply(lambda x: x[i] if len(x) > i else None)
                    final_data = race_merged_data.drop(columns=['position'])
                return final_data.rename(columns={"session_key_x":"session_key","key_x":"key"})

    
            elif measure == "avg":
                race_merged_data = race_merged_data()
                race_merged_data = race_merged_data.groupby(
                    ['session_key_x', 'driver_number','key_x']
                )['position'].mean().reset_index()
                col_name = f'avg_position_last_{n_races}_{race_type}'.lower()
                return race_merged_data.rename(columns={"session_key_x":"session_key","key_x":"key","position":col_name})
            
            else:
                race_merged_data = race_merged_data()
                race_merged_data = race_merged_data.groupby(
                    ['session_key_x', 'driver_number','key_x']
                )['position'].std().reset_index()
                col_name = f'std_position_last_{n_races}_{race_type}'.lower()
                return race_merged_data.rename(columns={"session_key_x":"session_key","key_x":"key","position":col_name})
        

           
                



        
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
        Calculates the median time of pit stop from last 5 races.
        """
        needed_cols = ["key", "session_key", "date_start"]
        fact_pit_stops = self.facts.fact_pit_stops()
        dim_session = self.dims.dim_sessions()
        dim_session = dim_session[dim_session["session_name"]=="Race"]
        dim_session = dim_session[needed_cols]
        pits_sessions = dim_session.merge(
        fact_pit_stops,
        on = "session_key"
        )
        pits_sessions_spark = self.spark.createDataFrame(pits_sessions)
        window_spec = Window\
            .partitionBy("driver_number")\
            .orderBy(f.col("date_start"))\
            .rowsBetween(-5, -1)
        
        pits_aggregated = pits_sessions_spark.groupBy(
            "driver_number", "key"
        ).agg(
            f.expr("percentile_approx(pit_duration, 0.5)").alias("median_pit_stop_time"),
            f.first("date_start").alias("date_start")
        )

        total_median = pits_aggregated.agg(
            f.expr("percentile_approx(median_pit_stop_time, 0.5)").alias("overall_median_pit_stop_time")
        ).collect()[0]["overall_median_pit_stop_time"]


        result = pits_aggregated.withColumn(
            "last_5_races_median_pit_stop_time",
            f.round(f.expr("percentile_approx(median_pit_stop_time, 0.5)").over(window_spec),3)
        ).select(
            "driver_number",
            "key",
            f.coalesce(
                f.col("last_5_races_median_pit_stop_time"), f.lit(total_median)
            ).alias("last_5_races_median_pit_stop_time")
        )

        return result.toPandas()
        
    

    def calculate_race_sequence_number(self) -> pd.DataFrame:
        """
        Provides information about the race number in the season
        """
        dim_sessions = self.dims.dim_sessions(race='Race')
        dim_sessions = dim_sessions.sort_values(['year','date_start'])
        dim_sessions['race_number'] = dim_sessions.groupby('year').cumcount() + 1
        return dim_sessions[['key','race_number']]
    

    def calculate_gap_to_best_team(self) -> pd.DataFrame:
        """
        Calculates points gap to the best team per session.
        """
        racer_team_points = self.get_racer_team_points(racer_team="team")
        max_per_session = racer_team_points.groupby(
            "key"
        ).agg(
            {'team_points_gained':'max'}
        ).reset_index()

        merged_results = racer_team_points.merge(
            max_per_session,
            on="key",
            how="inner",
            suffixes=["_team","_total"]
        )

        merged_results["gap_to_best_team"] = merged_results["team_points_gained_total"] - merged_results["team_points_gained_team"]

        return merged_results[["key","driver_number","gap_to_best_team"]]

    def calculate_total_wins(self, race_type: str) -> pd.DataFrame:
        """
        Calculates total number of wins for driver in the season.
        _______________________
        params
            race_type -> 'Race' or 'Qualifying'.
        """
        if race_type not in ("Race", "Qualifying"):
            raise ValueError(f"Race variable should be 'Race', or 'Qualifying' got: {race_type}")
        fact_session_results = self.facts.fact_session_results()
        dim_sessions = self.dims.dim_sessions()
        dim_sessions_race = dim_sessions[dim_sessions["session_name"] == race_type]
        fact_session_results_races = fact_session_results.merge(
            dim_sessions_race,
            on="session_key",
            how="inner"
        )[['driver_number', 'position', 'session_key', 'date_end', 'key']]
        fact_session_results_races['position'] = fact_session_results_races['position'].fillna(value=21)
        fact_session_results_races['is_winner'] = (fact_session_results_races['position'] == 1).astype(int)
        fact_session_results_races = fact_session_results_races.sort_values(['driver_number', 'date_end'])
        col_name = f"wins_before_session_{race_type}"
        fact_session_results_races[col_name] = (
        fact_session_results_races
        .groupby('driver_number')['is_winner']
        .cumsum()
        .shift(1)
        .fillna(0)
        .astype(int)
        )
        return fact_session_results_races[['driver_number', 'key', col_name]]
        
        

