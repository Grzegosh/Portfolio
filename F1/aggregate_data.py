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
        
        else:
            def quali_merged_data():     
                fact_session_results = self.facts.fact_session_results()
                dim_session_quali = self.dims.dim_sessions('Qualifying')
                dim_session_quali["date_end"] = pd.to_datetime(dim_session_quali["date_end"])
                def last_quali_before(date):
                    prev = quali_dates[quali_dates < date]
                    return [d.date() for d in prev.tail(n_races)]
                quali_dates = dim_session_quali.sort_values("date_end")["date_end"].reset_index(drop=True)
                dim_session_quali["last5_quali_dates"] = dim_session_quali["date_end"].apply(last_quali_before)
                dim_sessions_quali_exploded = dim_session_quali.explode('last5_quali_dates')
                dim_sessions_quali_exploded['last5_quali_dates'] = pd.to_datetime(dim_sessions_quali_exploded['last5_quali_dates'])
                merged = dim_sessions_quali_exploded.merge(
                    dim_session_quali,
                    left_on='last5_quali_dates',
                    right_on='date_end'
                ).merge(
                    fact_session_results,
                    left_on='session_key_y',
                    right_on='session_key'
                )
                return merged
        
            if measure == "position":
                fact_session_results = self.facts.fact_session_results()
                quali_merged_data = quali_merged_data()
                quali_merged_data = quali_merged_data.groupby(
                    ['session_key_x', 'driver_number','key_x']
                    )['position'].apply(list).reset_index()
                for i in range(n_races):
                    col_name = f'last_{i+1}_quali_position'
                    quali_merged_data[col_name] = quali_merged_data['position'].apply(lambda x: x[i] if len(x) > i else None)

                final_data = quali_merged_data.drop(columns=['position'])
                final_data = final_data.merge(
                    fact_session_results[['session_key','driver_number','position']],
                    left_on=['session_key_x','driver_number'],
                    right_on=['session_key','driver_number'],
                    how='inner'
                )
                return final_data.rename(columns={"key_x":"key", "position":"current_quali_position"}).drop(columns=['session_key']).rename(
                    columns={"session_key_x":"session_key"}
                )

            elif measure == "avg":
                quali_merged_data = quali_merged_data()
                quali_merged_data = quali_merged_data.groupby(
                    ['session_key_x', 'driver_number','key_x']
                )['position'].mean().reset_index()
                col_name = f'avg_position_last_{n_races}_{race_type}'.lower()
                return quali_merged_data.rename(columns={"session_key_x":"session_key","key_x":"key","position":col_name})
            else:
                quali_merged_data = quali_merged_data()
                quali_merged_data = quali_merged_data.groupby(
                    ['session_key_x', 'driver_number','key_x']
                )['position'].std().reset_index()
                col_name = f'std_position_last_{n_races}_{race_type}'.lower()
                return quali_merged_data.rename(columns={"session_key_x":"session_key","key_x":"key","position":col_name})
            
      
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
        
        # Get the session results Data
        fact_session_results = self.facts.fact_session_results()
        fact_session_results['points'] = fact_session_results['points'].fillna(0)
        # Prepare qualification data dim
        dim_session_quali = self.dims.dim_sessions(race="Qualifying")
        dim_session_quali["date_end"] = pd.to_datetime(dim_session_quali["date_end"])
        dim_session_quali['race_date'] = pd.to_datetime(dim_session_quali['date_end'].dt.date + timedelta(days=1))
        dim_session_quali = dim_session_quali.sort_values("date_end")
        # Prepare race data dim
        dim_session_race = self.dims.dim_sessions(race="Race")
        dim_session_race["date_end"] = pd.to_datetime(dim_session_race["date_end"])
        dim_session_race = dim_session_race.sort_values("date_end")
  
        # Get all the dates of race before qualification
        merged_quali = dim_session_quali.merge(
            dim_session_race,
            how = 'cross'
        )
        merged_quali = merged_quali[(merged_quali["date_end_y"] < merged_quali["race_date"]) & (
            merged_quali["year_x"] == merged_quali["year_y"]
        )]
        
        # Driver Results
        if racer_team == "driver":
            result = merged_quali.merge(
                fact_session_results,
                left_on = 'session_key_y',
                right_on = 'session_key'
            )

            # Group The data by driver and key
            result = result.groupby(
                ['driver_number', 'key_x', 'session_key_x']
            ).agg(
                points_gained_driver = ('points', 'sum')
            ).reset_index()
            return result.rename(columns={"key_x":"key", "session_key_x":"session_key"})
        
        # Team Results
        else:
            dim_driver_team = self.dims.dim_driver_team()
            merged = merged_quali.merge(
                fact_session_results,
                left_on = 'session_key_y',
                right_on = 'session_key'
            ).merge(
                dim_driver_team,
                suffixes=['','_team'],
                on = ['driver_number','session_key']
            ).groupby(
                ['team_name','key_x', 'session_key_x']
            ).agg(
                points_gained_team = ('points', 'sum')
            ).reset_index(

            ).rename(
                columns={"key_x":"key", "session_key_x":"session_key"}
            )

            return merged

            

        
    def calculate_gap_to_teammate(self) -> pd.DataFrame:
        """
        Function that calculate points difference between drivers in the same team per session.
        """
        racer_team_points = self.get_racer_team_points()
        time.sleep(5)
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
        merged_filtered["points_gap_to_teammate"] = merged_filtered["points_gained_driver"] - merged_filtered["points_gained_driver_other"]
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
            points_gained_driver = ('points_gained_driver', 'max')
        ).reset_index()

        # Join to racer_points DataFrame

        merged_results = racer_points.merge(
            max_per_session,
            on="key",
            how="inner",
            suffixes=["","_total"]
        )

        # Calculate the difference to leader

        merged_results["gap_to_leader"] = merged_results["points_gained_driver_total"] - merged_results["points_gained_driver"]

        return merged_results[["key","driver_number","gap_to_leader"]]
    
    
    def calculate_pit_stop_efficiency(self) -> pd.DataFrame:

        """
        Calculates the median time of pit stop per driver.
        """
        # Get Necessary Data
        needed_cols = ["key", "session_key", "date_end"]
        fact_pit_stops = self.facts.fact_pit_stops()
        dim_session_race = self.dims.dim_sessions(race="Race")[needed_cols]
        dim_session_quali = self.dims.dim_sessions(race="Qualifying")[needed_cols]

        #Change date types
        dim_session_race['date_end'] = pd.to_datetime(dim_session_race['date_end'])
        dim_session_quali['date_end'] = pd.to_datetime(dim_session_quali['date_end'])

        # Add race date to quali sessions
        dim_session_quali["race_date"] = pd.to_datetime(dim_session_quali["date_end"].dt.date + timedelta(days=1))

        # Merge data with race data

        merged_quali = dim_session_quali.merge(
            dim_session_race,
            how = 'cross'
        )

        # Filter only previous races
        merged_quali = merged_quali[merged_quali['date_end_y'] < merged_quali['race_date']]

        # Merge with pit stop data

        merged_quali_pitstop = merged_quali.merge(
            fact_pit_stops,
            left_on = 'session_key_y',
            right_on='session_key'
        )
        
        result = merged_quali_pitstop.groupby(
            ['key_x', 'driver_number']
        ).agg(
            median_pit_stop_time = ('pit_duration', 'median')
        ).reset_index()

        return result.rename(columns={"key_x":"key"})


    def calculate_race_sequence_number(self) -> pd.DataFrame:
        """
        Provides information about the race number in the season
        """
        dim_sessions = self.dims.dim_sessions(race='Qualifying')
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
            {'points_gained_team':'max'}
        ).reset_index()

        merged_results = racer_team_points.merge(
            max_per_session,
            on="key",
            how="inner",
            suffixes=["","_total"]
        )

        merged_results["gap_to_best_team"] = merged_results["points_gained_team_total"] - merged_results["points_gained_team"]

        return merged_results[["key","gap_to_best_team"]]

    def calculate_total_wins(self, race_type: str) -> pd.DataFrame:
        """
        Calculates cumulative number of wins for each driver in the season.
        race_type -> 'Race' or 'Qualifying'
        """
        if race_type not in ("Race", "Qualifying"):
            raise ValueError(f"Race variable should be 'Race', or 'Qualifying' got: {race_type}")
        
        # Get necessary Data
        fact_session_results = self.facts.fact_session_results()
        dim_session_quali = self.dims.dim_sessions('Qualifying')
        dim_session_race = self.dims.dim_sessions('Race')

        # Correct date formats
        dim_session_quali['date_end'] = pd.to_datetime(dim_session_quali['date_end'])
        dim_session_race['date_end'] = pd.to_datetime(dim_session_race['date_end'])

        # Sort sessions
        dim_session_quali = dim_session_quali.sort_values('date_end')
        dim_session_race = dim_session_race.sort_values('date_end')

        # ==========================
        #        QUALIFYING
        # ==========================
        if race_type == 'Qualifying':
            # Merge
            merged = dim_session_quali.merge(
                fact_session_results,
                on='session_key'
            )

            # Flag winner
            merged['is_winner'] = merged['position'].apply(lambda x: 1 if x == 1 else 0)

            # Aggregate wins per session
            merged_result = merged.groupby(
                ['session_key', 'key', 'driver_number', 'date_end']
            ).agg(
                wins=('is_winner', 'sum')
            ).reset_index()

            # Sort for cumulative
            merged_result = merged_result.sort_values(['driver_number', 'date_end'])

            # Cumulative wins
            merged_result['cumulative_wins_quali'] = (
                merged_result.groupby('driver_number')['wins'].cumsum()
            )

            return merged_result[['session_key', 'key', 'driver_number', 'cumulative_wins_quali']]

        # ==========================
        #           RACE
        # ==========================
        else:
            # Add race date (your logic)
            dim_session_quali['race_date'] = dim_session_quali['date_end'].dt.date + timedelta(days=1)

            # Cross join
            merged_sessions = dim_session_quali.merge(
                dim_session_race,
                how='cross'
            )

            # Filter by logic: race after quali, same year
            merged_sessions = merged_sessions[
                (merged_sessions['date_end_y'] > merged_sessions['date_end_x']) &
                (merged_sessions['year_x'] == merged_sessions['year_y'])
            ]

            # Merge with actual results
            merged_sessions_race = merged_sessions.merge(
                fact_session_results,
                left_on='session_key_y',
                right_on='session_key'
            )

            # Winner flag
            merged_sessions_race['is_winner'] = merged_sessions_race['position'].apply(lambda x: 1 if x == 1 else 0)

            # Aggregate wins per race session
            groupped_reslts = merged_sessions_race.groupby(
                ['session_key_x', 'key_x', 'driver_number', 'date_end_y']
            ).agg(
                wins=('is_winner', 'sum')
            ).reset_index()

            # Sort for cumulative sum
            groupped_reslts = groupped_reslts.sort_values(['driver_number', 'date_end_y'])

            # Add cumulative wins
            groupped_reslts['cumulative_wins_race'] = (
                groupped_reslts.groupby('driver_number')['wins'].cumsum()
            )

            # Rename for consistency
            groupped_reslts = groupped_reslts.rename(
                columns={
                    "session_key_x": "session_key",
                    "key_x": "key"
                }
            )

            return groupped_reslts[['session_key', 'key', 'driver_number', 'cumulative_wins_race']]

            


        

        

