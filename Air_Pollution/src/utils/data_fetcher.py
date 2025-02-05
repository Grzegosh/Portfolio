import os
import pandas as pd
from Air_Pollution.src.utils.configuration import Configuration
from openaq import OpenAQ
import json
import ast
import s3fs


class DataFetcher:
    """
    Fetches all the data for particular region
    """
    def __init__(self):
        self.config = Configuration()
        self.api_key = self.config.get("OpenAQ", "api_key")
        self.coords = ast.literal_eval(self.config.get("OpenAQ", "coords"))


    def connect_to_client(self):
        """
        Function that provides a connection for OpenAQ client
        :return:
        """
        client = OpenAQ(api_key=self.api_key)

        if isinstance(client, OpenAQ):
            return client
        else:
            raise TypeError("Client is not a OpenAQ class")

    def fetch_locations(self, save=True):
        """
        Function that save json file with location id
        :param save: If True the file will be saved, if False, the function will return the dictionary itself.
        :return:
        JSON file with locations id or python dictionary.
        """
        location_dictionary = {}
        client = self.connect_to_client()
        results = client.locations.list(
            limit=1000,
            bbox=self.coords
        ).results

        for location in results:
            location_dictionary[location.id] = location.name

        if save:
            with open("locations.json", "w") as loc:
                json.dump(
                    location_dictionary,
                    loc,
                    sort_keys=True,
                    ensure_ascii=False
                )
        else:
            return location_dictionary


    def get_data(self, month: str = 1, year: str = 2025):
        """
        Function that saves the data for all the locations id in the locations.json file
        :return:
        Union DataFrame with all the data.
        """
        file_path = os.path.join(os.getcwd(), "locations.json")
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as file:
                locs = json.load(file)
        else:
            locs = self.fetch_locations(save=False)

        keys = locs.keys()

        all_data = pd.DataFrame()
        fs = s3fs.S3FileSystem(anon=True)

        for key in keys:
            file_list = fs.glob(f'openaq-data-archive/records/csv.gz/locationid={key}/year={year}/month={month}/*.csv.gz')

            dataframes = [pd.read_csv(fs.open(file),encoding="utf-8", compression="gzip") for file in file_list]
            all_data = pd.concat([all_data] + dataframes, ignore_index=True)

        return all_data.to_csv("pollution_data.csv", encoding="utf-8")


    def clean_data(self)->pd.DataFrame:
        """
        Function that cleans the data gathered from the "get_data" method
        :return:
        DataFrame ready for visualisation.
        """
        file_path = os.path.join(os.getcwd(), "pollution_data.csv")
        try:
            data = pd.read_csv(file_path)
        except FileNotFoundError:
            print(f"File: {file_path} does not exists.")
            return pd.DataFrame()

        data = data.drop("Unnamed: 0", axis=1)
        data = data[data['parameter'].isin(['pm10','pm25', 'o3', 'so2'])]
        pd.set_option("display.max_columns", None)
        pivot_data = data.pivot_table(
            index=['location_id', 'sensors_id', 'location', 'datetime','lat','lon'],
            columns='parameter',
            values='value'
        ).reset_index().fillna(0)
        return pivot_data

test = DataFetcher()
print(test.clean_data())























