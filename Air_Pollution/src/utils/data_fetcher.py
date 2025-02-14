import sys
sys.path.append("/Users/grzegorznaporowski/Desktop/Portfolio")

import os
import pandas as pd
import datetime
from air_pollution.src.utils.configuration import Configuration
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
        self.coords = self.config.get("OpenAQ", "coords")

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

    def get_data(self, month: str = "01", year: str = "2025"):
        """
        Function that retrieves data for all location IDs in the locations.json file
        and returns a DataFrame.
        """
        file_path = os.path.join(os.getcwd(), "locations.json")
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as file:
                locs = json.load(file)
        else:
            locs = self.fetch_locations(save=False)

        keys = locs.keys()
        fs = s3fs.S3FileSystem(anon=True)
        dataframes = []

        for key in keys:
            file_list = fs.glob(
                f'openaq-data-archive/records/csv.gz/locationid={key}/year={year}/month={month}/*.csv.gz')

            if not file_list:
                print(f"Brak plików dla locationid={key}, year={year}, month={month}")

            for file in file_list:
                df = pd.read_csv(fs.open(file), encoding="utf-8", compression="gzip")
                dataframes.append(df)

        if dataframes:
            concatenated_data = pd.concat(dataframes, ignore_index=True)
            all_data = concatenated_data[concatenated_data ['parameter'].isin(['pm10','pm25', 'o3', 'so2'])]
            all_data = all_data.pivot_table(
                index=['location_id', 'sensors_id', 'location', 'datetime','lat','lon'],
                columns='parameter',
                values='value'
            ).reset_index().fillna(0)
            now = datetime.datetime.now().strftime("%Y%m%d")
            all_data['load_date'] = now
        else:
            all_data = pd.DataFrame()

        print(f"Number of rows: {len(all_data)}")
        return all_data

















