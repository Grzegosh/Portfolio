from Air_Pollution.src.utils.configuration import Configuration
from openaq import OpenAQ
import json
import pandas as pd
import ast

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
        Fully connected OpenAQ client
        """
        client = OpenAQ(api_key=self.api_key)

        if isinstance(client, OpenAQ):
            return client
        else:
            raise TypeError("Client is not a OpenAQ class")


    def fetch_location_id(self, save=False):
        """
        Function that collect location id based on the attached config file
        :param save: if True the locations and names will be saved in your working directory as json file.
        :return: DataFrame with uncleared data.
        """
        location_dictionary = {}
        client = self.connect_to_client()
        results = client.locations.list(
            page=1,
            limit=1000,
            bbox=self.coords
        ).results

        if len(results)==0:
            raise ValueError("There's no results that meet your critetia.")

        for location in results:
            location_dictionary[location.id] = location.name

        if save:
            with open("locations.json", "w") as loc:
                json.dump(
                    location_dictionary,
                    loc,
                    ensure_ascii=False,
                    sort_keys=True
                )
        else:
            return location_dictionary










