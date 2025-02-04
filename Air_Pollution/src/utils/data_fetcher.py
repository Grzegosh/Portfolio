from Air_Pollution.src.utils.configuration import Configuration
from openaq import OpenAQ
import json
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









