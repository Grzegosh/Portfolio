from Air_Pollution.src.utils.configuration import Configuration
from openaq import OpenAQ
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
        Fully connected OpetnAQ client
        """
        client = OpenAQ(api_key=self.api_key)

        if isinstance(client, OpenAQ):
            return client
        else:
            raise TypeError("Client is not a OpenAQ class")





test = DataFetcher()
test2 = test.connect_to_client()




