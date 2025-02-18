import sys
sys.path.append("/Users/grzegorznaporowski/Desktop/Portfolio")

import requests
from air_pollution.src.utils.configuration import Configuration

class GetGeo:
    def __init__(self):
        self.config = Configuration()
        self.url = self.config.get("GeoJson", "url")


    def get_geo_data(self):
        response = requests.get(self.url)
        if response.status_code != 200:
            raise ConnectionError("There's some probem with connection to your link.")
        else:
            geojson = response.json()
            return geojson