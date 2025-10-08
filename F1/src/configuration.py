import os 
import configparser

class Configuration:
    def __init__(self, config_file = 'config.cfg') -> None:
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

    # open_f1 configuration

        self.drivers_url = self.config['open_f1']['drivers_url']
        self.pit_url = self.config['open_f1']['pit_url']
        self.sessions_url = self.config['open_f1']['sessions_url']
        self.starting_grid_url = self.config['open_f1']['starting_grid_url']
        self.overtakes_url = self.config['open_f1']['overtakes_url']
        self.session_results_url = self.config['open_f1']['session_results_url']
        self.laps_url = self.config['open_f1']['laps_url']

    # Season configuration

        self.season_start_date = self.config['season']['date_start']
    
    # Experiments configuration

        self.experiments_path = self.config['experiments']['data_path']


    def get_sections(self) -> None:
        """
        Print all sections and their key-value pairs from the configuration file.
        """
        for section in self.config.sections():
            print(f"[{section}]")
            for key, value in self.config[section].items():
                print(f"{key} = {value}")