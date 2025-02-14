import os
import configparser

class Configuration:
    """
    Class responsible for reading a config file.
    """

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config_path = os.path.dirname(__file__)
        self.full_path = os.path.abspath(os.path.join(self.config_path,"..","..","config","config.cfg"))
        self.config.read(self.full_path)

    def get(self, section, key, default = None):
        """

        :param section: Particular section in cfg file
        :param key: Key in section
        :param default: Default values
        :return: Particular value from config file or default value
        """
        try:
            return self.config.get(section, key)
        except (configparser.NoSectionError, configparser.NoOptionError):
            return default

    def get_all_sections(self):
        """
        Function that provides all the names of sections
        :return: Names of the sections
        """
        return self.config.sections()

    def get_all_keys(self, section=None):
        """

        :param section: Section of a config file
        :return: All of the keys
        """
        if section in self.get_all_sections():
            return list(self.config[section].keys())
        else:
            raise ValueError(f"Section: {section} not found in the config file!")




