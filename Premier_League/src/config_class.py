import yaml

class DataLoader:
    """
    Klasa odpowiedzialna za ładowanie danych z pliku konfiguracyjnego YAML.
    Atrybuty:
    - config_path: Ścieżka do pliku konfiguracyjnego YAML.
    """
    def __init__(self, config_path):
        self.config_path = config_path

    def load_config(self):
        """
        Funkcja ładująca plik konfiguracyjny YAML i zwracająca jego zawartość.
        """
        with open(self.config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
        
    def get_seasons(self):
        """
        Funkcja zwracająca listę sezonów z pliku konfiguracyjnego.
        """
        config = self.load_config()
        return config['seasons']
    
    