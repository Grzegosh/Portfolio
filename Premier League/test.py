import yaml

config_path = "Premier League/config.yaml"


with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

print(config['seasons'])