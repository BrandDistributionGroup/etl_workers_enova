import os
import configparser

# Load configuration
config_path = os.getenv("CONFIG_PATH")
config = configparser.ConfigParser()
config.read(config_path)