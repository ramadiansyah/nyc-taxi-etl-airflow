"""
config_loader.py

Utility module to load configuration settings from a YAML file.
"""
import yaml
import os

def load_config(config_path="config/config.yaml"):
    """
    Loads configuration from a YAML file.

    This function reads the YAML file at the specified path and returns the parsed contents
    as a Python dictionary. It raises a `FileNotFoundError` if the file does not exist.

    Args:
        config_path (str): Path to the YAML configuration file. Defaults to "config/config.yaml".

    Returns:
        dict: Configuration parameters loaded from the YAML file.

    Raises:
        FileNotFoundError: If the specified config file does not exist.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    return config
