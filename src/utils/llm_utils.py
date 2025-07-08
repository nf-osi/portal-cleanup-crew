import yaml
from crewai import LLM
import os

def get_llm():
    """
    Initializes and returns the LLM based on the configuration in config.yaml and creds.yaml.
    """
    try:
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f).get('llm', {})
    except (FileNotFoundError, yaml.YAMLError):
        print("Warning: Could not read or parse config.yaml. Relying on environment variables.")
        config = {}
    
    try:
        with open('creds.yaml', 'r') as f:
            creds_config = yaml.safe_load(f).get('llm', {})
    except (FileNotFoundError, yaml.YAMLError):
        print("Warning: Could not read or parse creds.yaml. Relying on environment variables.")
        creds_config = {}

    # Merge credentials, giving priority to creds.yaml
    credentials = {**config.get('credentials', {}), **creds_config.get('credentials', {})}

    for key, value in credentials.items():
        os.environ[key] = value

    model_name = config.get('model')
    if not model_name:
        raise ValueError("LLM model not specified in config.yaml")

    return LLM(model=model_name) 