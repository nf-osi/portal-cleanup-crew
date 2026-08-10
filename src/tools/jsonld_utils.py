import json
import requests

def _load_jsonld(source: str) -> dict:
    """Loads a JSON-LD file from a URL or a local path."""
    if source.startswith('http://') or source.startswith('https://'):
        try:
            response = requests.get(source)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error fetching data from URL '{source}': {e}")
        except json.JSONDecodeError:
            raise Exception(f"Error: The content at '{source}' is not a valid JSON file.")
    else:
        try:
            with open(source, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise Exception(f"Error: The local file '{source}' was not found.")
        except json.JSONDecodeError:
            raise Exception(f"Error: The local file '{source}' is not a valid JSON file.") 