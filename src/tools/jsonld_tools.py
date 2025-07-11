from crewai.tools import BaseTool
import json
import requests
from typing import List, Union

def _load_jsonld(source: str):
    """Loads a JSON-LD file from a URL or a local path."""
    if source.startswith('http://') or source.startswith('https://'):
        try:
            response = requests.get(source)
            response.raise_for_status()  # Raise an exception for bad status codes
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
            raise Exception(f"Error: The file '{source}' was not found.")
        except json.JSONDecodeError:
            raise Exception(f"Error: The file '{source}' is not a valid JSON file.")

class JsonLdGetAttributeDisplayNameTool(BaseTool):
    name: str = "JSON-LD Get Attribute Display Name Tool"
    description: str = "Gets the correct displayName for an attribute from the JSON-LD schema using its label or ID."

    def _run(self, source: str, attribute_name: str) -> Union[str, str]:
        """
        Gets the displayName for an attribute from the JSON-LD schema.
        """
        try:
            data_model = _load_jsonld(source)
        except Exception as e:
            return str(e)

        if '@graph' not in data_model:
            return "Error: JSON-LD file does not contain a '@graph' key."

        # Find the attribute by its label or ID and get its displayName
        for item in data_model['@graph']:
            # Check if this item matches our attribute
            item_label = item.get('rdfs:label', '')
            item_id = item.get('@id', '')
            
            if (item_label.lower() == attribute_name.lower() or 
                item_id.lower().endswith(attribute_name.lower()) or
                item_id.lower() == attribute_name.lower()):
                # Return the displayName if it exists, otherwise return the label
                return item.get('sms:displayName', item_label or attribute_name)

        return f"Attribute '{attribute_name}' not found in the data model."

class JsonLdGetValidValuesTool(BaseTool):
    name: str = "JSON-LD Get Valid Values Tool"
    description: str = "Parses a JSON-LD file from a URL or local path to get the list of valid 'displayNames' for a specific attribute, which is found by its 'label'."

    def _run(self, source: str, attribute_name: str) -> Union[List[str], str]:
        """
        Parses the JSON-LD file to find the valid values for a given attribute.
        """
        try:
            data_model = _load_jsonld(source)
        except Exception as e:
            return str(e)

        if '@graph' not in data_model:
            return "Error: JSON-LD file does not contain a '@graph' key."

        # Find the parent attribute's ID using its label
        parent_id = None
        for item in data_model['@graph']:
            if 'rdfs:label' in item and item['rdfs:label'].lower() == attribute_name.lower():
                parent_id = item['@id']
                break

        if not parent_id:
            return f"Error: Attribute '{attribute_name}' not found in the data model."

        # Find all subclasses of the parent attribute and get their displayNames
        valid_values = []
        for item in data_model['@graph']:
            if 'rdfs:subClassOf' in item:
                subclass_of_list = item['rdfs:subClassOf']
                if not isinstance(subclass_of_list, list):
                    subclass_of_list = [subclass_of_list]
                for sub in subclass_of_list:
                    if '@id' in sub and sub['@id'] == parent_id:
                        if 'sms:displayName' in item:
                            valid_values.append(item['sms:displayName'])

        if not valid_values:
            return f"No valid values (subclasses with displayNames) found for attribute '{attribute_name}'."

        return valid_values 

class JsonLdGetManifestsTool(BaseTool):
    name: str = "JSON-LD Get Manifests Tool"
    description: str = (
        "Extracts all manifest schemas defined in a JSON-LD data model. "
        "A manifest is a component with 'rdfs:subClassOf' set to 'bts:DataFile'."
    )

    def _run(self, source: str) -> list:
        """
        Extracts all manifest schemas from a JSON-LD file.
        """
        try:
            data = _load_jsonld(source)

            manifests = []
            # Return all nodes in the graph - let the caller filter for what they need
            for node in data.get('@graph', []):
                # Include nodes that have Template in their name/label or are subclasses
                node_id = node.get('@id', '')
                node_label = node.get('rdfs:label', '')
                
                if ('Template' in node_id or 'Template' in node_label or 
                    'rdfs:subClassOf' in node):
                    manifests.append(node)
            
            return manifests

        except Exception as e:
            return f"Error: {e}" 