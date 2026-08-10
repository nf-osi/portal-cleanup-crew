from crewai.tools import BaseTool
import json
import requests
from typing import List, Union, Dict, Type
from src.tools.jsonld_utils import _load_jsonld
from pydantic import BaseModel, Field

def get_common_rnaseq_display_names():
    """
    Fallback function that provides common displayNames for RNA-seq annotations
    when schema parsing fails. Uses correct camelCase field names.
    """
    return {
        'Component': 'component',
        'Filename': 'filename', 
        'FileFormat': 'fileFormat',
        'ResourceType': 'resourceType',
        'DataType': 'dataType',
        'DataSubtype': 'dataSubtype',
        'Assay': 'assay',
        'IndividualID': 'individualID',
        'Species': 'species',
        'Sex': 'sex',
        'Age': 'age',
        'AgeUnit': 'ageUnit',
        'Diagnosis': 'diagnosis',
        'Nf1Genotype': 'nf1Genotype',
        'Nf2Genotype': 'nf2Genotype', 
        'TumorType': 'tumorType',
        'ModelSystemName': 'modelSystemName',
        'Organ': 'organ',
        'Comments': 'comments',
        'ParentSpecimenID': 'parentSpecimenID',
        'SpecimenID': 'specimenID',
        'AliquotID': 'aliquotID',
        'Platform': 'platform',
        'NucleicAcidSource': 'nucleicAcidSource',
        'SpecimenPreparationMethod': 'specimenPreparationMethod',
        'SpecimenType': 'specimenType',
        'RunType': 'runType',
        'LibraryStrand': 'libraryStrand',
        'LibraryPrep': 'libraryPrep',
        'LibraryPreparationMethod': 'libraryPreparationMethod',
        'ReadPair': 'readPair',
        'ReadLength': 'readLength',
        'ReadDepth': 'readDepth',
        'TargetDepth': 'targetDepth',
        'BatchID': 'batchID',
        'GenePerturbed': 'genePerturbed',
        'GenePerturbationType': 'genePerturbationType',
        'GenePerturbationTechnology': 'genePerturbationTechnology',
        'ExperimentalCondition': 'experimentalCondition',
        'IsCellLine': 'isCellLine',
        'IsXenograft': 'isXenograft'
    }

class CommonDisplayNamesInput(BaseModel):
    """Input for getting common RNA-seq display names."""
    pass

class CommonRNASeqDisplayNamesTool(BaseTool):
    name: str = "Get Common RNA-seq Display Names Tool"
    description: str = (
        "Returns common displayNames for RNA-seq annotations as a fallback when "
        "schema parsing fails. Provides correct camelCase field names like 'readPair' "
        "instead of 'ReadPair'. Use this when JSON-LD tools fail."
    )
    args_schema: Type[BaseModel] = CommonDisplayNamesInput

    def _run(self) -> Dict[str, str]:
        """Returns common RNA-seq displayNames mapping."""
        return get_common_rnaseq_display_names()

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

    def _run(self, attribute_name: str, data_model_url: str) -> list:
        try:
            jsonld_data = _load_jsonld(data_model_url)
            return self._get_valid_values(jsonld_data, attribute_name)
        except Exception as e:
            return [f"An error occurred: {str(e)}"]

    def _get_valid_values(self, jsonld_data: dict, attribute_name: str) -> list:
        if '@graph' not in jsonld_data:
            return ["Error: JSON-LD file does not contain a '@graph' key."]

        # Find the parent attribute's ID using its label
        parent_id = None
        for item in jsonld_data['@graph']:
            if 'rdfs:label' in item and item['rdfs:label'].lower() == attribute_name.lower():
                parent_id = item['@id']
                break

        if not parent_id:
            return [f"Error: Attribute '{attribute_name}' not found in the data model."]

        # Find all subclasses of the parent attribute and get their displayNames
        valid_values = []
        for item in jsonld_data['@graph']:
            if 'rdfs:subClassOf' in item:
                subclass_of_list = item['rdfs:subClassOf']
                if not isinstance(subclass_of_list, list):
                    subclass_of_list = [subclass_of_list]
                for sub in subclass_of_list:
                    if '@id' in sub and sub['@id'] == parent_id:
                        if 'sms:displayName' in item:
                            valid_values.append(item['sms:displayName'])

        if not valid_values:
            return [f"No valid values (subclasses with displayNames) found for attribute '{attribute_name}'."]

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

class JsonLdAttributeDisplayNamesInput(BaseModel):
    """Input for getting attribute display names from JSON-LD schema."""
    template_name: str = Field(description="Name of the template/component (e.g., 'RNASeqTemplate', 'ScRNASeqTemplate')")
    data_model_url: str = Field(description="URL or path to the JSON-LD data model")

class JsonLdAttributeDisplayNamesTool(BaseTool):
    name: str = "JSON-LD Get Attribute Display Names Tool"
    description: str = (
        "Gets the correct displayName for all attributes in a specific template from a JSON-LD data model. "
        "This ensures you use the correct field names (e.g., 'readPair' not 'ReadPair') when creating annotations. "
        "Returns a mapping of attribute labels to their displayNames."
    )
    args_schema: Type[BaseModel] = JsonLdAttributeDisplayNamesInput

    def _run(self, template_name: str, data_model_url: str) -> Dict[str, str]:
        """
        Gets the displayName for all attributes in a template.
        
        Args:
            template_name: Name of the template (e.g., 'RNASeqTemplate')
            data_model_url: URL or path to the JSON-LD data model
            
        Returns:
            Dictionary mapping attribute labels to their displayNames
        """
        try:
            # Load the JSON-LD data
            if data_model_url.startswith(('http://', 'https://')):
                response = requests.get(data_model_url)
                response.raise_for_status()
                data = response.json()
            else:
                with open(data_model_url, 'r') as f:
                    data = json.load(f)
            
            # Find the template
            template_data = None
            graph = data.get('@graph', [])
            
            for item in graph:
                if (item.get('@id', '').endswith(template_name) or 
                    item.get('rdfs:label') == template_name or
                    item.get('sms:displayName') == template_name):
                    template_data = item
                    break
            
            if not template_data:
                return {"error": f"Template '{template_name}' not found in the data model"}
            
            # Get all properties for this template
            properties = template_data.get('sms:properties', [])
            
            display_names = {}
            
            for prop in properties:
                # Handle both direct properties and references
                if isinstance(prop, dict):
                    prop_id = prop.get('@id', '')
                    label = prop_id.split(':')[-1] if ':' in prop_id else prop_id
                    display_name = prop.get('sms:displayName', label)
                    display_names[label] = display_name
                elif isinstance(prop, str):
                    # This is a reference, find the actual property definition
                    prop_id = prop
                    for item in graph:
                        if item.get('@id') == prop_id:
                            label = prop_id.split(':')[-1] if ':' in prop_id else prop_id
                            display_name = item.get('sms:displayName', label)
                            display_names[label] = display_name
                            break
            
            return display_names
            
        except Exception as e:
            return {"error": f"Failed to parse JSON-LD data model: {str(e)}"} 