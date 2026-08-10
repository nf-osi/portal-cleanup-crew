import requests
import json
import pandas as pd
from crewai.tools import BaseTool
from typing import Optional, Type, Dict, Any
from pydantic import BaseModel, Field
import tempfile
import os
from urllib.parse import urljoin
from .jsonld_tools import JsonLdGetValidValuesTool, JsonLdGetManifestsTool, _load_jsonld


class PrideDatasetMetadataInput(BaseModel):
    """Input for fetching PRIDE dataset metadata."""
    pride_id: str = Field(description="The PRIDE dataset accession ID (e.g., PXD001234)")


class PrideDatasetFilesInput(BaseModel):
    """Input for fetching PRIDE dataset file listings."""
    pride_id: str = Field(description="The PRIDE dataset accession ID (e.g., PXD001234)")


class PrideDatasetMetadataTool(BaseTool):
    name: str = "PRIDE Dataset Metadata Fetcher"
    description: str = (
        "Fetches metadata for a PRIDE dataset using the PRIDE Archive REST API. "
        "Returns comprehensive dataset information including title, description, "
        "species, instruments, publication details, and submission information."
    )
    args_schema: Type[BaseModel] = PrideDatasetMetadataInput

    def _run(self, pride_id: str) -> dict:
        """
        Fetches metadata for a PRIDE dataset.
        
        Args:
            pride_id: PRIDE accession ID (e.g., "PXD001234")
            
        Returns:
            Dictionary containing dataset metadata or error information
        """
        base_url = "https://www.ebi.ac.uk/pride/ws/archive/v2/"
        endpoint = f"projects/{pride_id}"
        
        try:
            response = requests.get(urljoin(base_url, endpoint))
            response.raise_for_status()
            
            data = response.json()
            
            # Extract and structure key metadata
            metadata = {
                "accession": data.get("accession"),
                "title": data.get("title"),
                "description": data.get("projectDescription"),
                "publication_date": data.get("publicationDate"),
                "submission_date": data.get("submissionDate"),
                "species": [species.get("name") for species in data.get("species", [])],
                "instruments": [instr.get("name") for instr in data.get("instruments", [])],
                "experiment_types": [exp.get("name") for exp in data.get("experimentTypes", [])],
                "keywords": data.get("keywords", []),
                "publications": [
                    {
                        "title": pub.get("title"),
                        "authors": pub.get("authors"),
                        "pubmed_id": pub.get("pubmedId"),
                        "doi": pub.get("doi")
                    } for pub in data.get("publications", [])
                ],
                "contacts": [
                    {
                        "name": contact.get("name"),
                        "email": contact.get("email"),
                        "affiliation": contact.get("affiliation")
                    } for contact in data.get("contacts", [])
                ],
                "sample_processing_protocol": data.get("sampleProcessingProtocol"),
                "data_processing_protocol": data.get("dataProcessingProtocol"),
                "raw_data": data
            }
            
            return metadata
            
        except requests.exceptions.RequestException as e:
            return {"error": f"Failed to fetch PRIDE dataset metadata: {str(e)}"}
        except json.JSONDecodeError as e:
            return {"error": f"Failed to parse PRIDE API response: {str(e)}"}
        except Exception as e:
            return {"error": f"Unexpected error while fetching PRIDE metadata: {str(e)}"}


class PrideDatasetFilesTool(BaseTool):
    name: str = "PRIDE Dataset Files Fetcher"
    description: str = (
        "Fetches the list of files available for a PRIDE dataset. "
        "Returns information about each file including name, size, type, "
        "and download URLs."
    )
    args_schema: Type[BaseModel] = PrideDatasetFilesInput

    def _run(self, pride_id: str) -> dict:
        """
        Fetches file listings for a PRIDE dataset.
        
        Args:
            pride_id: PRIDE accession ID (e.g., "PXD001234")
            
        Returns:
            Dictionary containing file information or error details
        """
        base_url = "https://www.ebi.ac.uk/pride/ws/archive/v2/"
        endpoint = f"projects/{pride_id}/files"
        
        try:            
            response = requests.get(urljoin(base_url, endpoint))
            response.raise_for_status()
            
            data = response.json()
            
            files_info = []
            # The response is a direct array of files
            for file_data in data if isinstance(data, list) else []:
                file_info = {
                    "file_name": file_data.get("fileName"),
                    "file_size": file_data.get("fileSizeBytes"),
                    "file_category": file_data.get("fileCategory", {}).get("value") if file_data.get("fileCategory") else None,
                    "file_type": file_data.get("fileCategory", {}).get("value") if file_data.get("fileCategory") else None,
                    "compression": file_data.get("compress", False),
                    "download_url": None,  # Will extract from publicFileLocations
                    "ftp_download_url": None,  # Will extract from publicFileLocations
                    "checksum": file_data.get("checksum"),
                    "public_url": None  # Will extract from publicFileLocations
                }
                
                # Extract download URLs from publicFileLocations
                public_locations = file_data.get("publicFileLocations", [])
                for location in public_locations:
                    if location.get("name") == "FTP Protocol":
                        file_info["ftp_download_url"] = location.get("value")
                        if not file_info["download_url"]:  # Use FTP as primary download URL
                            file_info["download_url"] = location.get("value")
                    elif location.get("name") == "Aspera Protocol":
                        # Could add aspera support later if needed
                        pass
                files_info.append(file_info)
            
            # Create summary statistics
            summary = {
                "total_files": len(files_info),
                "total_size_bytes": sum(f.get("file_size", 0) for f in files_info if f.get("file_size")),
                "file_types": list(set(f.get("file_type") for f in files_info if f.get("file_type"))),
                "file_categories": list(set(f.get("file_category") for f in files_info if f.get("file_category")))
            }
            
            return {
                "pride_id": pride_id,
                "summary": summary,
                "files": files_info
            }
            
        except requests.exceptions.RequestException as e:
            return {"error": f"Failed to fetch PRIDE dataset files: {str(e)}"}
        except json.JSONDecodeError as e:
            return {"error": f"Failed to parse PRIDE API response: {str(e)}"}
        except Exception as e:
            return {"error": f"Unexpected error while fetching PRIDE files: {str(e)}"}


class PrideAnnotationMapperInput(BaseModel):
    """Input for mapping PRIDE metadata to schema annotations."""
    pride_metadata: dict = Field(description="PRIDE dataset metadata dictionary")
    file_info: Optional[dict] = Field(default=None, description="PRIDE file information dictionary (optional)") 
    data_model_url: str = Field(
        default="https://raw.githubusercontent.com/nf-osi/nf-metadata-dictionary/main/NF.jsonld",
        description="URL or path to the JSON-LD data model (defaults to NF schema if not provided)"
    )


class PrideAnnotationMapperTool(BaseTool):
    name: str = "PRIDE Annotation Mapper"
    description: str = (
        "Intelligently maps PRIDE dataset and file metadata to appropriate schema annotations "
        "based on the MassSpecAssayTemplate manifest. Uses JSON-LD schema analysis to determine "
        "valid attributes and controlled vocabularies for intelligent mapping."
    )
    args_schema: Type[BaseModel] = PrideAnnotationMapperInput

    def _run(self, pride_metadata: dict, file_info: Optional[dict] = None, data_model_url: str = "https://raw.githubusercontent.com/nf-osi/nf-metadata-dictionary/main/NF.jsonld") -> dict:
        """
        Maps PRIDE metadata to schema annotations using intelligent schema-aware mapping.
        
        Args:
            pride_metadata: PRIDE dataset metadata
            file_info: PRIDE file information (optional, can be None)
            data_model_url: URL or path to JSON-LD data model
            
        Returns:
            Dictionary with mapping results, schema analysis, and mapping guidance
        """
        try:
            # Step 1: Get the MassSpecAssayTemplate structure from the schema
            manifests_tool = JsonLdGetManifestsTool()
            manifests_result = manifests_tool._run(source=data_model_url)
            
            # Find MassSpecAssayTemplate - search more thoroughly
            mass_spec_template = None
            if isinstance(manifests_result, list):
                for manifest in manifests_result:
                    # Check if this manifest IS the MassSpecAssayTemplate
                    if isinstance(manifest, dict):
                        # Check various ways it might be identified
                        manifest_id = manifest.get('@id', '')
                        manifest_label = manifest.get('rdfs:label', '')
                        manifest_name = manifest.get('name', '')
                        
                        if ('MassSpecAssayTemplate' in manifest_id or 
                            'MassSpecAssayTemplate' in manifest_label or
                            'MassSpecAssayTemplate' in manifest_name):
                            mass_spec_template = manifest
                            break
                    
                    # Also check string representation as fallback
                    if 'MassSpecAssayTemplate' in str(manifest):
                        mass_spec_template = manifest
                        break
            
            # If manifests_result is a dict, it might contain the template directly
            elif isinstance(manifests_result, dict):
                if ('MassSpecAssayTemplate' in str(manifests_result) or
                    manifests_result.get('@id', '').endswith('MassSpecAssayTemplate') or
                    manifests_result.get('rdfs:label') == 'MassSpecAssayTemplate'):
                    mass_spec_template = manifests_result
            
            if not mass_spec_template:
                return {
                    "error": "Could not find MassSpecAssayTemplate in the data model",
                    "debug_info": f"Manifests result type: {type(manifests_result)}, length: {len(manifests_result) if isinstance(manifests_result, list) else 'N/A'}"
                }
            
            # Step 2: Extract template attributes and their constraints
            template_attributes = self._extract_template_attributes(mass_spec_template)
            
            # Step 3: Get valid values for attributes that might have controlled vocabularies
            attribute_constraints = {}
            valid_values_tool = JsonLdGetValidValuesTool()
            
            for attr in template_attributes[:15]:  # Limit to avoid too many API calls
                try:
                    valid_values = valid_values_tool._run(source=data_model_url, attribute_name=attr)
                    if valid_values and isinstance(valid_values, list) and len(valid_values) > 0:
                        # Check if the valid values contain URIs (controlled vocabulary)
                        if any(isinstance(v, dict) and 'uri' in v for v in valid_values):
                            attribute_constraints[attr] = valid_values
                        elif len(valid_values) < 50:  # Reasonable number of enum values
                            attribute_constraints[attr] = valid_values
                except Exception:
                    continue  # Skip if no valid values found
            
            # Step 4: Perform intelligent mapping
            mapping_analysis = self._analyze_pride_data_for_mapping(
                pride_metadata, file_info, template_attributes, attribute_constraints
            )
            
            # Step 5: Apply the intelligent mapping
            annotations = self._apply_intelligent_mapping(
                pride_metadata, file_info, mapping_analysis
            )
            
            return {
                "success": True,
                "annotations": annotations,
                "total_annotations": len(annotations),
                "template_attributes": template_attributes,
                "attribute_constraints": {k: len(v) for k, v in attribute_constraints.items()},
                "mapping_analysis": mapping_analysis,
                "mapping_method": "Schema-aware intelligent mapping"
            }
            
        except Exception as e:
            return {"error": f"Failed to map PRIDE metadata to annotations: {str(e)}"}
    
    def _extract_template_attributes(self, template: dict) -> list:
        """
        Extract attribute names from the MassSpecAssayTemplate.
        """
        attributes = []
        
        try:
            # Handle different possible template structures
            if isinstance(template, dict):
                # Look for properties in various possible keys
                possible_props_keys = ['properties', 'sms:properties', 'attributes', '@graph']
                
                for key in possible_props_keys:
                    if key in template:
                        props = template[key]
                        if isinstance(props, list):
                            for prop in props:
                                if isinstance(prop, dict):
                                    # Extract attribute name from various possible formats
                                    attr_name = (prop.get('rdfs:label') or 
                                                prop.get('name') or 
                                                prop.get('@id', '').split('/')[-1] or
                                                prop.get('@id', '').split(':')[-1])
                                    if attr_name and attr_name not in ['', 'MassSpecAssayTemplate']:
                                        attributes.append(attr_name)
                        elif isinstance(props, dict):
                            attributes.extend([k for k in props.keys() if k not in ['@type', '@id']])
                
                # Also look for direct keys that might be attributes
                direct_attrs = ['study', 'species', 'assay', 'platform', 'fileFormat', 
                              'dataType', 'resourceId', 'doi', 'fileName', 'fileSize',
                              'instrument', 'keywords', 'description', 'title']
                attributes.extend(direct_attrs)
            
            # Remove duplicates and clean up
            attributes = list(set([attr for attr in attributes if attr and isinstance(attr, str)]))
            
        except Exception as e:
            print(f"Warning: Could not extract template attributes: {e}")
            # Fallback to common proteomics attributes
            attributes = ['study', 'species', 'assay', 'platform', 'fileFormat', 
                         'dataType', 'resourceId', 'doi', 'fileName', 'fileSize']
        
        return attributes
    
    def _analyze_pride_data_for_mapping(self, pride_metadata: dict, file_info: Optional[dict], 
                                       template_attributes: list, attribute_constraints: dict) -> dict:
        """
        Analyze PRIDE data and create intelligent mapping suggestions.
        """
        mapping_analysis = {
            "direct_mappings": {},
            "suggested_mappings": {},
            "controlled_vocab_mappings": {},
            "data_transformations": {},
            "mapping_confidence": {}
        }
        
        # Direct mappings (high confidence)
        direct_map = {
            'title': 'study',
            'description': 'studyDescription', 
            'accession': 'resourceId',
            'file_name': 'fileName',
            'file_size': 'fileSize'
        }
        
        for pride_key, schema_attr in direct_map.items():
            if schema_attr in template_attributes:
                value_found = False
                if pride_key in pride_metadata:
                    value_found = True
                elif file_info and pride_key in file_info:
                    value_found = True
                
                if value_found:
                    mapping_analysis["direct_mappings"][pride_key] = schema_attr
                    mapping_analysis["mapping_confidence"][pride_key] = "high"
        
        # Species mapping with intelligent selection
        if 'species' in template_attributes and pride_metadata.get('species'):
            species_data = pride_metadata['species']
            if attribute_constraints.get('species'):
                # Try to match against controlled vocabulary
                mapped_species = self._map_to_controlled_vocab(
                    species_data, attribute_constraints['species'], 'species'
                )
                if mapped_species:
                    mapping_analysis["controlled_vocab_mappings"]['species'] = mapped_species
                    mapping_analysis["mapping_confidence"]['species'] = "medium"
            else:
                mapping_analysis["suggested_mappings"]['species'] = species_data
                mapping_analysis["mapping_confidence"]['species'] = "high"
        
        # Assay/experiment type mapping
        if 'assay' in template_attributes and pride_metadata.get('experiment_types'):
            exp_types = pride_metadata['experiment_types']
            if attribute_constraints.get('assay'):
                mapped_assay = self._map_to_controlled_vocab(
                    exp_types, attribute_constraints['assay'], 'assay'
                )
                if mapped_assay:
                    mapping_analysis["controlled_vocab_mappings"]['assay'] = mapped_assay
                    mapping_analysis["mapping_confidence"]['assay'] = "medium"
            else:
                mapping_analysis["suggested_mappings"]['assay'] = exp_types
                mapping_analysis["mapping_confidence"]['assay'] = "high"
        
        # File format mapping based on PRIDE categories
        if file_info and file_info.get('file_category'):
            category = file_info['file_category']
            format_mapping = {
                'RAW': 'raw data',
                'SEARCH': 'processed data', 
                'OTHER': 'metadata'
            }
            
            if 'fileFormat' in template_attributes:
                suggested_format = format_mapping.get(category, category.lower())
                if attribute_constraints.get('fileFormat'):
                    mapped_format = self._map_to_controlled_vocab(
                        [suggested_format], attribute_constraints['fileFormat'], 'fileFormat'
                    )
                    if mapped_format:
                        mapping_analysis["controlled_vocab_mappings"]['fileFormat'] = mapped_format[0]
                        mapping_analysis["mapping_confidence"]['fileFormat'] = "medium"
                else:
                    mapping_analysis["suggested_mappings"]['fileFormat'] = suggested_format
                    mapping_analysis["mapping_confidence"]['fileFormat'] = "high"
        
        # Publication information
        publications = pride_metadata.get('publications', [])
        if publications:
            pub = publications[0]
            if 'doi' in template_attributes and pub.get('doi'):
                mapping_analysis["direct_mappings"]['doi'] = 'doi'
                mapping_analysis["mapping_confidence"]['doi'] = "high"
            
            if 'pubmedId' in template_attributes and pub.get('pubmed_id'):
                mapping_analysis["data_transformations"]['pubmed_id'] = {
                    'target_attr': 'pubmedId',
                    'transformation': 'convert_to_string',
                    'value': str(pub['pubmed_id'])
                }
                mapping_analysis["mapping_confidence"]['pubmed_id'] = "high"
        
        # Instrument mapping
        if 'instrument' in template_attributes and pride_metadata.get('instruments'):
            instruments = pride_metadata['instruments']
            if attribute_constraints.get('instrument'):
                mapped_instruments = self._map_to_controlled_vocab(
                    instruments, attribute_constraints['instrument'], 'instrument'
                )
                if mapped_instruments:
                    mapping_analysis["controlled_vocab_mappings"]['instrument'] = mapped_instruments
                    mapping_analysis["mapping_confidence"]['instrument'] = "medium"
            else:
                mapping_analysis["suggested_mappings"]['instrument'] = instruments
                mapping_analysis["mapping_confidence"]['instrument'] = "high"
        
        return mapping_analysis
    
    def _map_to_controlled_vocab(self, values: list, controlled_vocab: list, attr_name: str) -> list:
        """
        Intelligently map values to a controlled vocabulary.
        """
        if not values or not controlled_vocab:
            return []
        
        mapped_values = []
        
        # Extract vocabulary terms
        vocab_terms = []
        for item in controlled_vocab:
            if isinstance(item, dict):
                term = item.get('term') or item.get('label') or item.get('name')
                if term:
                    vocab_terms.append((term, item))
            elif isinstance(item, str):
                vocab_terms.append((item, item))
        
        # Map each value
        for value in values:
            if isinstance(value, dict):
                value_str = str(value.get('name', value))
            else:
                value_str = str(value)
            
            # Try exact match first
            exact_match = None
            for term, term_data in vocab_terms:
                if value_str.lower() == term.lower():
                    exact_match = term_data
                    break
            
            if exact_match:
                mapped_values.append(exact_match)
                continue
            
            # Try partial matching
            best_match = None
            best_score = 0
            for term, term_data in vocab_terms:
                # Simple string similarity scoring
                if value_str.lower() in term.lower() or term.lower() in value_str.lower():
                    score = len(set(value_str.lower().split()) & set(term.lower().split()))
                    if score > best_score:
                        best_score = score
                        best_match = term_data
            
            if best_match and best_score > 0:
                mapped_values.append(best_match)
        
        return mapped_values
    
    def _apply_intelligent_mapping(self, pride_metadata: dict, file_info: Optional[dict], 
                                 mapping_analysis: dict) -> dict:
        """
        Apply the intelligent mapping analysis to create final annotations.
        """
        annotations = {}
        
        # Apply direct mappings
        for pride_key, schema_attr in mapping_analysis["direct_mappings"].items():
            if pride_key in pride_metadata:
                annotations[schema_attr] = pride_metadata[pride_key]
            elif file_info and pride_key in file_info:
                annotations[schema_attr] = file_info[pride_key]
        
        # Apply suggested mappings
        for attr, value in mapping_analysis["suggested_mappings"].items():
            annotations[attr] = value
        
        # Apply controlled vocabulary mappings
        for attr, mapped_values in mapping_analysis["controlled_vocab_mappings"].items():
            if isinstance(mapped_values, list):
                if len(mapped_values) == 1:
                    # Single value
                    val = mapped_values[0]
                    annotations[attr] = val.get('term') if isinstance(val, dict) else val
                else:
                    # Multiple values
                    annotations[attr] = [
                        (val.get('term') if isinstance(val, dict) else val) 
                        for val in mapped_values
                    ]
            else:
                # Single value
                val = mapped_values
                annotations[attr] = val.get('term') if isinstance(val, dict) else val
        
        # Apply data transformations
        for pride_key, transform_info in mapping_analysis["data_transformations"].items():
            target_attr = transform_info['target_attr']
            annotations[target_attr] = transform_info['value']
        
        # ✅ FIXED: Use schema-aware mapping for dataType instead of hardcoding
        if 'dataType' not in annotations:
            # Try to map proteomics data to valid schema values
            try:
                from .jsonld_tools import JsonLdGetValidValuesTool
                valid_values_tool = JsonLdGetValidValuesTool()
                data_model_url = "https://raw.githubusercontent.com/nf-osi/nf-metadata-dictionary/main/NF.jsonld"
                
                valid_data_types = valid_values_tool._run(source=data_model_url, attribute_name='dataType')
                
                # Look for mass spectrometry related values in the schema
                proteomics_values = []
                for val in valid_data_types or []:
                    val_str = str(val).lower()
                    if any(term in val_str for term in ['proteomic', 'mass spectrometry', 'ms']):
                        proteomics_values.append(val)
                
                if proteomics_values:
                    # Use the first matching value from the schema
                    annotations['dataType'] = proteomics_values[0]
                elif valid_data_types and 'proteomics' in [str(v).lower() for v in valid_data_types]:
                    # Fallback to 'proteomics' if it exists
                    annotations['dataType'] = 'proteomics'
                    
            except Exception:
                # Only add dataType if we can validate it against schema
                pass
        
        # ✅ FIXED: Use schema-aware mapping for platform instead of hardcoding
        if 'platform' not in annotations:
            try:
                from .jsonld_tools import JsonLdGetValidValuesTool
                valid_values_tool = JsonLdGetValidValuesTool()
                data_model_url = "https://raw.githubusercontent.com/nf-osi/nf-metadata-dictionary/main/NF.jsonld"
                
                valid_platforms = valid_values_tool._run(source=data_model_url, attribute_name='platform')
                
                # Look for PRIDE or mass spec platforms in the schema
                if valid_platforms:
                    pride_platforms = []
                    for val in valid_platforms:
                        val_str = str(val).lower()
                        if any(term in val_str for term in ['pride', 'proteomics', 'mass spec']):
                            pride_platforms.append(val)
                    
                    if pride_platforms:
                        annotations['platform'] = pride_platforms[0]
                        
            except Exception:
                # Only add platform if we can validate it against schema
                pass
        
        # ✅ FIXED: Only add attributes that exist in the schema
        schema_attrs = mapping_analysis.get("template_attributes", [])
        
        # Add resourceType only if it's in the schema
        if 'resourceType' in schema_attrs and 'resourceType' not in annotations:
            try:
                from .jsonld_tools import JsonLdGetValidValuesTool
                valid_values_tool = JsonLdGetValidValuesTool()
                data_model_url = "https://raw.githubusercontent.com/nf-osi/nf-metadata-dictionary/main/NF.jsonld"
                
                valid_resource_types = valid_values_tool._run(source=data_model_url, attribute_name='resourceType')
                if valid_resource_types and 'proteomics' in [str(v).lower() for v in valid_resource_types]:
                    annotations['resourceType'] = 'proteomics'
            except Exception:
                pass
        
        # ✅ FIXED: Only add dates if they're actually in the schema
        if 'publicationDate' in schema_attrs and pride_metadata.get('publication_date'):
            annotations['publicationDate'] = pride_metadata['publication_date']
        # Note: removed submissionDate since user says it's not in their schema
        
        # Add keywords only if the attribute exists in schema
        if 'keywords' in schema_attrs and pride_metadata.get('keywords'):
            annotations['keywords'] = pride_metadata['keywords']
        
        return annotations 