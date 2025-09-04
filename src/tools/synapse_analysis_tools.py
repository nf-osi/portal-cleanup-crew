import requests
import json
import pandas as pd
from crewai.tools import BaseTool
from typing import Optional, Type, Dict, Any, List
from pydantic import BaseModel, Field
import tempfile
import os
import re
import csv
from urllib.parse import urljoin
import synapseclient

# Handle relative imports for both development and production
try:
    from .jsonld_tools import JsonLdGetManifestsTool, _load_jsonld
except ImportError:
    try:
        from jsonld_tools import JsonLdGetManifestsTool, _load_jsonld
    except ImportError:
        # For standalone testing
        JsonLdGetManifestsTool = None
        _load_jsonld = None


class SynapseFolderAnalysisInput(BaseModel):
    """Input for analyzing a Synapse folder's contents."""
    synapse_id: str = Field(description="The Synapse ID of the folder or file to analyze")


class SynapseFolderAnalysisTool(BaseTool):
    name: str = "Synapse Folder Analysis Tool"
    description: str = (
        "Recursively analyzes a Synapse folder or dataset to identify all files, "
        "classify them as data files vs metadata/auxiliary files, and extract "
        "existing annotations and external identifiers. Returns structured information "
        "about the folder contents for further processing."
    )
    args_schema: Type[BaseModel] = SynapseFolderAnalysisInput
    syn: Optional[synapseclient.Synapse] = None

    def __init__(self, syn: synapseclient.Synapse, **kwargs):
        super().__init__(**kwargs)
        self.syn = syn

    def _run(self, synapse_id: str) -> dict:
        """
        Analyzes a Synapse folder and its contents.
        
        Args:
            synapse_id: Synapse ID of the folder/dataset to analyze
            
        Returns:
            Dictionary containing structured analysis of folder contents
        """
        if not self.syn:
            return {"error": "Synapse client not initialized. Login failed."}
        
        try:
            # Get the main entity
            entity = self.syn.get(synapse_id, downloadFile=False)
            
            # Get all children recursively
            all_files = []
            self._get_children_recursive(synapse_id, all_files)
            
            # Classify files
            data_files = []
            metadata_files = []
            other_files = []
            
            for file_info in all_files:
                classification = self._classify_file(file_info)
                if classification == "data":
                    data_files.append(file_info)
                elif classification == "metadata":
                    metadata_files.append(file_info)
                else:
                    other_files.append(file_info)
            
            # Extract external identifiers
            external_identifiers = self._extract_external_identifiers(all_files, entity)
            
            return {
                "synapse_id": synapse_id,
                "entity_name": entity.name,
                "entity_type": entity.__class__.__name__,
                "total_files": len(all_files),
                "data_files": data_files,
                "metadata_files": metadata_files,
                "other_files": other_files,
                "external_identifiers": external_identifiers,
                "summary": {
                    "data_file_count": len(data_files),
                    "metadata_file_count": len(metadata_files),
                    "other_file_count": len(other_files),
                    "identified_external_ids": list(external_identifiers.keys())
                }
            }
            
        except Exception as e:
            return {"error": f"Failed to analyze Synapse folder {synapse_id}: {str(e)}"}

    def _get_children_recursive(self, parent_id: str, files_list: list):
        """Recursively get all file children of a Synapse entity."""
        try:
            children = list(self.syn.getChildren(parent_id, includeTypes=["file", "folder"]))
            
            for child in children:
                child_id = child['id']
                child_type = child.get('type', '')
                
                if 'FileEntity' in child_type or child_type.endswith('.FileEntity'):
                    # Get file entity with annotations
                    try:
                        file_entity = self.syn.get(child_id, downloadFile=False)
                        file_info = {
                            'id': file_entity.id,
                            'name': file_entity.name,
                            'path': getattr(file_entity, 'path', ''),
                            'contentType': getattr(file_entity, 'contentType', ''),
                            'contentSize': getattr(file_entity, 'contentSize', 0),
                            'annotations': dict(file_entity.annotations) if hasattr(file_entity, 'annotations') else {},
                            'description': getattr(file_entity, 'description', ''),
                            'parent_id': parent_id
                        }
                        files_list.append(file_info)
                    except Exception as e:
                        print(f"Warning: Could not get file entity {child_id}: {e}")
                
                elif 'FolderEntity' in child_type or 'Folder' in child_type or child_type.endswith('.Folder'):
                    # Recurse into folder
                    self._get_children_recursive(child_id, files_list)
                    
        except Exception as e:
            print(f"Warning: Could not get children of {parent_id}: {e}")

    def _classify_file(self, file_info: dict) -> str:
        """Classify a file as data, metadata, or other based on basic heuristics."""
        name = file_info['name'].lower()
        content_type = file_info.get('contentType', '').lower()
        description = file_info.get('description', '').lower()
        
        # Very basic classification - let the LLM make the final decision
        # Text-based files are likely metadata
        if any(ext in name for ext in ['.txt', '.csv', '.tsv', '.json', '.xml', '.yaml', '.yml', '.md', '.readme']):
            return "metadata"
        elif any(word in name for word in ['readme', 'metadata', 'manifest', 'protocol', 'info']):
            return "metadata"
        elif any(word in description for word in ['metadata', 'manifest', 'protocol', 'readme']):
            return "metadata"
        # Everything else is potentially data - LLM will make final determination
        else:
            return "data"

    def _extract_external_identifiers(self, files_list: list, main_entity) -> dict:
        """Extract external identifiers from file names, descriptions, and annotations."""
        identifiers = {}
        
        # Common identifier patterns
        patterns = {
            'pride': r'PXD\d{6}',
            'geo': r'GSE\d+',
            'sra': r'SRP\d+|SRR\d+|SRX\d+|SRS\d+',
            'ena': r'ERP\d+|ERR\d+|ERS\d+|ERX\d+|PRJ[END][AB]\d+',
            'arrayexpress': r'E-\w+-\d+',
            'dbgap': r'phs\d+',
            'doi': r'10\.\d+/[^\s]+',
            'pubmed': r'PMID:\s*\d+|pubmed:\s*\d+',
            'uniprot': r'[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}'
        }
        
        # Search in main entity
        search_text = f"{main_entity.name} {getattr(main_entity, 'description', '')}"
        if hasattr(main_entity, 'annotations'):
            for key, values in main_entity.annotations.items():
                if isinstance(values, (list, tuple)):
                    search_text += " " + " ".join(str(v) for v in values)
                else:
                    search_text += " " + str(values)
        
        # Search in all files
        for file_info in files_list:
            search_text += f" {file_info['name']} {file_info.get('description', '')}"
            for key, values in file_info.get('annotations', {}).items():
                if isinstance(values, (list, tuple)):
                    search_text += " " + " ".join(str(v) for v in values)
                else:
                    search_text += " " + str(values)
        
        # Find matches
        for id_type, pattern in patterns.items():
            matches = re.findall(pattern, search_text, re.IGNORECASE)
            if matches:
                identifiers[id_type] = list(set(matches))  # Remove duplicates
        
        return identifiers


class MetadataFileAnalysisInput(BaseModel):
    """Input for analyzing metadata files."""
    file_ids: List[str] = Field(description="List of Synapse file IDs to download and analyze for metadata")
    max_files: int = Field(default=5, description="Maximum number of files to download and analyze")


class MetadataFileAnalysisTool(BaseTool):
    name: str = "Metadata File Analysis Tool"
    description: str = (
        "Downloads and analyzes metadata files from Synapse to extract structured "
        "information that can be used for annotation. Supports common formats like "
        "CSV, TSV, JSON, XML, and text files. Files larger than 10MB are automatically "
        "skipped to avoid performance issues. Returns extracted metadata in a "
        "structured format."
    )
    args_schema: Type[BaseModel] = MetadataFileAnalysisInput
    syn: Optional[synapseclient.Synapse] = None

    def __init__(self, syn: synapseclient.Synapse, **kwargs):
        super().__init__(**kwargs)
        self.syn = syn

    def _run(self, file_ids: List[str], max_files: int = 5) -> dict:
        """
        Downloads and analyzes metadata files.
        
        Args:
            file_ids: List of Synapse file IDs to analyze
            max_files: Maximum number of files to process
            
        Returns:
            Dictionary containing extracted metadata
        """
        if not self.syn:
            return {"error": "Synapse client not initialized. Login failed."}
        
        if not file_ids:
            return {"metadata": {}, "message": "No metadata files provided"}
        
        # Limit the number of files to process
        file_ids = file_ids[:max_files]
        
        extracted_metadata = {}
        max_file_size = 10 * 1024 * 1024  # 10MB in bytes
        
        for file_id in file_ids:
            try:
                # First get file entity without downloading to check size
                file_entity_info = self.syn.get(file_id, downloadFile=False)
                file_name = file_entity_info.name
                file_size = getattr(file_entity_info, 'contentSize', 0)
                
                # Check file size limit
                if file_size > max_file_size:
                    extracted_metadata[file_id] = {
                        'file_name': file_name,
                        'file_id': file_id,
                        'skipped': True,
                        'reason': f'File too large ({file_size / (1024*1024):.1f}MB > 10MB limit)',
                        'file_size_mb': file_size / (1024*1024)
                    }
                    continue
                
                # Download file to temporary location
                file_entity = self.syn.get(file_id, downloadLocation=tempfile.gettempdir())
                file_path = file_entity.path
                
                # Parse file based on extension
                metadata = self._parse_metadata_file(file_path, file_name)
                if metadata:
                    extracted_metadata[file_id] = {
                        'file_name': file_name,
                        'file_id': file_id,
                        'file_size_mb': file_size / (1024*1024) if file_size else 0,
                        'metadata': metadata
                    }
                
                # Clean up downloaded file
                if os.path.exists(file_path):
                    os.remove(file_path)
                    
            except Exception as e:
                extracted_metadata[file_id] = {
                    'file_name': f'unknown_{file_id}',
                    'file_id': file_id,
                    'error': str(e)
                }
        
        return {
            "extracted_metadata": extracted_metadata,
            "files_processed": len(file_ids),
            "successful_extractions": len([v for v in extracted_metadata.values() if 'metadata' in v]),
            "skipped_files": len([v for v in extracted_metadata.values() if v.get('skipped', False)]),
            "skipped_details": [v for v in extracted_metadata.values() if v.get('skipped', False)]
        }

    def _parse_metadata_file(self, file_path: str, file_name: str) -> dict:
        """Parse a metadata file and extract structured information."""
        try:
            file_ext = os.path.splitext(file_name)[1].lower()
            
            if file_ext in ['.csv', '.tsv']:
                return self._parse_tabular_file(file_path, file_ext)
            elif file_ext == '.json':
                return self._parse_json_file(file_path)
            elif file_ext in ['.xml']:
                return self._parse_xml_file(file_path)
            elif file_ext in ['.txt', '.md', '.readme']:
                return self._parse_text_file(file_path)
            elif file_ext in ['.xlsx', '.xls']:
                return self._parse_excel_file(file_path)
            else:
                # Try to parse as text
                return self._parse_text_file(file_path)
                
        except Exception as e:
            return {"error": f"Failed to parse {file_name}: {str(e)}"}

    def _parse_tabular_file(self, file_path: str, file_ext: str) -> dict:
        """Parse CSV/TSV files."""
        delimiter = ',' if file_ext == '.csv' else '\t'
        
        try:
            # Try to read as pandas DataFrame
            df = pd.read_csv(file_path, delimiter=delimiter, nrows=100)  # Limit rows
            
            return {
                "type": "tabular",
                "columns": df.columns.tolist(),
                "row_count": len(df),
                "sample_data": df.head(5).to_dict('records'),
                "summary": df.describe(include='all').to_dict() if len(df) > 0 else {}
            }
        except Exception as e:
            return {"error": f"Failed to parse tabular file: {str(e)}"}

    def _parse_json_file(self, file_path: str) -> dict:
        """Parse JSON files."""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            return {
                "type": "json",
                "structure": self._get_json_structure(data),
                "data": data if isinstance(data, dict) and len(str(data)) < 10000 else "Data too large to include"
            }
        except Exception as e:
            return {"error": f"Failed to parse JSON file: {str(e)}"}

    def _parse_xml_file(self, file_path: str) -> dict:
        """Parse XML files (basic parsing)."""
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Basic XML structure extraction
            return {
                "type": "xml",
                "content_preview": content[:1000] + "..." if len(content) > 1000 else content,
                "size": len(content)
            }
        except Exception as e:
            return {"error": f"Failed to parse XML file: {str(e)}"}

    def _parse_text_file(self, file_path: str) -> dict:
        """Parse text files."""
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            lines = content.split('\n')
            
            return {
                "type": "text",
                "line_count": len(lines),
                "content_preview": content[:1000] + "..." if len(content) > 1000 else content,
                "key_value_pairs": self._extract_key_value_pairs(content)
            }
        except Exception as e:
            return {"error": f"Failed to parse text file: {str(e)}"}

    def _parse_excel_file(self, file_path: str) -> dict:
        """Parse Excel files."""
        try:
            # Read first sheet
            df = pd.read_excel(file_path, nrows=100)
            
            return {
                "type": "excel",
                "columns": df.columns.tolist(),
                "row_count": len(df),
                "sample_data": df.head(5).to_dict('records'),
                "summary": df.describe(include='all').to_dict() if len(df) > 0 else {}
            }
        except Exception as e:
            return {"error": f"Failed to parse Excel file: {str(e)}"}

    def _get_json_structure(self, data, max_depth=3, current_depth=0):
        """Get structure of JSON data."""
        if current_depth >= max_depth:
            return "..."
        
        if isinstance(data, dict):
            return {k: self._get_json_structure(v, max_depth, current_depth + 1) for k, v in list(data.items())[:10]}
        elif isinstance(data, list):
            if len(data) > 0:
                return [self._get_json_structure(data[0], max_depth, current_depth + 1)]
            else:
                return []
        else:
            return type(data).__name__

    def _extract_key_value_pairs(self, content: str) -> dict:
        """Extract key-value pairs from text content."""
        pairs = {}
        
        # Look for patterns like "key: value" or "key = value"
        patterns = [
            r'([^:\n=]+):\s*([^\n]+)',
            r'([^=\n:]+)=\s*([^\n]+)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for key, value in matches:
                key = key.strip()
                value = value.strip()
                if len(key) > 0 and len(value) > 0 and len(key) < 100:
                    pairs[key] = value
        
        return pairs


class TemplateDetectionInput(BaseModel):
    """Input for template detection."""
    file_analysis: dict = Field(description="File analysis results from SynapseFolderAnalysisTool")
    metadata_analysis: dict = Field(default={}, description="Metadata analysis results from MetadataFileAnalysisTool")
    data_model_url: str = Field(
        default="https://raw.githubusercontent.com/nf-osi/nf-metadata-dictionary/main/NF.jsonld",
        description="URL or path to the JSON-LD data model"
    )


class TemplateDetectionTool(BaseTool):
    name: str = "Template Detection Tool"
    description: str = (
        "Analyzes file types, external identifiers, and metadata to determine the most "
        "appropriate JSON-LD metadata template for annotation. Returns all available "
        "templates and extracted information for LLM decision-making rather than "
        "making hardcoded assumptions about data types."
    )
    args_schema: Type[BaseModel] = TemplateDetectionInput

    def _run(self, file_analysis: dict, metadata_analysis: dict = {}, data_model_url: str = "https://raw.githubusercontent.com/nf-osi/nf-metadata-dictionary/main/NF.jsonld") -> dict:
        """
        Gathers available templates and file information for LLM decision-making.
        
        Args:
            file_analysis: Results from folder analysis
            metadata_analysis: Results from metadata file analysis
            data_model_url: URL to JSON-LD data model
            
        Returns:
            Dictionary with all available templates and analysis data for LLM processing
        """
        try:
            # Load JSON-LD schema
            jsonld_data = _load_jsonld(data_model_url)
            
            # Get all available templates
            manifests_tool = JsonLdGetManifestsTool()
            templates = manifests_tool._run(data_model_url)
            
            # Extract template information more systematically
            template_options = []
            for template in templates:
                if isinstance(template, dict):
                    template_info = {
                        'id': template.get('@id', ''),
                        'label': template.get('rdfs:label', ''),
                        'description': template.get('rdfs:comment', ''),
                        'attributes': self._extract_template_attributes(jsonld_data, template)
                    }
                    # Only include if it has a meaningful label or ID
                    if template_info['label'] or 'Template' in template_info['id']:
                        template_options.append(template_info)
            
            # Summarize file analysis for easier LLM processing
            data_files = file_analysis.get('data_files', [])
            metadata_files = file_analysis.get('metadata_files', [])
            external_ids = file_analysis.get('external_identifiers', {})
            
            # Extract file extensions and types for pattern analysis
            file_extensions = {}
            for file_info in data_files:
                ext = os.path.splitext(file_info['name'])[1].lower()
                if ext:
                    file_extensions[ext] = file_extensions.get(ext, 0) + 1
            
            # Extract content types
            content_types = {}
            for file_info in data_files:
                ct = file_info.get('contentType', 'unknown')
                content_types[ct] = content_types.get(ct, 0) + 1
            
            return {
                "available_templates": template_options,
                "file_analysis_summary": {
                    "total_files": len(data_files) + len(metadata_files),
                    "data_file_count": len(data_files),
                    "metadata_file_count": len(metadata_files),
                    "file_extensions": file_extensions,
                    "content_types": content_types,
                    "sample_data_files": [
                        {
                            'name': f['name'],
                            'size': f.get('contentSize', 0),
                            'type': f.get('contentType', 'unknown')
                        } for f in data_files[:10]  # Show first 10 files
                    ]
                },
                "external_identifiers": external_ids,
                "metadata_analysis": metadata_analysis,
                "entity_info": {
                    "name": file_analysis.get('entity_name', ''),
                    "type": file_analysis.get('entity_type', ''),
                    "synapse_id": file_analysis.get('synapse_id', '')
                },
                "data_model_url": data_model_url
            }
            
        except Exception as e:
            return {"error": f"Failed to analyze templates and data: {str(e)}"}

    def _extract_template_attributes(self, jsonld_data: dict, template: dict) -> list:
        """Extract attributes defined for a template."""
        template_id = template.get('@id', '')
        
        attributes = []
        
        # Find all properties that have this template as domain or are required by it
        for item in jsonld_data.get('@graph', []):
            if 'sms:domainIncludes' in item:
                domain_includes = item['sms:domainIncludes']
                if not isinstance(domain_includes, list):
                    domain_includes = [domain_includes]
                
                for domain in domain_includes:
                    domain_id = domain.get('@id') if isinstance(domain, dict) else domain
                    if domain_id == template_id:
                        attr_info = {
                            'id': item.get('@id', ''),
                            'label': item.get('rdfs:label', ''),
                            'description': item.get('rdfs:comment', ''),
                            'required': item.get('sms:required', False)
                        }
                        attributes.append(attr_info)
        
        return attributes


class AnnotationCSVSaveInput(BaseModel):
    """Input for saving annotations as CSV."""
    annotations: List[dict] = Field(description="List of annotation specifications with entity_id and annotations")
    output_path: str = Field(description="Path where to save the CSV file")


class AnnotationCSVSaveTool(BaseTool):
    name: str = "Annotation CSV Save Tool"
    description: str = (
        "Saves annotation specifications to a CSV file for record keeping and review. "
        "Creates a structured CSV with one row per file and columns for each annotation attribute."
    )
    args_schema: Type[BaseModel] = AnnotationCSVSaveInput

    def _run(self, annotations: List[dict], output_path: str) -> str:
        """
        Saves annotations to a CSV file.
        
        Args:
            annotations: List of annotation specifications
            output_path: Path to save CSV file
            
        Returns:
            Status message
        """
        try:
            if not annotations:
                return "No annotations to save."
            
            # Collect all unique annotation keys
            all_keys = set(['entity_id', 'file_name'])
            for ann_spec in annotations:
                ann_dict = ann_spec.get('annotations', {})
                all_keys.update(ann_dict.keys())
            
            # Create CSV data
            csv_data = []
            for ann_spec in annotations:
                row = {
                    'entity_id': ann_spec.get('entity_id', ''),
                    'file_name': ann_spec.get('file_name', '')
                }
                
                ann_dict = ann_spec.get('annotations', {})
                for key in all_keys:
                    if key not in ['entity_id', 'file_name']:
                        value = ann_dict.get(key, '')
                        # Handle list values
                        if isinstance(value, list):
                            value = '; '.join(str(v) for v in value)
                        row[key] = value
                
                csv_data.append(row)
            
            # Write CSV
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                if csv_data:
                    fieldnames = list(csv_data[0].keys())
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(csv_data)
            
            return f"Successfully saved {len(annotations)} annotations to {output_path}"
            
        except Exception as e:
            return f"Failed to save annotations to CSV: {str(e)}" 


class SingleAttributeAnnotationInput(BaseModel):
    """Input for annotating a single attribute across multiple files."""
    attribute_name: str = Field(description="Name of the attribute to annotate (e.g., 'Assay', 'DataType')")
    attribute_description: str = Field(description="Description of what this attribute represents")
    valid_values: List[str] = Field(default=[], description="List of valid controlled vocabulary values for this attribute")
    file_names: List[str] = Field(description="List of file names to annotate")
    available_metadata: dict = Field(default={}, description="Available metadata to help with annotation decisions")
    data_model_url: str = Field(
        default="https://raw.githubusercontent.com/nf-osi/nf-metadata-dictionary/main/NF.jsonld",
        description="URL or path to the JSON-LD data model"
    )


class SingleAttributeAnnotationTool(BaseTool):
    name: str = "Single Attribute Annotation Tool"
    description: str = (
        "Annotates a single attribute across multiple files. Provides controlled "
        "vocabulary options and metadata context to help make consistent annotation "
        "decisions for one attribute at a time."
    )
    args_schema: Type[BaseModel] = SingleAttributeAnnotationInput

    def _run(self, attribute_name: str, attribute_description: str, valid_values: List[str], file_names: List[str], available_metadata: dict = {}, data_model_url: str = "https://raw.githubusercontent.com/nf-osi/nf-metadata-dictionary/main/NF.jsonld") -> dict:
        """
        Provides annotation guidance for a single attribute.
        
        Args:
            attribute_name: Name of the attribute to annotate
            attribute_description: Description of the attribute
            valid_values: List of valid controlled vocabulary values
            file_names: List of file names to annotate
            available_metadata: Available metadata for decision making
            data_model_url: URL to JSON-LD data model
            
        Returns:
            Dictionary with annotation guidance and options
        """
        try:
            return {
                "attribute": {
                    "name": attribute_name,
                    "description": attribute_description,
                    "has_controlled_vocabulary": len(valid_values) > 0,
                    "valid_values": valid_values[:50] if valid_values else None  # Limit for readability
                },
                "files_to_annotate": file_names,
                "file_count": len(file_names),
                "available_metadata": available_metadata,
                "annotation_guidance": self._generate_attribute_guidance(
                    attribute_name, attribute_description, valid_values, available_metadata
                )
            }
            
        except Exception as e:
            return {"error": f"Failed to generate annotation guidance: {str(e)}"}

    def _generate_attribute_guidance(self, attribute_name: str, description: str, valid_values: List[str], metadata: dict) -> str:
        """Generate guidance for annotating a specific attribute."""
        
        has_vocab = len(valid_values) > 0
        vocab_text = f"Choose from {len(valid_values)} controlled vocabulary options" if has_vocab else "Free text value"
        
        guidance = f"""
        ANNOTATING ATTRIBUTE: {attribute_name}
        
        DESCRIPTION: {description}
        
        VALUE TYPE: {vocab_text}
        
        INSTRUCTIONS:
        1. Review the attribute description and understand what it represents
        2. {"Use ONLY values from the controlled vocabulary list" if has_vocab else "Provide an appropriate text value"}
        3. Apply consistent values across files when appropriate (study-level metadata)
        4. Use file-specific values when they differ (sample-level metadata)
        5. Use available metadata to inform decisions
        6. If unsure, derive reasonable values from file names or external identifiers
        
        AVAILABLE METADATA SOURCES:
        - External identifiers: {list(metadata.get('external_identifiers', {}).keys())}
        - Sample metadata: {list(metadata.get('sample_metadata', {}).keys())}
        
        OUTPUT FORMAT:
        For each file, provide: {{"file_name": "value"}}
        """
        
        return guidance


class AnnotationCSVBuilderInput(BaseModel):
    """Input for building annotation CSV incrementally."""
    csv_path: str = Field(description="Path to the CSV file to create or update")
    file_names: List[str] = Field(description="List of file names (creates initial CSV if new)")
    attribute_name: str = Field(default="", description="Name of attribute column to add/update")
    attribute_values: dict = Field(default={}, description="Dictionary mapping file_name to attribute value")
    synapse_ids: dict = Field(default={}, description="Dictionary mapping file_name to synapse_id (for initial CSV)")


class AnnotationCSVBuilderTool(BaseTool):
    name: str = "Annotation CSV Builder Tool"
    description: str = (
        "Builds or updates an annotation CSV file incrementally. Can create initial "
        "CSV with file names and IDs, then add attribute columns one at a time."
    )
    args_schema: Type[BaseModel] = AnnotationCSVBuilderInput

    def _run(self, csv_path: str, file_names: List[str], attribute_name: str = "", attribute_values: dict = {}, synapse_ids: dict = {}) -> str:
        """
        Creates or updates annotation CSV file.
        
        Args:
            csv_path: Path to CSV file
            file_names: List of file names
            attribute_name: Name of attribute to add (empty for initial CSV)
            attribute_values: Values for the attribute {file_name: value}
            synapse_ids: Synapse IDs for files {file_name: synapse_id}
            
        Returns:
            Status message
        """
        try:
            # Check if CSV exists
            csv_exists = os.path.exists(csv_path)
            
            if not csv_exists and not synapse_ids:
                return "Error: Cannot create new CSV without synapse_ids mapping"
            
            if not csv_exists:
                # Create initial CSV with file names and synapse IDs
                csv_data = []
                for file_name in file_names:
                    row = {
                        'file_name': file_name,
                        'synapse_id': synapse_ids.get(file_name, '')
                    }
                    csv_data.append(row)
                
                # Write initial CSV
                with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ['file_name', 'synapse_id']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(csv_data)
                
                return f"Created initial CSV with {len(csv_data)} files at {csv_path}"
            
            elif attribute_name and attribute_values:
                # Add/update attribute column
                # Read existing CSV
                existing_data = []
                with open(csv_path, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    fieldnames = reader.fieldnames
                    existing_data = list(reader)
                
                # Update fieldnames if new attribute
                if attribute_name not in fieldnames:
                    fieldnames = list(fieldnames) + [attribute_name]
                
                # Update data with new attribute values
                for row in existing_data:
                    file_name = row['file_name']
                    if file_name in attribute_values:
                        row[attribute_name] = attribute_values[file_name]
                    elif attribute_name not in row:
                        row[attribute_name] = ''  # Default empty for missing values
                
                # Write updated CSV
                with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(existing_data)
                
                return f"Updated CSV with attribute '{attribute_name}' for {len(attribute_values)} files"
            
            else:
                return "Error: Must provide either synapse_ids (for new CSV) or attribute_name+attribute_values (for update)"
                
        except Exception as e:
            return f"Failed to build/update CSV: {str(e)}"


class ApplyAnnotationsFromCSVInput(BaseModel):
    """Input for applying annotations from CSV to Synapse."""
    csv_path: str = Field(description="Path to the completed annotation CSV file")
    dry_run: bool = Field(default=True, description="If True, only show what would be applied without making changes")


class ApplyAnnotationsFromCSVTool(BaseTool):
    name: str = "Apply Annotations From CSV Tool"
    description: str = (
        "Reads a completed annotation CSV file and applies all annotations to "
        "Synapse files. Supports dry-run mode to preview changes before applying."
    )
    args_schema: Type[BaseModel] = ApplyAnnotationsFromCSVInput
    syn: Optional[synapseclient.Synapse] = None

    def __init__(self, syn: synapseclient.Synapse, **kwargs):
        super().__init__(**kwargs)
        self.syn = syn

    def _run(self, csv_path: str, dry_run: bool = True) -> str:
        """
        Applies annotations from CSV to Synapse files.
        
        Args:
            csv_path: Path to annotation CSV file
            dry_run: If True, only preview changes
            
        Returns:
            Status message with results
        """
        try:
            if not os.path.exists(csv_path):
                return f"Error: CSV file not found at {csv_path}"
            
            # Read annotation CSV
            annotations_data = []
            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                annotations_data = list(reader)
            
            if not annotations_data:
                return "Error: CSV file is empty"
            
            # Process annotations
            results = []
            annotation_columns = [col for col in annotations_data[0].keys() 
                                if col not in ['file_name', 'synapse_id']]
            
            for row in annotations_data:
                synapse_id = row.get('synapse_id', '')
                file_name = row.get('file_name', '')
                
                if not synapse_id:
                    results.append(f"❌ {file_name}: Missing synapse_id")
                    continue
                
                # Collect non-empty annotations
                annotations = {}
                for col in annotation_columns:
                    value = row.get(col, '').strip()
                    if value:
                        annotations[col] = value
                
                if not annotations:
                    results.append(f"⚠️  {file_name}: No annotations to apply")
                    continue
                
                if dry_run:
                    annotation_preview = ', '.join(f"{k}='{v}'" for k, v in annotations.items())
                    results.append(f"📋 {file_name} ({synapse_id}): {annotation_preview}")
                else:
                    # Apply annotations to Synapse
                    try:
                        if self.syn:
                            entity = self.syn.get(synapse_id, downloadFile=False)
                            entity.annotations.update(annotations)
                            self.syn.store(entity, forceVersion=False)
                            results.append(f"✅ {file_name}: Applied {len(annotations)} annotations")
                        else:
                            results.append(f"❌ {file_name}: Synapse client not available")
                    except Exception as e:
                        results.append(f"❌ {file_name}: Failed to apply annotations - {str(e)}")
            
            # Summary
            mode = "DRY RUN - " if dry_run else ""
            summary = f"{mode}Processed {len(annotations_data)} files with {len(annotation_columns)} attributes"
            
            return f"{summary}\n\n" + '\n'.join(results)
            
        except Exception as e:
            return f"Failed to apply annotations from CSV: {str(e)}" 