"""
Utility functions for dataset annotation workflows.
These functions help simplify common annotation patterns.
"""

import synapseclient
from typing import List, Dict, Any
import os
import re


def extract_file_types(file_list: List[Dict]) -> Dict[str, int]:
    """
    Extract and count file types from a list of file info dictionaries.
    
    Args:
        file_list: List of file info dictionaries with 'name' field
        
    Returns:
        Dictionary mapping file extensions to counts
    """
    file_types = {}
    for file_info in file_list:
        filename = file_info.get('name', '')
        # Handle compressed files like .fastq.gz
        if filename.endswith('.gz'):
            # Get the extension before .gz
            base_name = filename[:-3]
            if '.' in base_name:
                ext = '.' + base_name.split('.')[-1] + '.gz'
            else:
                ext = '.gz'
        else:
            ext = os.path.splitext(filename)[1].lower()
        
        if ext:
            file_types[ext] = file_types.get(ext, 0) + 1
    
    return file_types


def suggest_template_from_files(file_types: Dict[str, int], external_ids: Dict = None) -> str:
    """
    Suggest an appropriate template based on file types and external identifiers.
    
    Args:
        file_types: Dictionary mapping file extensions to counts
        external_ids: Dictionary of external identifiers found
        
    Returns:
        Suggested template name
    """
    # Check for sequencing data
    sequencing_extensions = {'.fastq.gz', '.fastq', '.fq.gz', '.fq', '.bam', '.sam'}
    has_sequencing = any(ext in file_types for ext in sequencing_extensions)
    
    # Check for single-cell indicators
    single_cell_indicators = ['10x', 'cellranger', 'single.cell', 'sc.rna', 'scrna']
    
    # Check for bulk RNA-seq indicators
    bulk_rna_indicators = ['bulk', 'rna.seq', 'rnaseq']
    
    if has_sequencing:
        # Check file names and external IDs for single-cell indicators
        has_single_cell = False
        if external_ids:
            all_text = ' '.join(str(v).lower() for v in external_ids.values())
            has_single_cell = any(indicator in all_text for indicator in single_cell_indicators)
        
        if has_single_cell:
            return "ScRNASeqTemplate"
        else:
            return "RNASeqTemplate"  # Default for sequencing data
    
    # Check for other data types
    if '.xlsx' in file_types or '.csv' in file_types:
        return "DataTemplate"  # Generic template for tabular data
    
    # Default fallback
    return "DataTemplate"


def create_basic_annotations(template: str, file_info: Dict, additional_annotations: Dict = None) -> Dict[str, Any]:
    """
    Create basic annotations for a file based on the template and file information.
    
    Args:
        template: Template name to use
        file_info: File information dictionary
        additional_annotations: Additional annotations to include
        
    Returns:
        Dictionary of annotations
    """
    annotations = {}
    
    # Basic annotations for all templates
    filename = file_info.get('name', '')
    file_size = file_info.get('contentSize', 0)
    
    # Set basic required fields
    annotations['Component'] = 'DataFile'
    annotations['Filename'] = filename
    
    # Set file format based on extension
    if filename.endswith('.fastq.gz') or filename.endswith('.fq.gz'):
        annotations['fileFormat'] = 'fastq'
    elif filename.endswith('.fastq') or filename.endswith('.fq'):
        annotations['fileFormat'] = 'fastq'
    elif filename.endswith('.bam'):
        annotations['fileFormat'] = 'bam'
    elif filename.endswith('.sam'):
        annotations['fileFormat'] = 'sam'
    elif filename.endswith('.csv'):
        annotations['fileFormat'] = 'csv'
    elif filename.endswith('.xlsx'):
        annotations['fileFormat'] = 'excel'
    else:
        # Try to infer from extension
        ext = os.path.splitext(filename)[1].lower()
        if ext:
            annotations['fileFormat'] = ext[1:]  # Remove the dot
    
    # Template-specific annotations
    if template == "RNASeqTemplate" or template == "ScRNASeqTemplate":
        annotations['resourceType'] = 'experimentalData'
        annotations['dataType'] = 'geneExpression'
        annotations['assay'] = 'rnaSeq'
        annotations['species'] = 'Homo sapiens'  # Default, should be updated based on actual data
        annotations['platform'] = 'Illumina'  # Default, should be updated based on actual data
        
        if template == "ScRNASeqTemplate":
            annotations['dataSubtype'] = 'raw'
            # Single-cell specific annotations could be added here
        else:
            annotations['dataSubtype'] = 'raw'
    
    # Add any additional annotations
    if additional_annotations:
        annotations.update(additional_annotations)
    
    return annotations


def prepare_batch_annotations(data_files: List[Dict], template: str, common_annotations: Dict = None) -> List[Dict]:
    """
    Prepare a list of annotation specifications for batch application.
    
    Args:
        data_files: List of file information dictionaries
        template: Template name to use for all files
        common_annotations: Common annotations to apply to all files
        
    Returns:
        List of annotation specifications for SynapseBatchAnnotationTool
    """
    batch_annotations = []
    
    for file_info in data_files:
        entity_id = file_info.get('id')
        if not entity_id:
            continue
            
        annotations = create_basic_annotations(template, file_info, common_annotations)
        
        batch_annotations.append({
            'entity_id': entity_id,
            'annotations': annotations
        })
    
    return batch_annotations 