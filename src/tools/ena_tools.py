from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from typing import Type, List, Dict, Optional
import re
import requests


class EnaDatasetMetadataInput(BaseModel):
    """Input for fetching ENA dataset metadata."""
    ena_id: str = Field(description="The ENA accession ID (e.g., SRP399711, ERP123456, ERR123456)")


class EnaDatasetMetadataTool(BaseTool):
    name: str = "ENA Dataset Metadata Fetcher"
    description: str = (
        "Fetches metadata for an ENA dataset using the ENA Portal API. "
        "Supports various ENA accession types including SRP, ERP, ERR, ERS, ERX. "
        "Returns comprehensive dataset information including title, description, "
        "and study details."
    )
    args_schema: Type[BaseModel] = EnaDatasetMetadataInput

    def _run(self, ena_id: str) -> dict:
        """
        Fetches metadata for an ENA dataset.
        
        Args:
            ena_id: ENA accession ID (e.g., SRP399711, ERP123456, ERR123456)
            
        Returns:
            Dictionary containing metadata
        """
        try:
            # Determine the type of ENA accession and appropriate search strategy
            search_strategy = self._determine_search_strategy(ena_id)
            
            base_url = "https://www.ebi.ac.uk/ena/portal/api/search"
            
            # Try to get study-level metadata first
            if search_strategy['result_type'] == 'study':
                params = {
                    'result': 'study',
                    'query': search_strategy['query'],
                    'format': 'json',
                    'fields': 'study_accession,secondary_study_accession,study_title,study_description,center_name,first_public,last_updated'
                }
                
                response = requests.get(base_url, params=params, timeout=30)
                
                if response.status_code == 200:
                    studies = response.json()
                    if studies:
                        study = studies[0]
                        return {
                            "ena_id": ena_id,
                            "accession": study.get('study_accession', ena_id),
                            "secondary_accession": study.get('secondary_study_accession', ''),
                            "title": study.get('study_title', 'Unknown'),
                            "description": study.get('study_description', 'No description available'),
                            "center_name": study.get('center_name', ''),
                            "first_public": study.get('first_public', ''),
                            "last_updated": study.get('last_updated', ''),
                            "record_type": "study"
                        }
            
            # If not a study or no study found, try to get run-level metadata
            params = {
                'result': 'read_run',
                'query': search_strategy['query'],
                'format': 'json',
                'fields': 'run_accession,study_accession,secondary_study_accession,study_title,sample_accession,experiment_accession,library_strategy,library_source,platform,instrument_model',
                'limit': 5
            }
            
            response = requests.get(base_url, params=params, timeout=30)
            
            if response.status_code == 200:
                runs = response.json()
                if runs:
                    # Aggregate metadata from runs
                    run = runs[0]  # Use first run as representative
                    
                    # Collect unique values across all runs
                    library_strategies = set()
                    library_sources = set()
                    platforms = set()
                    instruments = set()
                    
                    for r in runs:
                        if r.get('library_strategy'):
                            library_strategies.add(r['library_strategy'])
                        if r.get('library_source'):
                            library_sources.add(r['library_source'])
                        if r.get('platform'):
                            platforms.add(r['platform'])
                        if r.get('instrument_model'):
                            instruments.add(r['instrument_model'])
                    
                    return {
                        "ena_id": ena_id,
                        "accession": run.get('study_accession', ena_id),
                        "secondary_accession": run.get('secondary_study_accession', ''),
                        "title": run.get('study_title', 'Unknown'),
                        "description": f"Dataset with {len(runs)} sequencing runs",
                        "total_runs_found": len(runs),
                        "library_strategy": list(library_strategies),
                        "library_source": list(library_sources),
                        "platform": list(platforms),
                        "instruments": list(instruments),
                        "record_type": "runs"
                    }
            
            # If nothing found, return error
            return {
                "ena_id": ena_id,
                "error": f"No ENA records found for {ena_id}",
                "title": "Unknown",
                "description": "Dataset not found"
            }
            
        except requests.RequestException as e:
            return {
                "ena_id": ena_id,
                "error": f"Failed to fetch ENA metadata: {str(e)}",
                "title": "Error",
                "description": "Network error occurred"
            }
        except Exception as e:
            return {
                "ena_id": ena_id,
                "error": f"Failed to parse ENA metadata: {str(e)}",
                "title": "Error",
                "description": "Parsing error occurred"
            }

    def _determine_search_strategy(self, ena_id: str) -> dict:
        """Determine the appropriate ENA search strategy based on accession type."""
        ena_id = ena_id.upper().strip()
        
        # Study-level accessions
        if ena_id.startswith(('SRP', 'ERP', 'DRP')):
            return {
                'result_type': 'study',
                'query': f'secondary_study_accession="{ena_id}"'
            }
        elif ena_id.startswith('PRJ'):
            return {
                'result_type': 'study', 
                'query': f'study_accession="{ena_id}"'
            }
        
        # Run-level accessions
        elif ena_id.startswith(('SRR', 'ERR', 'DRR')):
            return {
                'result_type': 'run',
                'query': f'run_accession="{ena_id}"'
            }
        
        # Sample-level accessions
        elif ena_id.startswith(('SRS', 'ERS', 'DRS')):
            return {
                'result_type': 'run',
                'query': f'sample_accession="{ena_id}"'
            }
        
        # Experiment-level accessions
        elif ena_id.startswith(('SRX', 'ERX', 'DRX')):
            return {
                'result_type': 'run',
                'query': f'experiment_accession="{ena_id}"'
            }
        
        # Default: try as secondary study accession first
        else:
            return {
                'result_type': 'study',
                'query': ena_id
            }


class EnaFastqFilesInput(BaseModel):
    """Input for fetching ENA FASTQ files."""
    ena_id: str = Field(description="The ENA accession ID (e.g., SRP399711, ERP123456, ERR123456)")


class EnaFastqFilesTool(BaseTool):
    name: str = "ENA FASTQ Files Fetcher"
    description: str = (
        "Fetches FASTQ files directly from ENA (European Nucleotide Archive). "
        "Supports various ENA accession types including SRP, ERP, ERR, ERS, ERX. "
        "Returns direct download links to FASTQ files with accurate file sizes and MD5 hashes. "
        "Much more efficient than SRA tools when you only need FASTQ files."
    )
    args_schema: Type[BaseModel] = EnaFastqFilesInput

    def _run(self, ena_id: str) -> dict:
        """
        Fetches FASTQ files directly from ENA.
        
        Args:
            ena_id: ENA accession ID (e.g., SRP399711, ERP123456, ERR123456)
            
        Returns:
            Dictionary containing FASTQ file information
        """
        try:
            files = []
            warnings = []
            
            # Determine search strategy
            search_strategy = self._determine_search_strategy(ena_id)
            
            base_url = "https://www.ebi.ac.uk/ena/portal/api/search"
            params = {
                'result': 'read_run',
                'query': search_strategy['query'],
                'format': 'json',
                'fields': 'run_accession,fastq_ftp,fastq_md5,fastq_bytes'
            }
            
            response = requests.get(base_url, params=params, timeout=30)
            
            if response.status_code == 200:
                runs_data = response.json()
                
                if not runs_data:
                    warnings.append(f"No runs found for ENA accession {ena_id}")
                    return {
                        "ena_id": ena_id,
                        "total_files": 0,
                        "files": [],
                        "warnings": warnings,
                        "summary": f"No FASTQ files found for {ena_id}"
                    }
                
                for run in runs_data:
                    run_acc = run.get('run_accession', '')
                    
                    if run_acc:
                        # Get FASTQ URLs
                        fastq_ftp = run.get('fastq_ftp', '')
                        fastq_md5 = run.get('fastq_md5', '')
                        fastq_bytes = run.get('fastq_bytes', '')
                        
                        if fastq_ftp:
                            # Parse multiple FASTQ files (paired-end)
                            ftp_urls = fastq_ftp.split(';')
                            md5_hashes = fastq_md5.split(';') if fastq_md5 else []
                            file_sizes = fastq_bytes.split(';') if fastq_bytes else []
                            
                            for i, ftp_url in enumerate(ftp_urls):
                                if ftp_url.strip():
                                    file_name = ftp_url.split('/')[-1]
                                    # Get actual file size from fastq_bytes
                                    file_size = 0
                                    if i < len(file_sizes) and file_sizes[i]:
                                        try:
                                            file_size = int(file_sizes[i])
                                        except (ValueError, TypeError):
                                            file_size = 0
                                    
                                    files.append({
                                        'file_name': file_name,
                                        'file_size': file_size,
                                        'file_category': 'FASTQ',
                                        'file_type': 'FASTQ',
                                        'run_accession': run_acc,
                                        'download_url': f"https://{ftp_url}",
                                        'ftp_download_url': f"ftp://{ftp_url}",
                                        'compression': file_name.endswith('.gz'),
                                        'md5_hash': md5_hashes[i] if i < len(md5_hashes) else None,
                                        'public_url': f"https://www.ebi.ac.uk/ena/browser/view/{run_acc}"
                                    })
                        else:
                            warnings.append(f"No FASTQ files available for run {run_acc}")
                
                if files:
                    warnings.append(
                        f"Found {len(files)} FASTQ files from {len(runs_data)} runs in ENA. "
                        "These files provide direct access to FASTQ data without conversion."
                    )
                else:
                    warnings.append(
                        f"No FASTQ files found for {ena_id}. The dataset may only have SRA format files."
                    )
            
            else:
                warnings.append(f"ENA API returned status {response.status_code}")
            
            # Categorize files
            file_categories = self._categorize_files(files)
            
            return {
                "ena_id": ena_id,
                "total_files": len(files),
                "files": files,
                "file_categories": file_categories,
                "warnings": warnings,
                "summary": f"Found {len(files)} FASTQ files for ENA accession {ena_id}"
            }
            
        except requests.RequestException as e:
            return {
                "ena_id": ena_id,
                "error": f"Failed to fetch ENA FASTQ files: {str(e)}",
                "files": [],
                "warnings": [f"ERROR: Network error occurred: {str(e)}"]
            }
        except Exception as e:
            return {
                "ena_id": ena_id,
                "error": f"Failed to parse ENA data: {str(e)}",
                "files": [],
                "warnings": [f"ERROR: Parsing error occurred: {str(e)}"]
            }

    def _determine_search_strategy(self, ena_id: str) -> dict:
        """Determine the appropriate ENA search strategy based on accession type."""
        ena_id = ena_id.upper().strip()
        
        # Study-level accessions - use the same format as SRA tools since it works
        if ena_id.startswith(('SRP', 'ERP', 'DRP')):
            return {'query': f'secondary_study_accession="{ena_id}"'}
        elif ena_id.startswith('PRJ'):
            return {'query': f'study_accession="{ena_id}"'}
        
        # Run-level accessions
        elif ena_id.startswith(('SRR', 'ERR', 'DRR')):
            return {'query': f'run_accession="{ena_id}"'}
        
        # Sample-level accessions
        elif ena_id.startswith(('SRS', 'ERS', 'DRS')):
            return {'query': f'sample_accession="{ena_id}"'}
        
        # Experiment-level accessions
        elif ena_id.startswith(('SRX', 'ERX', 'DRX')):
            return {'query': f'experiment_accession="{ena_id}"'}
        
        # Default: try as general search
        else:
            return {'query': ena_id}

    def _categorize_files(self, files: List[Dict]) -> Dict:
        """Categorize files by type."""
        categories = {
            'FASTQ': [],
            'FASTQ_PAIRED': [],
            'FASTQ_SINGLE': []
        }
        
        # Group files by run to identify paired vs single-end
        runs = {}
        for file_info in files:
            run_acc = file_info.get('run_accession', '')
            if run_acc not in runs:
                runs[run_acc] = []
            runs[run_acc].append(file_info['file_name'])
        
        # Categorize based on pairing
        for run_acc, file_names in runs.items():
            if len(file_names) == 2:
                categories['FASTQ_PAIRED'].extend(file_names)
            elif len(file_names) == 1:
                categories['FASTQ_SINGLE'].extend(file_names)
            categories['FASTQ'].extend(file_names)
                
        return {k: len(v) for k, v in categories.items()} 