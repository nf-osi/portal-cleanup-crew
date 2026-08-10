import requests
import xml.etree.ElementTree as ET
from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from typing import Type, List, Dict, Optional
import re
import time


class SraDatasetMetadataInput(BaseModel):
    """Input for fetching SRA dataset metadata."""
    srp_id: str = Field(description="The SRA Project accession ID (e.g., SRP399711)")


class SraDatasetMetadataTool(BaseTool):
    name: str = "SRA Dataset Metadata Fetcher"
    description: str = (
        "Fetches metadata for an SRA dataset using the NCBI E-utilities API. "
        "Returns comprehensive dataset information including title, description, "
        "experimental design, and sample information."
    )
    args_schema: Type[BaseModel] = SraDatasetMetadataInput

    def _run(self, srp_id: str) -> dict:
        """
        Fetches metadata for an SRA dataset.
        
        Args:
            srp_id: SRA Project ID (e.g., SRP399711)
            
        Returns:
            Dictionary containing metadata
        """
        try:
            # Step 1: Search for the SRP to get associated SRR IDs
            search_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
            search_params = {
                'db': 'sra',
                'term': f'{srp_id}[PRJA]',
                'retmax': 100,
                'retmode': 'xml'
            }
            
            search_response = requests.get(search_url, params=search_params, timeout=30)
            search_response.raise_for_status()
            
            # Parse search results
            search_root = ET.fromstring(search_response.content)
            id_list = search_root.find('.//IdList')
            
            if id_list is None or len(id_list) == 0:
                return {
                    "srp_id": srp_id,
                    "error": f"No SRA records found for {srp_id}",
                    "title": "Unknown",
                    "description": "Dataset not found"
                }
            
            # Get the first few IDs to fetch detailed metadata
            sra_ids = [id_elem.text for id_elem in id_list.findall('Id')][:5]  # Limit for efficiency
            
            # Step 2: Fetch detailed metadata
            fetch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
            fetch_params = {
                'db': 'sra',
                'id': ','.join(sra_ids),
                'retmode': 'xml'
            }
            
            fetch_response = requests.get(fetch_url, params=fetch_params, timeout=30)
            fetch_response.raise_for_status()
            
            # Parse metadata
            metadata = self._parse_sra_metadata(fetch_response.content, srp_id)
            return metadata
            
        except requests.RequestException as e:
            return {
                "srp_id": srp_id,
                "error": f"Failed to fetch SRA metadata: {str(e)}",
                "title": "Error",
                "description": "Network error occurred"
            }
        except Exception as e:
            return {
                "srp_id": srp_id,
                "error": f"Failed to parse SRA metadata: {str(e)}",
                "title": "Error", 
                "description": "Parsing error occurred"
            }

    def _parse_sra_metadata(self, xml_content: bytes, srp_id: str) -> dict:
        """Parse SRA XML metadata."""
        try:
            root = ET.fromstring(xml_content)
            
            # Initialize metadata structure
            metadata = {
                "srp_id": srp_id,
                "title": "Unknown",
                "description": "No description available",
                "organism": [],
                "library_strategy": [],
                "library_source": [],
                "platform": [],
                "submission_date": None,
                "publication_date": None,
                "total_samples": 0,
                "total_experiments": 0,
                "total_runs": 0
            }
            
            # Extract information from SRA XML
            experiments = root.findall('.//EXPERIMENT')
            runs = root.findall('.//RUN')
            samples = root.findall('.//SAMPLE')
            
            metadata["total_experiments"] = len(experiments)
            metadata["total_runs"] = len(runs)
            metadata["total_samples"] = len(samples)
            
            # Extract study information
            studies = root.findall('.//STUDY')
            if studies:
                study = studies[0]
                study_title = study.find('.//STUDY_TITLE')
                if study_title is not None:
                    metadata["title"] = study_title.text
                
                study_desc = study.find('.//STUDY_DESCRIPTION')
                if study_desc is not None:
                    metadata["description"] = study_desc.text
            
            # Extract sample/experiment details
            organisms = set()
            strategies = set()
            sources = set()
            platforms = set()
            
            for exp in experiments:
                # Library strategy
                lib_strategy = exp.find('.//LIBRARY_STRATEGY')
                if lib_strategy is not None:
                    strategies.add(lib_strategy.text)
                
                # Library source
                lib_source = exp.find('.//LIBRARY_SOURCE')
                if lib_source is not None:
                    sources.add(lib_source.text)
                
                # Platform
                platform = exp.find('.//PLATFORM')
                if platform is not None:
                    for child in platform:
                        platforms.add(child.tag)
            
            for sample in samples:
                # Organism
                org_elem = sample.find('.//SCIENTIFIC_NAME')
                if org_elem is not None:
                    organisms.add(org_elem.text)
            
            metadata["organism"] = list(organisms)
            metadata["library_strategy"] = list(strategies)
            metadata["library_source"] = list(sources)
            metadata["platform"] = list(platforms)
            
            return metadata
            
        except Exception as e:
            return {
                "srp_id": srp_id,
                "error": f"Failed to parse XML: {str(e)}",
                "title": "Error",
                "description": "XML parsing failed"
            }


class SraDatasetFilesInput(BaseModel):
    """Input for fetching SRA dataset file listings."""
    srp_id: str = Field(description="The SRA Project accession ID (e.g., SRP399711)")
    include_ena_fastq: bool = Field(default=True, description="Whether to also search for FASTQ files on ENA")


class SraDatasetFilesTool(BaseTool):
    name: str = "SRA Dataset Files Fetcher"
    description: str = (
        "Fetches the list of files available for an SRA dataset. Returns both SRA format files "
        "and optionally searches for equivalent FASTQ files on ENA. Provides warnings about "
        "file format limitations and download options."
    )
    args_schema: Type[BaseModel] = SraDatasetFilesInput

    def _run(self, srp_id: str, include_ena_fastq: bool = True) -> dict:
        """
        Fetches file information for an SRA dataset.
        
        Args:
            srp_id: SRA Project ID (e.g., SRP399711)
            include_ena_fastq: Whether to search for FASTQ alternatives on ENA
            
        Returns:
            Dictionary containing file information and warnings
        """
        try:
            files = []
            warnings = []
            
            # Step 1: Get SRA files
            sra_files = self._get_sra_files(srp_id)
            files.extend(sra_files)
            
            # Add warning about SRA format
            if sra_files:
                warnings.append(
                    "WARNING: SRA provides files in .sra format only. These require SRA Toolkit "
                    "to extract FASTQ files locally. For direct FASTQ access, consider ENA links below."
                )
            
            # Step 2: Optionally search for ENA FASTQ files
            ena_files = []
            if include_ena_fastq:
                ena_files = self._get_ena_fastq_files(srp_id)
                files.extend(ena_files)
                
                if ena_files:
                    warnings.append(
                        "INFO: Found equivalent FASTQ files on ENA (European Nucleotide Archive). "
                        "These provide direct access to FASTQ files without conversion."
                    )
                else:
                    warnings.append(
                        "INFO: No equivalent FASTQ files found on ENA. Only SRA format files are available."
                    )
            
            # Categorize files
            file_categories = self._categorize_sra_files(files)
            
            return {
                "srp_id": srp_id,
                "total_files": len(files),
                "files": files,
                "file_categories": file_categories,
                "warnings": warnings,
                "summary": f"Found {len(sra_files)} SRA files and {len(ena_files)} ENA FASTQ files for {srp_id}"
            }
            
        except Exception as e:
            return {
                "srp_id": srp_id,
                "error": f"Failed to fetch SRA files: {str(e)}",
                "files": [],
                "warnings": ["ERROR: Failed to fetch file information"]
            }

    def _get_sra_files(self, srp_id: str) -> List[Dict]:
        """Get SRA format files."""
        sra_files = []
        
        try:
            # Try the ENA API first - it's more reliable for getting run accessions
            ena_search_url = "https://www.ebi.ac.uk/ena/portal/api/search"
            search_params = {
                'result': 'read_run',
                'query': f'secondary_study_accession="{srp_id}"',
                'format': 'json',
                'fields': 'run_accession,base_count,read_count',
                'limit': 50
            }
            
            response = requests.get(ena_search_url, params=search_params, timeout=30)
            
            if response.status_code == 200:
                runs_data = response.json()
                
                for run in runs_data:
                    run_acc = run.get('run_accession', '')
                    
                    if run_acc and run_acc.startswith('SRR'):
                        # Get file size estimate from base count
                        base_count = run.get('base_count', 0)
                        read_count = run.get('read_count', 0)
                        
                        # Estimate SRA file size (typically 20-40% of base count)
                        if base_count:
                            try:
                                bases = int(base_count)
                                file_size = int(bases * 0.3)  # Conservative 30% estimate
                                # Ensure minimum reasonable size
                                if file_size < 1024 * 1024:  # Less than 1MB
                                    file_size = 10 * 1024 * 1024  # 10MB minimum
                            except (ValueError, TypeError):
                                file_size = 100 * 1024 * 1024  # 100MB default
                        else:
                            file_size = 100 * 1024 * 1024  # 100MB default
                        
                        sra_files.append({
                            'file_name': f"{run_acc}.sra",
                            'file_size': file_size,
                            'file_category': 'SRA',
                            'file_type': 'SRA',
                            'run_accession': run_acc,
                            'download_url': f"https://sra-downloadb.be-md.ncbi.nlm.nih.gov/sos3/sra-pub-run-15/{run_acc}/{run_acc}.sra",
                            'ftp_download_url': f"ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByRun/sra/SRR/{run_acc[:6]}/{run_acc}/{run_acc}.sra",
                            'compression': False,
                            'public_url': f"https://www.ncbi.nlm.nih.gov/sra/{run_acc}"
                        })
            
            # If ENA didn't work, fall back to NCBI approach
            if not sra_files:
                # Search for runs in this project
                search_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
                search_params = {
                    'db': 'sra',
                    'term': f'{srp_id}[PRJA]',
                    'retmax': 50,  # Limit to avoid too many files
                    'retmode': 'xml'
                }
                
                search_response = requests.get(search_url, params=search_params, timeout=30)
                search_response.raise_for_status()
                
                # Parse search results
                search_root = ET.fromstring(search_response.content)
                id_list = search_root.find('.//IdList')
                
                if id_list is not None:
                    sra_ids = [id_elem.text for id_elem in id_list.findall('Id')]
                    
                    # Fetch detailed info to get SRR accessions
                    if sra_ids:
                        fetch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
                        fetch_params = {
                            'db': 'sra',
                            'id': ','.join(sra_ids[:20]),  # Limit for efficiency
                            'retmode': 'xml'
                        }
                        
                        fetch_response = requests.get(fetch_url, params=fetch_params, timeout=30)
                        fetch_response.raise_for_status()
                        
                        # Parse runs
                        root = ET.fromstring(fetch_response.content)
                        runs = root.findall('.//RUN')
                        
                        for run in runs:
                            run_acc = run.get('accession')
                            if run_acc and run_acc.startswith('SRR'):
                                # Get file size if available - try multiple possible locations
                                file_size = 0
                                size_elem = run.find('.//total_bases')
                                if size_elem is not None:
                                    try:
                                        file_size = int(size_elem.text)
                                    except (ValueError, TypeError):
                                        pass
                                
                                # If no size found, try total_spots * avg_length
                                if file_size == 0:
                                    spots_elem = run.find('.//total_spots')
                                    length_elem = run.find('.//avg_length')
                                    if spots_elem is not None and length_elem is not None:
                                        try:
                                            spots = int(spots_elem.text)
                                            length = int(length_elem.text)
                                            file_size = spots * length  # Rough estimate
                                        except (ValueError, TypeError):
                                            pass
                                
                                # Default to 100MB if no size found
                                if file_size == 0:
                                    file_size = 100 * 1024 * 1024  # 100MB default
                                
                                sra_files.append({
                                    'file_name': f"{run_acc}.sra",
                                    'file_size': file_size,
                                    'file_category': 'SRA',
                                    'file_type': 'SRA',
                                    'run_accession': run_acc,
                                    'download_url': f"https://sra-downloadb.be-md.ncbi.nlm.nih.gov/sos3/sra-pub-run-15/{run_acc}/{run_acc}.sra",
                                    'ftp_download_url': f"ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByRun/sra/SRR/{run_acc[:6]}/{run_acc}/{run_acc}.sra",
                                    'compression': False,
                                    'public_url': f"https://www.ncbi.nlm.nih.gov/sra/{run_acc}"
                                })
                            
        except Exception as e:
            # Continue even if SRA file fetching fails
            pass
            
        return sra_files

    def _get_ena_fastq_files(self, srp_id: str) -> List[Dict]:
        """Get equivalent FASTQ files from ENA."""
        ena_files = []
        
        try:
            # Search ENA for the same project
            # First try to find the project on ENA
            ena_search_url = "https://www.ebi.ac.uk/ena/portal/api/search"
            search_params = {
                'result': 'read_run',
                'query': f'secondary_study_accession="{srp_id}"',
                'format': 'json',
                'fields': 'run_accession,fastq_ftp,fastq_md5,fastq_bytes,base_count',
                'limit': 50
            }
            
            response = requests.get(ena_search_url, params=search_params, timeout=30)
            
            if response.status_code == 200:
                runs_data = response.json()
                
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
                                    
                                    ena_files.append({
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
                            
        except Exception as e:
            # Continue even if ENA search fails
            pass
            
        return ena_files

    def _categorize_sra_files(self, files: List[Dict]) -> Dict:
        """Categorize files by type."""
        categories = {
            'SRA': [],
            'FASTQ': [],
            'OTHER': []
        }
        
        for file_info in files:
            category = file_info.get('file_category', 'OTHER')
            if category in categories:
                categories[category].append(file_info['file_name'])
            else:
                categories['OTHER'].append(file_info['file_name'])
                
        return {k: len(v) for k, v in categories.items()} 