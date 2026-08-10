import GEOparse
from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from typing import Type, List, Dict, Optional
import pandas as pd
import tempfile
import os
import requests
import re
import xml.etree.ElementTree as ET


class GeoMetadataFetcherInput(BaseModel):
    """Input for fetching GEO metadata."""
    gse_id: str = Field(description="The GEO Series accession ID (e.g., GSE123456)")


class GeoMetadataFetcherTool(BaseTool):
    name: str = "GEO Metadata Fetcher"
    description: str = (
        "Fetches and parses experiment and sample metadata from a GEO Accession ID (GSE). "
        "Returns a dictionary containing experiment metadata and a pandas DataFrame of sample metadata."
    )
    args_schema: Type[BaseModel] = GeoMetadataFetcherInput

    def _run(self, gse_id: str) -> dict:
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                gse = GEOparse.get_GEO(geo=gse_id, destdir=tmpdir, silent=True)
                
                experiment_metadata = gse.metadata
                sample_metadata_df = pd.DataFrame()

                # Combine metadata from all GSM samples into a single DataFrame
                all_gsm_metadata = []
                for gsm_name, gsm in gse.gsms.items():
                    gsm_metadata = gsm.metadata
                    gsm_metadata['gsm'] = gsm_name
                    all_gsm_metadata.append(gsm_metadata)
                
                if all_gsm_metadata:
                    sample_metadata_df = pd.DataFrame(all_gsm_metadata)

                return {
                    "experiment_metadata": experiment_metadata,
                    "sample_metadata": sample_metadata_df
                }

        except Exception as e:
            return {"error": f"An error occurred while fetching GEO data: {str(e)}"} 


class GeoDatasetFilesInput(BaseModel):
    """Input for fetching GEO dataset file listings."""
    gse_id: str = Field(description="The GEO Series accession ID (e.g., GSE123456)")


class GeoDatasetFilesTool(BaseTool):
    name: str = "GEO Dataset Files Fetcher"
    description: str = (
        "Fetches the list of files available for a GEO dataset including supplementary files, "
        "raw data links, and processed data files. Returns information about each file "
        "including name, size, type, and download URLs."
    )
    args_schema: Type[BaseModel] = GeoDatasetFilesInput

    def _run(self, gse_id: str) -> dict:
        """
        Fetches file information for a GEO dataset.
        
        Args:
            gse_id: GEO Series ID (e.g., GSE123456)
            
        Returns:
            Dictionary containing file information and summary
        """
        try:
            files = []
            
            # Method 1: Get supplementary files from GEO FTP
            supp_files = self._get_supplementary_files(gse_id)
            files.extend(supp_files)
            
            # Method 2: Get sample data files 
            sample_files = self._get_sample_files(gse_id)
            files.extend(sample_files)
            
            # Categorize files
            file_categories = self._categorize_geo_files(files)
            
            return {
                "gse_id": gse_id,
                "total_files": len(files),
                "files": files,
                "file_categories": file_categories,
                "summary": f"Found {len(files)} files for GEO dataset {gse_id}"
            }
            
        except Exception as e:
            return {
                "gse_id": gse_id,
                "error": f"Failed to fetch GEO files: {str(e)}",
                "files": []
            }

    def _get_supplementary_files(self, gse_id: str) -> List[Dict]:
        """Get supplementary files from GEO FTP server."""
        supp_files = []
        
        try:
            # GEO FTP URL pattern for supplementary files
            base_ftp = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{gse_id[:-3]}nnn/{gse_id}/suppl/"
            
            # Try to get directory listing
            try:
                response = requests.get(base_ftp, timeout=10)
                if response.status_code == 200:
                    # Parse HTML directory listing for file links
                    file_links = re.findall(r'href="([^"]+\.[a-zA-Z0-9]+)"', response.text)
                    
                    for file_link in file_links:
                        if not file_link.startswith('?'): # Skip parent directory links
                            file_url = base_ftp + file_link
                            
                            # Try to get file size
                            file_size = self._get_file_size(file_url)
                            
                            supp_files.append({
                                'file_name': file_link,
                                'file_size': file_size,
                                'file_category': 'SUPPLEMENTARY',
                                'file_type': self._determine_file_type(file_link),
                                'download_url': file_url,
                                'ftp_download_url': file_url.replace('https://', 'ftp://'),
                                'compression': file_link.endswith(('.gz', '.bz2', '.zip')),
                                'public_url': file_url
                            })
            except requests.RequestException:
                # If HTTP fails, we still continue - some datasets might not have supp files
                pass
                
        except Exception:
            # Continue even if supplementary file fetching fails
            pass
            
        return supp_files

    def _get_sample_files(self, gse_id: str) -> List[Dict]:
        """Get sample-level data files using GEOparse."""
        sample_files = []
        
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                gse = GEOparse.get_GEO(geo=gse_id, destdir=tmpdir, silent=True)
                
                # Check for platform data files
                for gpl_name, gpl in gse.gpls.items():
                    if hasattr(gpl, 'table') and gpl.table is not None:
                        # This is platform annotation data
                        sample_files.append({
                            'file_name': f"{gpl_name}_annotation.txt",
                            'file_size': len(str(gpl.table)) if gpl.table is not None else 0,
                            'file_category': 'PLATFORM',
                            'file_type': 'ANNOTATION',
                            'download_url': f"https://ftp.ncbi.nlm.nih.gov/geo/platforms/{gpl_name[:-3]}/{gpl_name}/annot/{gpl_name}.annot.gz",
                            'ftp_download_url': f"ftp://ftp.ncbi.nlm.nih.gov/geo/platforms/{gpl_name[:-3]}/{gpl_name}/annot/{gpl_name}.annot.gz",
                            'compression': True,
                            'public_url': f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={gpl_name}"
                        })
                
                # Check for series matrix files
                if hasattr(gse, 'table') and gse.table is not None:
                    sample_files.append({
                        'file_name': f"{gse_id}_series_matrix.txt",
                        'file_size': len(str(gse.table)) if gse.table is not None else 0,
                        'file_category': 'PROCESSED',
                        'file_type': 'MATRIX',
                        'download_url': f"https://ftp.ncbi.nlm.nih.gov/geo/series/{gse_id[:-3]}nnn/{gse_id}/matrix/{gse_id}_series_matrix.txt.gz",
                        'ftp_download_url': f"ftp://ftp.ncbi.nlm.nih.gov/geo/series/{gse_id[:-3]}nnn/{gse_id}/matrix/{gse_id}_series_matrix.txt.gz",
                        'compression': True,
                        'public_url': f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={gse_id}"
                    })
                    
        except Exception:
            # Continue even if sample file fetching fails
            pass
            
        return sample_files

    def _get_file_size(self, url: str) -> int:
        """Get file size from HTTP headers."""
        try:
            response = requests.head(url, timeout=5)
            if response.status_code == 200:
                content_length = response.headers.get('content-length')
                if content_length:
                    return int(content_length)
        except:
            pass
        return 0

    def _determine_file_type(self, filename: str) -> str:
        """Determine file type based on extension."""
        filename_lower = filename.lower()
        
        if filename_lower.endswith(('.txt', '.tsv', '.csv')):
            return 'TEXT'
        elif filename_lower.endswith(('.xlsx', '.xls')):
            return 'SPREADSHEET'
        elif filename_lower.endswith(('.tar', '.tar.gz', '.zip', '.gz')):
            return 'ARCHIVE'
        elif filename_lower.endswith(('.pdf')):
            return 'DOCUMENT'
        elif filename_lower.endswith(('.cel', '.cdf')):
            return 'MICROARRAY'
        elif filename_lower.endswith(('.fastq', '.fq', '.sra')):
            return 'SEQUENCING'
        else:
            return 'OTHER'

    def _categorize_geo_files(self, files: List[Dict]) -> Dict:
        """Categorize files by type."""
        categories = {
            'SUPPLEMENTARY': [],
            'PROCESSED': [],
            'PLATFORM': [],
            'RAW': []
        }
        
        for file_info in files:
            category = file_info.get('file_category', 'OTHER')
            if category in categories:
                categories[category].append(file_info['file_name'])
            else:
                categories['RAW'].append(file_info['file_name'])
                
        return {k: len(v) for k, v in categories.items()}


class SrrToGeoMetadataInput(BaseModel):
    """Input for fetching GEO metadata from an SRR ID."""
    srr_id: str = Field(description="The SRA Run accession ID (e.g., SRR123456)")


class SrrToGeoMetadataTool(BaseTool):
    name: str = "SRR to GEO Metadata Fetcher"
    description: str = (
        "Fetches GEO metadata for a given SRA Run (SRR) ID by finding the "
        "corresponding GEO Sample (GSM) and retrieving its metadata."
    )
    args_schema: Type[BaseModel] = SrrToGeoMetadataInput

    def _run(self, srr_id: str) -> dict:
        try:
            # Step 1: Use E-utilities to find the GSM for the SRR
            gsm_id = self._find_gsm_for_srr(srr_id)
            if not gsm_id:
                return {"error": f"Could not find a corresponding GEO Sample (GSM) for SRR ID {srr_id}."}

            # Step 2: Use GEOparse to fetch metadata for the GSM
            with tempfile.TemporaryDirectory() as tmpdir:
                gsm = GEOparse.get_GEO(geo=gsm_id, destdir=tmpdir, silent=True)
                if not gsm:
                    return {"error": f"Failed to fetch metadata for GSM ID {gsm_id}."}
                
                return {
                    "gsm_id": gsm_id,
                    "metadata": gsm.metadata
                }

        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"}

    def _find_gsm_for_srr(self, srr_id: str) -> Optional[str]:
        """Finds the GEO Sample ID (GSM) for a given SRA Run ID (SRR)."""
        try:
            # Use esearch to find the SRA record UID for the SRR ID
            esearch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
            esearch_params = {
                'db': 'sra',
                'term': srr_id,
                'retmode': 'json'
            }
            search_response = requests.get(esearch_url, params=esearch_params, timeout=30)
            search_response.raise_for_status()
            search_data = search_response.json()
            
            sra_uid = search_data.get('esearchresult', {}).get('idlist', [])
            if not sra_uid:
                return None

            # Use efetch to get the SRA experiment XML
            efetch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
            efetch_params = {
                'db': 'sra',
                'id': sra_uid[0],
                'retmode': 'xml'
            }
            fetch_response = requests.get(efetch_url, params=efetch_params, timeout=30)
            fetch_response.raise_for_status()
            
            # Parse XML to find the GSM ID
            root = ET.fromstring(fetch_response.content)
            # The structure is EXPERIMENT_PACKAGE -> EXPERIMENT -> IDENTIFIERS -> EXTERNAL_ID with namespace="GEO"
            gsm_element = root.find(".//EXPERIMENT/IDENTIFIERS/EXTERNAL_ID[@namespace='GEO']")
            if gsm_element is not None:
                return gsm_element.text

            # Fallback: check other locations
            # Check SAMPLE
            gsm_element = root.find(".//SAMPLE/IDENTIFIERS/EXTERNAL_ID[@namespace='GEO']")
            if gsm_element is not None:
                return gsm_element.text
            
            # Check RUN
            gsm_element = root.find(".//RUN/IDENTIFIERS/EXTERNAL_ID[@namespace='GEO']")
            if gsm_element is not None:
                return gsm_element.text

            return None
        except requests.RequestException as e:
            return None
        except ET.ParseError as e:
            return None
        except Exception as e:
            return None 