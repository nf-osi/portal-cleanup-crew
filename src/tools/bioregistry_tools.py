"""
Bioregistry Tools for Biological Identifier Resolution and Metadata Retrieval.

These tools integrate with bioregistry.io and identifiers.org to resolve biological
identifiers and fetch contextual metadata for dataset annotation.
"""

import requests
import re
from typing import Dict, List, Optional, Tuple, Any
from crewai.tools import BaseTool
from pydantic import BaseModel, Field
import json
from urllib.parse import urlparse, urljoin
import time


class BioregistryResolverInput(BaseModel):
    """Input for resolving biological identifiers."""
    identifier: str = Field(description="Biological identifier to resolve (e.g., 'sra:SRR21492342', 'SRR21492342', 'insdc.sra:SRR21492342')")
    include_metadata: bool = Field(default=True, description="Whether to include additional metadata about the identifier")


class BioregistryResolverTool(BaseTool):
    name: str = "Bioregistry Identifier Resolver"
    description: str = (
        "Resolves biological identifiers using bioregistry.io and returns URLs, metadata, "
        "and contextual information. Supports various formats like 'sra:SRR123', 'insdc.sra:SRR123', "
        "or just 'SRR123' (will attempt to infer prefix)."
    )
    args_schema: type[BaseModel] = BioregistryResolverInput

    def _run(self, identifier: str, include_metadata: bool = True) -> Dict[str, Any]:
        """
        Resolves a biological identifier using bioregistry.io.
        
        Args:
            identifier: The identifier to resolve
            include_metadata: Whether to include additional metadata
            
        Returns:
            Dictionary with resolution results and metadata
        """
        try:
            # Clean and parse the identifier
            parsed_id = self._parse_identifier(identifier)
            if not parsed_id:
                return {"error": f"Could not parse identifier: {identifier}"}
            
            prefix, local_id = parsed_id
            
            # Try to resolve using bioregistry
            result = self._resolve_with_bioregistry(prefix, local_id, include_metadata)
            
            # If bioregistry fails, try identifiers.org as fallback
            if not result.get('resolved') and prefix and local_id:
                result.update(self._resolve_with_identifiers_org(prefix, local_id))
            
            return result
            
        except Exception as e:
            return {"error": f"Error resolving identifier: {str(e)}"}

    def _parse_identifier(self, identifier: str) -> Optional[Tuple[str, str]]:
        """Parse an identifier into prefix and local ID components."""
        # Remove whitespace
        identifier = identifier.strip()
        
        # Handle different formats
        if ':' in identifier:
            # Format: prefix:id or namespace.provider:id
            parts = identifier.split(':', 1)
            prefix = parts[0].lower()
            local_id = parts[1]
            
            # Handle compound prefixes like 'insdc.sra'
            if '.' in prefix:
                # Try to map compound prefixes to standard ones
                compound_mappings = {
                    'insdc.sra': 'insdc.sra',
                    'insdc.ena': 'ena',
                    'ncbi.sra': 'insdc.sra',
                    'ebi.ena': 'ena'
                }
                prefix = compound_mappings.get(prefix, prefix)
            
            return (prefix, local_id)
        else:
            # Try to infer prefix from the identifier pattern
            inferred_prefix = self._infer_prefix_from_pattern(identifier)
            if inferred_prefix:
                return (inferred_prefix, identifier)
        
        return None

    def _infer_prefix_from_pattern(self, identifier: str) -> Optional[str]:
        """Infer the prefix based on identifier patterns."""
        patterns = {
            r'^SRR\d+$': 'insdc.sra',
            r'^SRX\d+$': 'insdc.sra', 
            r'^SRP\d+$': 'insdc.sra',
            r'^SRS\d+$': 'insdc.sra',
            r'^SAMN\d+$': 'biosample',
            r'^SAME\d+$': 'biosample',
            r'^SAMD\d+$': 'biosample',
            r'^GSE\d+$': 'geo',
            r'^GSM\d+$': 'geo',
            r'^GPL\d+$': 'geo',
            r'^PXD\d+$': 'pride',
            r'^PRJ[A-Z]\w+$': 'bioproject',
            r'^ERP\d+$': 'ena.project',
            r'^ERR\d+$': 'ena.run',
            r'^ERX\d+$': 'ena.experiment',
            r'^ERS\d+$': 'ena.sample'
        }
        
        for pattern, prefix in patterns.items():
            if re.match(pattern, identifier, re.IGNORECASE):
                return prefix
        
        return None

    def _resolve_with_bioregistry(self, prefix: str, local_id: str, include_metadata: bool) -> Dict[str, Any]:
        """Resolve using bioregistry.io API."""
        result = {
            'prefix': prefix,
            'local_id': local_id,
            'resolver': 'bioregistry',
            'resolved': False
        }
        
        try:
            # Get prefix metadata from bioregistry
            prefix_url = f"https://bioregistry.io/api/registry/{prefix}"
            prefix_response = requests.get(prefix_url, timeout=10)
            
            if prefix_response.status_code == 200:
                prefix_data = prefix_response.json()
                result['prefix_metadata'] = prefix_data
                result['resolved'] = True
                
                # Construct URLs
                curie = f"{prefix}:{local_id}"
                result['curie'] = curie
                result['bioregistry_url'] = f"https://bioregistry.io/{curie}"
                
                # Get provider URLs if available
                if 'uri_format' in prefix_data:
                    try:
                        provider_url = prefix_data['uri_format'].replace('$1', local_id)
                        result['provider_url'] = provider_url
                    except:
                        pass
                
                if include_metadata:
                    # Try to get additional metadata about the specific identifier
                    ref_url = f"https://bioregistry.io/api/reference/{curie}"
                    ref_response = requests.get(ref_url, timeout=10)
                    if ref_response.status_code == 200:
                        result['reference_metadata'] = ref_response.json()
            
        except requests.RequestException as e:
            result['bioregistry_error'] = str(e)
        
        return result

    def _resolve_with_identifiers_org(self, prefix: str, local_id: str) -> Dict[str, Any]:
        """Fallback resolution using identifiers.org."""
        result = {'identifiers_org_attempted': True}
        
        try:
            # Try identifiers.org resolution API
            curie = f"{prefix}:{local_id}"
            resolver_url = f"https://resolver.api.identifiers.org/{curie}"
            
            response = requests.get(resolver_url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                result['identifiers_org_data'] = data
                result['identifiers_org_url'] = f"https://identifiers.org/{curie}"
                
                # Extract provider URLs
                if 'payload' in data and 'resolvedResources' in data['payload']:
                    providers = data['payload']['resolvedResources']
                    if providers:
                        result['provider_urls'] = [r.get('compactIdentifierResolvedUrl') for r in providers if r.get('compactIdentifierResolvedUrl')]
                        result['primary_provider_url'] = providers[0].get('compactIdentifierResolvedUrl')
                
                result['resolved'] = True
                
        except requests.RequestException as e:
            result['identifiers_org_error'] = str(e)
        
        return result


class BioregistryMetadataFetcherInput(BaseModel):
    """Input for fetching metadata from resolved URLs."""
    url: str = Field(description="URL to fetch metadata from")
    parse_format: str = Field(default="auto", description="Format to parse (auto, json, xml, html)")


class BioregistryMetadataFetcherTool(BaseTool):
    name: str = "Bioregistry Metadata Fetcher"
    description: str = (
        "Fetches and parses metadata from URLs resolved by bioregistry. Can parse JSON, XML, "
        "and HTML content to extract structured information about biological entities."
    )
    args_schema: type[BaseModel] = BioregistryMetadataFetcherInput

    def _run(self, url: str, parse_format: str = "auto") -> Dict[str, Any]:
        """
        Fetches metadata from a URL and attempts to parse it.
        
        Args:
            url: URL to fetch
            parse_format: Format to parse (auto, json, xml, html)
            
        Returns:
            Dictionary with fetched metadata
        """
        try:
            # Set headers to appear as a browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; BioregistryBot/1.0)',
                'Accept': 'application/json, application/xml, text/html, text/plain, */*'
            }
            
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            content_type = response.headers.get('content-type', '').lower()
            content = response.text
            
            result = {
                'url': url,
                'status_code': response.status_code,
                'content_type': content_type,
                'content_length': len(content)
            }
            
            # Parse based on content type or format
            if parse_format == "auto":
                if 'json' in content_type:
                    parse_format = "json"
                elif 'xml' in content_type:
                    parse_format = "xml"
                else:
                    parse_format = "html"
            
            if parse_format == "json":
                try:
                    result['parsed_data'] = json.loads(content)
                    result['format'] = 'json'
                except json.JSONDecodeError:
                    result['parse_error'] = 'Failed to parse as JSON'
                    
            elif parse_format == "xml":
                result['format'] = 'xml'
                result['xml_content'] = content[:5000]  # Truncate for safety
                # Could add XML parsing here if needed
                
            elif parse_format == "html":
                result['format'] = 'html'
                # Extract useful information from HTML
                result['extracted_metadata'] = self._extract_html_metadata(content)
            
            return result
            
        except requests.RequestException as e:
            return {'error': f"Failed to fetch URL: {str(e)}", 'url': url}
        except Exception as e:
            return {'error': f"Error processing metadata: {str(e)}", 'url': url}

    def _extract_html_metadata(self, html_content: str) -> Dict[str, Any]:
        """Extract useful metadata from HTML content."""
        metadata = {}
        
        # Extract title
        title_match = re.search(r'<title[^>]*>(.*?)</title>', html_content, re.IGNORECASE | re.DOTALL)
        if title_match:
            metadata['title'] = title_match.group(1).strip()
        
        # Extract meta tags
        meta_matches = re.findall(r'<meta\s+([^>]+)>', html_content, re.IGNORECASE)
        meta_tags = {}
        for meta in meta_matches:
            # Parse attributes
            attrs = {}
            attr_matches = re.findall(r'(\w+)=["\']([^"\']*)["\']', meta)
            for name, value in attr_matches:
                attrs[name.lower()] = value
            
            if 'name' in attrs and 'content' in attrs:
                meta_tags[attrs['name']] = attrs['content']
            elif 'property' in attrs and 'content' in attrs:
                meta_tags[attrs['property']] = attrs['content']
        
        if meta_tags:
            metadata['meta_tags'] = meta_tags
        
        return metadata


class IdentifierExtractorInput(BaseModel):
    """Input for extracting identifiers from text."""
    text: str = Field(description="Text to extract identifiers from")
    identifier_types: List[str] = Field(default=[], description="Specific identifier types to look for (empty = all types)")


class IdentifierExtractorTool(BaseTool):
    name: str = "Biological Identifier Extractor"
    description: str = (
        "Extracts biological identifiers from text, file names, descriptions, etc. "
        "Recognizes common patterns for SRA, BioSample, GEO, PRIDE, and other repositories."
    )
    args_schema: type[BaseModel] = IdentifierExtractorInput

    def _run(self, text: str, identifier_types: List[str] = None) -> Dict[str, List[str]]:
        """
        Extracts biological identifiers from text.
        
        Args:
            text: Text to search for identifiers
            identifier_types: Specific types to look for (None = all types)
            
        Returns:
            Dictionary mapping identifier types to lists of found identifiers
        """
        if identifier_types is None:
            identifier_types = []
        
        # Define patterns for different identifier types (flexible boundaries for real-world file names)
        patterns = {
            'sra_run': r'(SRR\d+)',
            'sra_experiment': r'(SRX\d+)', 
            'sra_project': r'(SRP\d+)',
            'sra_sample': r'(SRS\d+)',
            'biosample': r'(SAM[NED]\d+)',
            'bioproject': r'(PRJ[A-Z]\w+)',
            'geo_series': r'(GSE\d+)',
            'geo_sample': r'(GSM\d+)',
            'geo_platform': r'(GPL\d+)',
            'pride': r'(PXD\d+)',
            'ena_project': r'(ERP\d+)',
            'ena_run': r'(ERR\d+)',
            'ena_experiment': r'(ERX\d+)',
            'ena_sample': r'(ERS\d+)',
            'doi': r'(?:doi:)?(10\.\d+/[^\s]+)',
            'pmid': r'(?:PMID:?)(\d+)',
            'pmcid': r'(PMC\d+)'
        }
        
        # Filter patterns if specific types requested
        if identifier_types:
            patterns = {k: v for k, v in patterns.items() if k in identifier_types}
        
        # Extract identifiers
        found_identifiers = {}
        
        for id_type, pattern in patterns.items():
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                # Remove duplicates while preserving order
                unique_matches = []
                seen = set()
                for match in matches:
                    match_upper = match.upper()
                    if match_upper not in seen:
                        seen.add(match_upper)
                        unique_matches.append(match_upper)
                
                found_identifiers[id_type] = unique_matches
        
        # Add summary
        total_found = sum(len(ids) for ids in found_identifiers.values())
        
        return {
            'found_identifiers': found_identifiers,
            'total_found': total_found,
            'text_length': len(text),
            'searched_patterns': list(patterns.keys())
        }


class RelatedIdentifierFinderInput(BaseModel):
    """Input for finding related identifiers."""
    identifier: str = Field(description="Source identifier to find related identifiers for")
    relation_types: List[str] = Field(default=[], description="Types of relations to find (e.g., 'sample', 'project', 'experiment')")


class RelatedIdentifierFinderTool(BaseTool):
    name: str = "Related Identifier Finder"
    description: str = (
        "Finds related biological identifiers by querying NCBI APIs and other sources. "
        "For example, finds BioSample (SAMN) identifiers related to SRA runs (SRR), "
        "or finds all runs in a project."
    )
    args_schema: type[BaseModel] = RelatedIdentifierFinderInput

    def _run(self, identifier: str, relation_types: List[str] = None) -> Dict[str, Any]:
        """
        Finds related identifiers using various APIs.
        
        Args:
            identifier: Source identifier
            relation_types: Types of relations to find
            
        Returns:
            Dictionary with related identifiers and metadata
        """
        if relation_types is None:
            relation_types = []
        
        try:
            # Parse the identifier
            parsed = self._parse_identifier(identifier)
            if not parsed:
                return {'error': f'Could not parse identifier: {identifier}'}
            
            id_type, id_value = parsed
            
            result = {
                'source_identifier': identifier,
                'source_type': id_type,
                'related_identifiers': {},
                'metadata': {}
            }
            
            # Find related identifiers based on source type
            if id_type == 'sra_run':
                result.update(self._find_sra_related(id_value, relation_types))
            elif id_type == 'biosample':
                result.update(self._find_biosample_related(id_value, relation_types))
            elif id_type == 'bioproject':
                result.update(self._find_bioproject_related(id_value, relation_types))
            
            return result
            
        except Exception as e:
            return {'error': f'Error finding related identifiers: {str(e)}'}

    def _parse_identifier(self, identifier: str) -> Optional[Tuple[str, str]]:
        """Parse identifier into type and value."""
        identifier = identifier.strip().upper()
        
        if identifier.startswith('SRR'):
            return ('sra_run', identifier)
        elif identifier.startswith('SRX'):
            return ('sra_experiment', identifier)
        elif identifier.startswith('SRP'):
            return ('sra_project', identifier)
        elif identifier.startswith('SAMN'):
            return ('biosample', identifier)
        elif identifier.startswith('PRJ'):
            return ('bioproject', identifier)
        
        return None

    def _find_sra_related(self, srr_id: str, relation_types: List[str]) -> Dict[str, Any]:
        """Find identifiers related to an SRA run."""
        related = {}
        metadata = {}
        
        try:
            # Use NCBI E-utilities to get SRA metadata
            base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
            
            # Search for the SRA run
            search_url = f"{base_url}esearch.fcgi"
            search_params = {
                'db': 'sra',
                'term': srr_id,
                'retmode': 'json'
            }
            
            search_response = requests.get(search_url, params=search_params, timeout=10)
            if search_response.status_code == 200:
                search_data = search_response.json()
                
                if 'esearchresult' in search_data and search_data['esearchresult'].get('idlist'):
                    uid = search_data['esearchresult']['idlist'][0]
                    
                    # Get detailed information
                    summary_url = f"{base_url}esummary.fcgi"
                    summary_params = {
                        'db': 'sra',
                        'id': uid,
                        'retmode': 'json'
                    }
                    
                    summary_response = requests.get(summary_url, params=summary_params, timeout=10)
                    if summary_response.status_code == 200:
                        summary_data = summary_response.json()
                        
                        if 'result' in summary_data and uid in summary_data['result']:
                            run_info = summary_data['result'][uid]
                            metadata['sra_metadata'] = run_info
                            
                            # Extract related identifiers from the metadata
                            expxml = run_info.get('expxml', '')
                            runs = run_info.get('runs', '')
                            
                            # Look for BioSample IDs
                            biosample_matches = re.findall(r'(SAMN\d+)', expxml + runs)
                            if biosample_matches:
                                related['biosample'] = list(set(biosample_matches))
                            
                            # Look for BioProject IDs  
                            bioproject_matches = re.findall(r'(PRJ[A-Z]\w+)', expxml + runs)
                            if bioproject_matches:
                                related['bioproject'] = list(set(bioproject_matches))
                            
                            # Look for study/project IDs
                            study_matches = re.findall(r'(SRP\d+)', expxml + runs)
                            if study_matches:
                                related['sra_project'] = list(set(study_matches))
                            
                            # Look for experiment IDs
                            exp_matches = re.findall(r'(SRX\d+)', expxml + runs)
                            if exp_matches:
                                related['sra_experiment'] = list(set(exp_matches))
                
        except requests.RequestException as e:
            metadata['ncbi_error'] = str(e)
        
        return {'related_identifiers': related, 'metadata': metadata}

    def _find_biosample_related(self, biosample_id: str, relation_types: List[str]) -> Dict[str, Any]:
        """Find identifiers related to a BioSample."""
        # This could be implemented to find SRA runs associated with a BioSample
        return {'related_identifiers': {}, 'metadata': {'note': 'BioSample relations not yet implemented'}}

    def _find_bioproject_related(self, bioproject_id: str, relation_types: List[str]) -> Dict[str, Any]:
        """Find identifiers related to a BioProject."""
        # This could be implemented to find all SRA/BioSample records in a project
        return {'related_identifiers': {}, 'metadata': {'note': 'BioProject relations not yet implemented'}}


# Export all tools
__all__ = [
    'BioregistryResolverTool',
    'BioregistryMetadataFetcherTool', 
    'IdentifierExtractorTool',
    'RelatedIdentifierFinderTool'
] 