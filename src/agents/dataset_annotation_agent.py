from crewai import Agent
from src.utils.llm_utils import get_llm
from src.tools.synapse_analysis_tools import SynapseFolderAnalysisTool
from src.tools.jsonld_tools import JsonLdGetValidValuesTool, JsonLdGetManifestsTool, JsonLdAttributeDisplayNamesTool, CommonRNASeqDisplayNamesTool
from src.tools.synapse_tools import SynapsePythonCodeExecutorTool, SynapseBatchAnnotationTool, SynapseLargeBatchAnnotationTool, SynapseFolderAnnotationTool
from src.tools.bioregistry_tools import (
    BioregistryResolverTool,
    BioregistryMetadataFetcherTool,
    IdentifierExtractorTool,
    RelatedIdentifierFinderTool
)
import synapseclient


def get_dataset_annotation_agent(syn: synapseclient.Synapse):
    """
    Creates a simplified Dataset Annotation Agent that analyzes Synapse datasets
    and applies appropriate annotations using a streamlined workflow.
    
    Args:
        syn: Authenticated Synapse client instance
        
    Returns:
        Agent configured for simplified dataset analysis and annotation
    """
    return Agent(
        role='Dataset Annotation Specialist',
        goal=(
            'Analyze a Synapse dataset, identify the appropriate metadata template, '
            'and apply consistent annotations to all data files based on the JSON-LD schema.'
        ),
        backstory=(
            "You are an expert in scientific data curation who specializes in analyzing "
            "datasets and applying appropriate metadata annotations. You work efficiently "
            "by taking a direct approach:\n\n"
            "1. First, analyze the dataset structure to understand what types of files exist\n"
            "2. Determine the best processing approach based on structure:\n"
            "   - For simple structures: process all files together\n"
            "   - For complex structures (many folders/SRR IDs): process folder by folder\n"
            "3. For each folder/group (process ONE folder at a time, never batch):\n"
            "   - Extract biological identifiers (e.g., SRR from folder name)\n"
            "   - Use bioregistry tools to get actual metadata for that specific identifier\n"
            "   - Determine appropriate template based on the specific metadata\n"
            "   - Build annotations using displayName from schema (e.g., 'readPair' not 'ReadPair')\n"
            "   - Apply annotations to that ONE folder using 'annotate_folder' tool\n"
            "   - Move to next folder and repeat\n"
            "4. CRITICAL: Process one folder at a time, never try to batch multiple folders.\n"
            "   - Try JSON-LD Get Attribute Display Names Tool first\n"
            "   - If that fails, use Get Common RNA-seq Display Names Tool as fallback\n"
            "   - Build annotations with correct displayNames (readPair, fileFormat, etc.)\n"
            "   - ALWAYS use 'annotate_folder' tool to actually apply annotations - don't just plan!\n"
            "   - Move to next folder immediately after successful annotation\n\n"
            "You focus on getting the job done correctly and efficiently rather than "
            "creating complex intermediate files or workflows. You use bioregistry tools "
            "to enrich annotations with contextual information from external databases."
        ),
        tools=[
            SynapseFolderAnalysisTool(syn=syn),
            JsonLdGetValidValuesTool(),
            JsonLdGetManifestsTool(),
            JsonLdAttributeDisplayNamesTool(),
            CommonRNASeqDisplayNamesTool(),
            SynapsePythonCodeExecutorTool(syn=syn),
            SynapseBatchAnnotationTool(syn=syn),
            SynapseLargeBatchAnnotationTool(syn=syn),
            SynapseFolderAnnotationTool(syn=syn),
            BioregistryResolverTool(),
            BioregistryMetadataFetcherTool(),
            IdentifierExtractorTool(),
            RelatedIdentifierFinderTool()
        ],
        llm=get_llm("dataset_annotation_agent"),
        verbose=True,
        allow_delegation=False,
        max_iter=25  # Reduced from 100 to keep it simple
    )


class DatasetAnnotationAgent(Agent):
    """
    Simplified class-based implementation of the Dataset Annotation Agent.
    Use get_dataset_annotation_agent() function for most use cases.
    """
    
    def __init__(self, syn: synapseclient.Synapse):
        self.syn = syn
        super().__init__(
            role='Dataset Annotation Specialist',
            goal=(
                'Analyze a Synapse dataset, identify the appropriate metadata template, '
                'and apply consistent annotations to all data files based on the JSON-LD schema.'
            ),
            backstory=(
                "You are an expert in scientific data curation who specializes in analyzing "
                "datasets and applying appropriate metadata annotations. You work efficiently "
                "by taking a direct approach:\n\n"
                "1. First, analyze the dataset structure to understand what types of files exist\n"
                "2. Extract biological identifiers from file names and metadata to get contextual information\n"
                "3. Use bioregistry to resolve identifiers and find related metadata (e.g., SRR→SAMN)\n"
                "4. Determine what template best fits the data based on file types and metadata\n"
                "5. Apply consistent annotations directly to all data files using the Synapse API\n\n"
                "You focus on getting the job done correctly and efficiently rather than "
                "creating complex intermediate files or workflows. You use bioregistry tools "
                "to enrich annotations with contextual information from external databases."
            ),
            tools=[
                SynapseFolderAnalysisTool(syn=syn),
                JsonLdGetValidValuesTool(),
                JsonLdGetManifestsTool(),
                SynapsePythonCodeExecutorTool(syn=syn),
                SynapseBatchAnnotationTool(syn=syn),
                BioregistryResolverTool(),
                BioregistryMetadataFetcherTool(),
                IdentifierExtractorTool(),
                RelatedIdentifierFinderTool()
            ],
            llm=get_llm("dataset_annotation_agent"),
            verbose=True,
            allow_delegation=False,
            max_iter=25  # Reduced from 100 to keep it simple
        ) 