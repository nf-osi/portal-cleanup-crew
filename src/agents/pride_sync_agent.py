from crewai import Agent
from src.utils.llm_utils import get_llm
from src.tools.pride_tools import (
    PrideDatasetMetadataTool, 
    PrideDatasetFilesTool
)
from src.tools.synapse_tools import (
    SynapseFileUploadTool, 
    SynapseFolderCreationTool,
    SynapsePythonCodeExecutorTool,
    SynapseExternalFileLinkTool,
    SynapseBatchFolderCreationTool,
    SynapseBatchExternalFileLinkTool
)
import synapseclient


def get_pride_sync_agent(syn: synapseclient.Synapse):
    """
    Creates a PRIDE Sync Agent equipped with tools to fetch data from PRIDE repository
    and upload it to Synapse containers.
    
    Args:
        syn: Authenticated Synapse client instance
        
    Returns:
        Agent configured for PRIDE-to-Synapse data synchronization
    """
    return Agent(
        role='PRIDE Data Synchronization Specialist',
        goal=(
            'Retrieve proteomics datasets from the PRIDE repository and create '
            'external file links in target Synapse containers (projects or folders) '
            'with proper organization and folder structure.'
        ),
        backstory=(
            "You are an expert in proteomics data management and bioinformatics infrastructure. "
            "You specialize in working with the PRIDE (PRoteomics IDEntifications Database) repository, "
            "which is the world's largest data repository of mass spectrometry-based proteomics data. "
            "Your expertise includes:\n"
            "- Navigating the PRIDE Archive REST API to retrieve dataset metadata and files\n"
            "- Understanding proteomics data formats and file organization\n"
            "- Managing large-scale data transfers efficiently\n"
            "- Organizing data in Synapse with proper folder structures and annotations\n"
            "- Ensuring data integrity and traceability during transfers\n\n"
            "You are meticulous about creating logical folder structures and ensuring "
            "that file organization reflects the original PRIDE dataset structure. "
            "You focus on efficient file linking without downloading large datasets, "
            "preserving external URLs and file metadata for downstream processing."
        ),
        tools=[
            PrideDatasetMetadataTool(),
            PrideDatasetFilesTool(),
            SynapseFileUploadTool(syn=syn),
            SynapseFolderCreationTool(syn=syn),
            SynapseExternalFileLinkTool(syn=syn),
            SynapseBatchFolderCreationTool(syn=syn),
            SynapseBatchExternalFileLinkTool(syn=syn),
            SynapsePythonCodeExecutorTool(syn=syn)
        ],
        llm=get_llm("pride_sync_agent"),
        verbose=True,
        allow_delegation=False
    )


class PrideSyncAgent(Agent):
    """
    Alternative class-based implementation of the PRIDE Sync Agent.
    Use get_pride_sync_agent() function for most use cases.
    """
    
    def __init__(self, syn: synapseclient.Synapse):
        self.syn = syn
        super().__init__(
            role='PRIDE Data Synchronization Specialist',
            goal=(
                'Retrieve proteomics datasets from the PRIDE repository and create '
                'external file links in target Synapse containers (projects or folders) '
                'with proper organization and folder structure.'
            ),
            backstory=(
                "You are an expert in proteomics data management and bioinformatics infrastructure. "
                "You specialize in working with the PRIDE (PRoteomics IDEntifications Database) repository, "
                "which is the world's largest data repository of mass spectrometry-based proteomics data. "
                "Your expertise includes:\n"
                "- Navigating the PRIDE Archive REST API to retrieve dataset metadata and files\n"
                "- Understanding proteomics data formats and file organization\n"
                "- Managing large-scale data transfers efficiently\n"
                "- Organizing data in Synapse with proper folder structures and annotations\n"
                "- Ensuring data integrity and traceability during transfers\n\n"
                "You are meticulous about creating logical folder structures and ensuring "
                "that file organization reflects the original PRIDE dataset structure. "
                "You focus on efficient file linking without downloading large datasets, "
                "preserving external URLs and file metadata for downstream processing."
            ),
            tools=[
                PrideDatasetMetadataTool(),
                PrideDatasetFilesTool(),
                SynapseFileUploadTool(syn=syn),
                SynapseFolderCreationTool(syn=syn),
                SynapseExternalFileLinkTool(syn=syn),
                SynapseBatchFolderCreationTool(syn=syn),
                SynapseBatchExternalFileLinkTool(syn=syn),
                SynapsePythonCodeExecutorTool(syn=syn)
            ],
            llm=get_llm("pride_sync_agent"),
            verbose=True,
            allow_delegation=False
        ) 