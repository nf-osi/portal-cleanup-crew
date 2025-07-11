from crewai import Agent
from crewai_tools import CodeInterpreterTool
from src.utils.llm_utils import get_llm
from src.tools.pride_tools import (
    PrideDatasetMetadataTool, 
    PrideDatasetFilesTool
)
from src.tools.geo_tools import GeoMetadataFetcherTool, GeoDatasetFilesTool
from src.tools.sra_tools import SraDatasetMetadataTool, SraDatasetFilesTool
from src.tools.ena_tools import EnaDatasetMetadataTool, EnaFastqFilesTool
from src.tools.synapse_tools import (
    SynapseFileUploadTool, 
    SynapseFolderCreationTool,
    SynapsePythonCodeExecutorTool,
    SynapseExternalFileLinkTool,
    SynapseBatchFolderCreationTool,
    SynapseBatchExternalFileLinkTool
)
import synapseclient


def get_link_external_data_agent(syn: synapseclient.Synapse):
    """
    Creates a Link External Data Agent equipped with tools to fetch data from various
    external repositories and create file links in Synapse containers.
    
    Args:
        syn: Authenticated Synapse client instance
        
    Returns:
        Agent configured for external data repository linking to Synapse
    """
    return Agent(
        role='External Data Repository Linking Specialist',
        goal=(
            'Identify external data repository types from dataset identifiers and create '
            'external file links in target Synapse containers with proper organization '
            'and folder structure. Support multiple repositories including PRIDE, GEO, SRA, '
            'and provide fallback capabilities for unknown repository types.'
        ),
        backstory=(
            "You are an expert in scientific data repositories and data integration. "
            "You specialize in working with multiple external data repositories including "
            "PRIDE (proteomics), GEO (genomics), SRA (sequencing), and others. Your expertise includes:\n"
            "- Recognizing dataset identifier patterns (PXD for PRIDE, GSE for GEO, SRP for SRA, etc.)\n"
            "- Understanding different data repository APIs and structures\n"
            "- Creating efficient external file links without downloading large datasets\n"
            "- Organizing data in logical folder structures that reflect repository organization\n"
            "- Handling various file formats and metadata standards across repositories\n"
            "- Providing fallback solutions for unrecognized or new repository types\n"
            "- Using advanced Python execution capabilities for custom integrations with unknown repositories\n\n"
            "You are intelligent about identifier recognition and can determine the appropriate "
            "tools and workflows to use based on the dataset identifier provided. You ALWAYS prefer "
            "ENA (European Nucleotide Archive) over SRA when both are available, as ENA provides "
            "superior direct access to raw FASTQ data. For unknown repository types (like Zenodo DOIs, "
            "institutional repositories, EBI Metagenomics, etc.), you can use the Code Interpreter Tool "
            "to write and execute custom Python code with external HTTP requests to fetch metadata and "
            "file information. When dealing with sequencing data, you prioritize finding ENA accessions "
            "over SRA accessions. You focus on efficient file linking, raw data preservation, and "
            "maintaining clean organizational structures in Synapse."
        ),
        tools=[
            PrideDatasetMetadataTool(),
            PrideDatasetFilesTool(),
            GeoMetadataFetcherTool(),
            GeoDatasetFilesTool(),
            SraDatasetMetadataTool(),
            SraDatasetFilesTool(),
            EnaDatasetMetadataTool(),
            EnaFastqFilesTool(),
            SynapseFileUploadTool(syn=syn),
            SynapseFolderCreationTool(syn=syn),
            SynapseExternalFileLinkTool(syn=syn),
            SynapseBatchFolderCreationTool(syn=syn),
            SynapseBatchExternalFileLinkTool(syn=syn),
            SynapsePythonCodeExecutorTool(syn=syn),
            CodeInterpreterTool()
        ],
        llm=get_llm(),
        verbose=True,
        allow_delegation=False
    )


class LinkExternalDataAgent(Agent):
    """
    Alternative class-based implementation of the Link External Data Agent.
    Use get_link_external_data_agent() function for most use cases.
    """
    
    def __init__(self, syn: synapseclient.Synapse):
        self.syn = syn
        super().__init__(
            role='External Data Repository Linking Specialist',
            goal=(
                'Identify external data repository types from dataset identifiers and create '
                'external file links in target Synapse containers with proper organization '
                'and folder structure. Support multiple repositories including PRIDE, GEO, SRA, '
                'and provide fallback capabilities for unknown repository types.'
            ),
            backstory=(
                "You are an expert in scientific data repositories and data integration. "
                "You specialize in working with multiple external data repositories including "
                "PRIDE (proteomics), GEO (genomics), SRA (sequencing), and others. Your expertise includes:\n"
                "- Recognizing dataset identifier patterns (PXD for PRIDE, GSE for GEO, SRP for SRA, etc.)\n"
                "- Understanding different data repository APIs and structures\n"
                "- Creating efficient external file links without downloading large datasets\n"
                "- Organizing data in logical folder structures that reflect repository organization\n"
                "- Handling various file formats and metadata standards across repositories\n"
                "- Providing fallback solutions for unrecognized or new repository types\n"
                "- Using advanced Python execution capabilities for custom integrations with unknown repositories\n\n"
                "You are intelligent about identifier recognition and can determine the appropriate "
                "tools and workflows to use based on the dataset identifier provided. You ALWAYS prefer "
                "ENA (European Nucleotide Archive) over SRA when both are available, as ENA provides "
                "superior direct access to raw FASTQ data. For unknown repository types (like Zenodo DOIs, "
                "institutional repositories, EBI Metagenomics, etc.), you can use the Code Interpreter Tool "
                "to write and execute custom Python code with external HTTP requests to fetch metadata and "
                "file information. When dealing with sequencing data, you prioritize finding ENA accessions "
                "over SRA accessions. You focus on efficient file linking, raw data preservation, and "
                "maintaining clean organizational structures in Synapse."
            ),
            tools=[
                PrideDatasetMetadataTool(),
                PrideDatasetFilesTool(),
                GeoMetadataFetcherTool(),
                GeoDatasetFilesTool(),
                SraDatasetMetadataTool(),
                SraDatasetFilesTool(),
                EnaDatasetMetadataTool(),
                EnaFastqFilesTool(),
                SynapseFileUploadTool(syn=syn),
                SynapseFolderCreationTool(syn=syn),
                SynapseExternalFileLinkTool(syn=syn),
                SynapseBatchFolderCreationTool(syn=syn),
                SynapseBatchExternalFileLinkTool(syn=syn),
                SynapsePythonCodeExecutorTool(syn=syn),
                CodeInterpreterTool()
            ],
            llm=get_llm(),
            verbose=True,
            allow_delegation=False
        ) 