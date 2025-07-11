from crewai import Agent
from src.utils.llm_utils import get_llm
from src.tools.synapse_analysis_tools import (
    SynapseFolderAnalysisTool,
    MetadataFileAnalysisTool,
    TemplateDetectionTool,
    AnnotationCSVSaveTool,
    AnnotationGenerationTool
)
from src.tools.jsonld_tools import JsonLdGetValidValuesTool, JsonLdGetManifestsTool
from src.tools.synapse_tools import (
    SynapsePythonCodeExecutorTool,
    SynapseBatchAnnotationTool
)
import synapseclient


def get_dataset_annotation_agent(syn: synapseclient.Synapse):
    """
    Creates a Dataset Annotation Agent equipped with tools to analyze existing
    Synapse datasets and apply intelligent schema-based annotations.
    
    Args:
        syn: Authenticated Synapse client instance
        
    Returns:
        Agent configured for dataset analysis and annotation
    """
    return Agent(
        role='Dataset Annotation Specialist',
        goal=(
            'Analyze existing Synapse datasets to understand their structure, content, '
            'and context, then apply appropriate schema-based annotations to data files '
            'using intelligent metadata template detection and controlled vocabularies.'
        ),
        backstory=(
            "You are an expert in scientific data curation and metadata standardization. "
            "You specialize in analyzing existing datasets to understand their scientific "
            "context and applying appropriate metadata annotations. Your expertise includes:\n"
            "- Analyzing file structures and classifying data vs metadata files\n"
            "- Extracting external identifiers from file names, descriptions, and annotations\n"
            "- Reading and parsing various metadata file formats (CSV, JSON, XML, etc.)\n"
            "- Determining appropriate metadata templates based on data type and content\n"
            "- Applying schema-compliant annotations using controlled vocabularies\n"
            "- Creating documentation of annotation decisions for reproducibility\n\n"
            "You approach each dataset systematically, first understanding what data is present, "
            "then gathering all available metadata, determining the most appropriate annotation "
            "template, and finally applying consistent, high-quality annotations to data files only. "
            "You are meticulous about using controlled vocabularies and ensuring all annotations "
            "comply with the specified data model. You focus on DATA files for annotation, "
            "distinguishing them from metadata and auxiliary files."
        ),
        tools=[
            SynapseFolderAnalysisTool(syn=syn),
            MetadataFileAnalysisTool(syn=syn),
            TemplateDetectionTool(),
            AnnotationGenerationTool(),
            JsonLdGetValidValuesTool(),
            JsonLdGetManifestsTool(),
            SynapseBatchAnnotationTool(syn=syn),
            AnnotationCSVSaveTool(),
            SynapsePythonCodeExecutorTool(syn=syn)
        ],
        llm=get_llm(),
        verbose=True,
        allow_delegation=False
    )


class DatasetAnnotationAgent(Agent):
    """
    Alternative class-based implementation of the Dataset Annotation Agent.
    Use get_dataset_annotation_agent() function for most use cases.
    """
    
    def __init__(self, syn: synapseclient.Synapse):
        self.syn = syn
        super().__init__(
            role='Dataset Annotation Specialist',
            goal=(
                'Analyze existing Synapse datasets to understand their structure, content, '
                'and context, then apply appropriate schema-based annotations to data files '
                'using intelligent metadata template detection and controlled vocabularies.'
            ),
            backstory=(
                "You are an expert in scientific data curation and metadata standardization. "
                "You specialize in analyzing existing datasets to understand their scientific "
                "context and applying appropriate metadata annotations. Your expertise includes:\n"
                "- Analyzing file structures and classifying data vs metadata files\n"
                "- Extracting external identifiers from file names, descriptions, and annotations\n"
                "- Reading and parsing various metadata file formats (CSV, JSON, XML, etc.)\n"
                "- Determining appropriate metadata templates based on data type and content\n"
                "- Applying schema-compliant annotations using controlled vocabularies\n"
                "- Creating documentation of annotation decisions for reproducibility\n\n"
                "You approach each dataset systematically, first understanding what data is present, "
                "then gathering all available metadata, determining the most appropriate annotation "
                "template, and finally applying consistent, high-quality annotations to data files only. "
                "You are meticulous about using controlled vocabularies and ensuring all annotations "
                "comply with the specified data model. You focus on DATA files for annotation, "
                "distinguishing them from metadata and auxiliary files."
            ),
            tools=[
                SynapseFolderAnalysisTool(syn=syn),
                MetadataFileAnalysisTool(syn=syn),
                TemplateDetectionTool(),
                AnnotationGenerationTool(),
                JsonLdGetValidValuesTool(),
                JsonLdGetManifestsTool(),
                SynapseBatchAnnotationTool(syn=syn),
                AnnotationCSVSaveTool(),
                SynapsePythonCodeExecutorTool(syn=syn)
            ],
            llm=get_llm(),
            verbose=True,
            allow_delegation=False
        ) 