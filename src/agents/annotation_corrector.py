from crewai import Agent
from src.tools.jsonld_tools import JsonLdGetValidValuesTool
from src.tools.synapse_tools import SynapsePythonCodeExecutorTool

def get_annotation_corrector_agent(llm, syn):
    """
    Creates an agent responsible for correcting Synapse annotations.
    """
    jsonld_tool = JsonLdGetValidValuesTool()
    synapse_tool = SynapsePythonCodeExecutorTool(syn=syn)
    
    return Agent(
        role='Synapse Annotation Corrector',
        goal='To ensure all annotations in a Synapse view are valid according to a specified data model.',
        backstory=(
            "You are an expert in data curation and validation within the Synapse platform. "
            "Your primary function is to analyze annotations in a given Synapse file view, "
            "compare them against a standard data model (JSON-LD), and identify any discrepancies. "
            "You meticulously check for invalid values, typos, or terms that need to be mapped to the data model's controlled vocabulary. "
            "You must then formulate a clear and executable plan to correct these annotations, which you will pass back to your coordinator."
        ),
        verbose=True,
        allow_delegation=False,
        tools=[jsonld_tool, synapse_tool],
        llm=llm,
    ) 