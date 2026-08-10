from crewai import Agent
from src.tools.synapse_tools import UpdateViewTool, UpdateTableTool
import synapseclient

def get_synapse_agent(llm, syn: synapseclient.Synapse):
    """
    Creates a Synapse Agent equipped with tools to update Synapse views and tables.
    """
    return Agent(
        role='Synapse Data Manager',
        goal='Manage and update data stored in Synapse views and tables based on provided instructions and dataframes.',
        backstory=(
            "You are an expert agent specializing in interacting with the Synapse platform. "
            "You are highly skilled at using the synapseclient library to perform data operations. "
            "You are careful, precise, and always ensure that data updates are performed correctly "
            "and efficiently. Your primary function is to take structured data in the form of pandas "
            "DataFrames and apply updates to the corresponding Synapse entities."
        ),
        tools=[
            UpdateViewTool(syn=syn),
            UpdateTableTool(syn=syn)
        ],
        llm=llm,
        verbose=True,
        allow_delegation=False
    ) 