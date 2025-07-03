from crewai import Agent

def get_term_standardizer_agent(llm):
    """
    Creates the Uncontrolled Term Standardizer agent.
    """
    return Agent(
        role='Uncontrolled Term Standardizer',
        goal=(
            "Standardize uncontrolled terms in Synapse annotations to "
            "ensure consistency across the portal."
        ),
        backstory=(
            "You have a keen eye for variations in terminology. You identify "
            "different representations of the same entity (e.g., investigator "
            "names) and consolidate them into a single, standardized format. "
            "Your work is crucial for improving the clarity and usability of "
            "portal filters and facets."
        ),
        allow_delegation=False,
        verbose=True,
        llm=llm
    ) 