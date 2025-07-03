from crewai import Task

def create_sample_task(agent):
    """
    Creates a sample task for an agent.
    """
    return Task(
        description=(
            "Investigate the 'AMP-ALS' data portal on Synapse for missing metadata in file assets. "
            "Identify files that are missing the 'fileFormat' annotation."
        ),
        expected_output=(
            "A list of Synapse IDs for files that are missing the 'fileFormat' annotation."
        ),
        agent=agent
    ) 