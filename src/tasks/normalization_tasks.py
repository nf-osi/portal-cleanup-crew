from crewai import Task, Agent
import json
import pandas as pd

def create_normalization_task(agent: Agent, terms_to_normalize, user_rules):
    """
    Creates a task for the agent to normalize a list of terms based on user-provided rules.
    """
    
    # Base description
    description = (
        "Your task is to analyze the following list of terms and normalize them based on a set of rules. "
        "Your goal is to consolidate variations of the same term into a single, canonical form. "
        "For example, if working with names, 'David Largaespada' and 'David Largespada' should be mapped to one of those. "
        "If working with statuses, 'In Progress' and 'in-progress' might be normalized to a single form. "
        "You will be given a list of terms to analyze."
    )

    # Add user-provided rules if they exist
    if user_rules:
        description += (
            "\n\nPlease apply the following user-provided rules to the entire list:\n"
            f"- {user_rules}"
        )

    # Add the terms to be normalized
    description += (
        f"\n\nHere is the full list of terms to normalize:\n"
        f"--- START OF LIST ---\n"
        f"{json.dumps(terms_to_normalize, indent=2)}\n"
        f"--- END OF LIST ---\n\n"
        "CRITICAL INSTRUCTION: Only consolidate terms that are clearly variations of the SAME entity "
        "(e.g., abbreviations, alternate spellings, typos, different capitalization). "
        "DO NOT map a term for one entity to a completely different entity. "
        "If you are unsure whether two terms represent the same entity, do not suggest a change."
    )

    return Task(
        description=description,
        agent=agent,
        expected_output=(
            "A JSON object mapping the original terms that need changing to their new, normalized values. "
            "Only include entries for terms that have been changed. "
            "The output should be in a JSON markdown block, like this:\n"
            "```json\n"
            '{\n'
            '  "original_term_1": "normalized_term_1",\n'
            '  "original_term_2": "normalized_term_2"\n'
            '}\n'
            "```\n"
            "If no terms need normalization, return an empty JSON object: {}. "
            "Do not include your own thought processes or any other text outside of this block."
        )
    )

def create_synapse_update_task(agent: Agent, entity_id: str, is_view: bool, updates_df: pd.DataFrame) -> Task:
    """
    Creates a task for the Synapse agent to update a Synapse entity.
    """
    tool_name = "update_view" if is_view else "update_table"
    entity_type = "View" if is_view else "Table"

    return Task(
        description=(
            f"Update the Synapse {entity_type} with Synapse ID '{entity_id}'. "
            f"You have been provided with a pandas DataFrame containing the necessary updates. "
            f"Use the '{tool_name}' tool to apply these changes. The DataFrame with the updates "
            f"is available in the context."
        ),
        expected_output=(
            f"A confirmation message indicating that the Synapse {entity_type} '{entity_id}' "
            "has been successfully updated."
        ),
        agent=agent,
        context={"entity_id": entity_id, "updates_df": updates_df}
    ) 