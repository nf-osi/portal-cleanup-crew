from crewai import Task
import json

def create_normalization_task(agent, terms_to_normalize, user_rules):
    """
    Creates a task for the agent to normalize a list of terms based on user-provided rules.
    """
    
    # Base description
    description = (
        "Your task is to analyze the following list of terms and normalize them based on a set of rules. "
        "Your goal is to consolidate variations of the same term into a single, canonical form. "
        "For example, 'David Largaespada' and 'David Largespada' should be mapped to one of those. "
        "'Dr. Brigitte Widemann' should be mapped to 'Brigitte Widemann'."
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
        f"--- END OF LIST ---"
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