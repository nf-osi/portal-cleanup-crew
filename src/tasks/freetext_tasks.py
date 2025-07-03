from crewai import Task

def create_freetext_correction_task(agent, text_to_correct):
    """
    Creates a task for the agent to correct a given text.
    """
    return Task(
        description=(
            "Your task is to identify and fix ONLY specific, objective errors in the following text. "
            "You should ONLY correct:\n"
            "1. Clear formatting errors, such as extra spaces, broken markdown, or misplaced punctuation.\n"
            "2. Broken or mojibake unicode characters that need to be repaired.\n"
            "3. Obvious, undeniable single-word typos (e.g., 'teh' should be 'the').\n\n"
            "DO NOT rephrase, reword, or restructure any part of the text. "
            "Your goal is to preserve the original phrasing and meaning exactly, only cleaning up technical or typographical artifacts. "
            "If there are no such errors, return the original text unchanged.\n\n"
            f"Here is the text to correct:\n"
            f"--- START OF TEXT ---\n"
            f"{text_to_correct}\n"
            f"--- END OF TEXT ---"
        ),
        agent=agent,
        expected_output=(
            "The corrected text, and ONLY the corrected text, inside a markdown block like this:\n"
            "```text\n"
            "[corrected text here]\n"
            "```\n"
            "Do not include your own thought processes or any other text outside of this block."
        )
    ) 