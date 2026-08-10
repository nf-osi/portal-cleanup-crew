from crewai import Agent, Task
from src.tools.zooma_tools import ZoomaTermMappingTool
from src.utils.llm_utils import get_llm
import subprocess

def get_ontology_expert_agent():
    return Agent(
        role='Biomedical Ontology and Terminology Expert',
        goal='Find the best-matching standardized ontology term for a given text, ensuring it is a sensible replacement in the given context.',
        backstory=(
            "You are an expert in biomedical ontologies and data modeling. Your mission is to provide high-quality, context-aware feedback on ontology term suggestions. "
            "You must evaluate whether a suggested term is semantically appropriate for the given column. If not, you must say so."
        ),
        tools=[ZoomaTermMappingTool(llm=get_llm("ontology_expert"))],
        verbose=True,
        llm=get_llm("ontology_expert")
    )

class OntologyExpert(Agent):
    def __init__(self, **kwargs):
        super().__init__(
            role='Biomedical Ontology and Terminology Expert',
            goal='Find the best-matching standardized ontology term for a given text, ensuring it is a sensible replacement in the given context.',
            backstory=(
                "You are an expert in biomedical ontologies, including foundational ontologies like OBO, and "
                "domain-specific ontologies like NCIT, EFO, and Mondo. You are adept at using mapping tools like ZOOMA "
                "to find the best ontology term for a given text. You are also a critical thinker who can assess "
                "whether a suggested term is a sensible replacement for a value in a specific column, based on "
                "the context provided by other values in that column. You understand that not all mappings are "
                "semantically valid, and your primary goal is to ensure accuracy and contextual appropriateness."
            ),
            tools=[ZoomaTermMappingTool(llm=get_llm("ontology_expert"))],
            verbose=True,
            allow_delegation=False,
            max_iter=5,
            instructions=(
                "Your task is to find the best standardized ontology term for a given value in the context of a specific data column.\n"
                "1.  **Analyze the context**: Carefully consider the column name and any existing values provided. This context is crucial for judging semantic appropriateness.\n"
                "2.  **Find a term**: Use the 'ZOOMA Term Mapper' tool to find a candidate ontology term.\n"
                "3.  **Evaluate the term**: Judge whether the found term is a sensible replacement. For example, a term for 'access level' is not a good replacement in a column for 'resource type'.\n"
                "4.  **Handle failures**: If your first search fails or returns an inappropriate term, try a slightly different query (e.g., more general or specific). **If you still cannot find a good match after 2-3 attempts, you must stop.**\n"
                "5.  **Final Answer**: Your final answer **MUST** be a single JSON object with three keys:\n"
                "    -   `term`: The standardized term name (or `null` if no suitable term is found).\n"
                "    -   `uri`: The full URI for the term (or `null`).\n"
                "    -   `confidence_score`: A float from 0.0 to 1.0 representing your confidence. For example, `0.95`. **This MUST be a number, not a string.** A score of 0.0 means the term is not a sensible replacement.\n"
                "Do not include any other text, thoughts, or explanations in your final answer."
            ),
            **kwargs
        )

    def file_github_issue(self, column_name, original_term, standard_term, uri):
        """Formats and files a GitHub issue to propose a new term for the data model."""
        issue_title = f"New Term Suggestion for '{column_name}': '{standard_term}'"
        issue_body = (
            f"A new term has been proposed for the **`{column_name}`** attribute in the data model.\\n\\n"
            f"**Original Term:** `{original_term}`\\n"
            f"**Suggested Term:** `{standard_term}`\\n"
            f"**Suggested URI:** `{uri}`\\n\\n"
            "This suggestion was approved during the data curation process. Please review and, if appropriate, add the new term to the data model."
        )

        try:
            command = [
                "gh", "issue", "create",
                "--title", issue_title,
                "--body", issue_body,
                "--repo", self.repo_url
            ]
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            issue_url = result.stdout.strip()
            print(f"\\n✅ Successfully created GitHub issue: {issue_url}")
            return issue_url
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            print(f"\\n❌ Failed to create GitHub issue.")
            print(f"Error: {e}")
            return None

    def _get_example_values(self, syn, view_id, column_name):
        """Gets a few example values from a Synapse view column for context."""
        # Implementation of _get_example_values method
        pass 