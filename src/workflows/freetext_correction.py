from crewai import Crew, Process, Agent, Task
from src.agents.freetext_corrector import get_freetext_corrector_agent
from src.agents.synapse_agent import get_synapse_agent
from src.agents.github_issue_filer import GitHubIssueFilerAgent
from src.tasks.freetext_tasks import create_freetext_correction_task
from tqdm import tqdm
import concurrent.futures
import difflib
import re
import time
import synapseclient
import pandas as pd
from src.utils.cli_utils import prompt_for_view_and_column

class FreetextCorrectionWorkflow:
    def __init__(self, syn, llm, views, freetext_settings=None):
        self.syn = syn
        self.llm = llm
        self.views = views
        self.freetext_agent = get_freetext_corrector_agent(llm=self.llm)
        self.synapse_agent = get_synapse_agent(llm=self.llm, syn=self.syn)

    def _select_context_column_with_agent(self, all_columns: list, column_being_corrected: str) -> str:
        """
        Uses an LLM agent to select the best context column from a list.
        """
        # Create a temporary agent to make the selection
        column_selector_agent = Agent(
            role="Data Context Analyst",
            goal=f"Select the single best column from a list that provides high-level context for a data correction. The column being corrected is '{column_being_corrected}'. The context column should ideally be a study name, project ID, or another primary identifier that helps locate the data's origin.",
            backstory="You are an expert at understanding data table structures and identifying columns that provide the most meaningful context for data curation tasks.",
            llm=self.llm,
            verbose=True,
        )

        task_prompt = f"""
        Analyze the following list of column names and select the ONE column that would provide the best context for a data correction task on the '{column_being_corrected}' column.

        The best context column is typically one that represents a broader grouping, such as:
        - A study ID or name (e.g., 'studyId', 'studyName', 'parentStudy')
        - A project ID or name (e.g., 'projectId', 'projectName')
        - A primary entity identifier (e.g., 'id', 'doi', 'pmid')

        Do not select the column that is being corrected ('{column_being_corrected}').
        Do not select columns that represent granular file details (e.g., 'dataFileMD5Hex', 'dataFileKey') or internal system IDs (e.g., 'etag', 'dataFileHandleId') unless they are the only option.

        Here is the list of columns:
        {all_columns}

        Your final answer MUST be just the name of the selected column and nothing else.
        """

        selection_task = Task(
            description=task_prompt,
            agent=column_selector_agent,
            expected_output="The name of the single best context column.",
        )

        crew = Crew(
            agents=[column_selector_agent],
            tasks=[selection_task],
            process=Process.sequential,
        )

        selected_column = crew.kickoff().raw
        
        # Validate that the agent returned a valid column name
        if selected_column.strip() in all_columns:
            return selected_column.strip()
        else:
            # Fallback or error
            print(f"Warning: Agent selected an invalid column '{selected_column}'. Falling back to the first column as context.")
            return all_columns[0]

    def _parse_agent_output(self, raw_output: str) -> str:
        """
        Parses the agent's raw output to extract the corrected text from a markdown block.
        If no block is found, it returns the raw output for debugging.
        """
        match = re.search(r"```text\n(.*?)\n```", raw_output, re.DOTALL)
        if match:
            return match.group(1).strip()
        # Fallback to raw if the agent fails to follow instructions
        return raw_output

    def _print_diff(self, original, corrected):
        """
        Calculates and prints a colored, sentence-by-sentence diff.
        Returns True if differences were found, False otherwise.
        """
        # For a more readable diff, split text into sentences.
        # This is for display purposes only.
        original_lines = re.split('(?<=[.?!])\\s+', original)
        corrected_lines = re.split('(?<=[.?!])\\s+', corrected)

        diff = difflib.unified_diff(original_lines, corrected_lines, fromfile='Original', tofile='Corrected', n=5, lineterm='')
        
        diff_lines = list(diff)
        if not diff_lines:
            # No semantic differences found, so don't prompt the user.
            return False

        print("\n--- Proposed Correction ---")
        print("Showing differences between original and corrected text (+/-):")
        
        for line in diff_lines:
            if line.startswith('+') and not line.startswith('+++'):
                print(f'\033[92m{line}\033[0m', end='') # Green
            elif line.startswith('-') and not line.startswith('---'):
                print(f'\033[91m{line}\033[0m', end='') # Red
            elif line.startswith('@@'):
                print(f'\033[96m{line}\033[0m', end='') # Cyan
            else:
                print(line, end='')
        
        return True

    def run(self):
        print("\n--- Free-Text Correction Workflow ---")
        
        if not self.views:
            print("No views configured in config.yaml. Exiting.")
            return

        view_synapse_id, column_name, row_id_col = prompt_for_view_and_column(self.syn, self.views)
        if not view_synapse_id:
            return # User cancelled

        self.view_synapse_id = view_synapse_id

        print(f"\n--- Checking column: '{column_name}' ---")

        # 2. Query for unique, non-null values
        try:
            # Download the necessary columns to perform filtering in memory
            query = f'SELECT * FROM {self.view_synapse_id} WHERE "{column_name}" IS NOT NULL'
            all_data_df = self.syn.tableQuery(query, resultsAs="csv", includeRowIdAndRowVersion=False).asDataFrame()

            # The Synapse client sometimes renames columns when downloading.
            # We need to find the actual column names in the dataframe.
            # This is a common source of KeyErrors.
            actual_row_id_col = next((c for c in all_data_df.columns if c.lower() == row_id_col.lower()), None)
            actual_column_name = next((c for c in all_data_df.columns if c.lower() == column_name.lower()), None)

            if not actual_row_id_col or not actual_column_name:
                print(f"Error: Could not find specified columns ('{row_id_col}', '{column_name}') in downloaded table.")
                print(f"Available columns: {list(all_data_df.columns)}")
                return

            unique_values = all_data_df[actual_column_name].dropna().unique().tolist()
        except Exception as e:
            print(f"Error querying for unique values in '{column_name}': {e}")
            return
            
        if not unique_values:
            print(f"No non-null values found in column '{column_name}'.")
            return
            
        print(f"Found {len(unique_values)} unique text values to check.")

        final_corrections = {}
        stop_processing = False
        # 3-4. Create and run tasks, then review with user
        for value in tqdm(unique_values, desc="Processing text fields"):
            if not isinstance(value, str) or len(value.split()) < 5: # Skip short or non-string values
                continue
                
            task = create_freetext_correction_task(self.freetext_agent, value)
            crew = Crew(
                agents=[self.freetext_agent],
                tasks=[task],
                process=Process.sequential,
                verbose=False
            )
            
            corrected_text = value # Default to original value
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    corrected_text_raw = crew.kickoff().raw
                    corrected_text = self._parse_agent_output(corrected_text_raw)
                    break  # Success
                except Exception as e:
                    # Catch network errors and retry
                    if "RemoteProtocolError" in str(e) or "peer closed connection" in str(e):
                        if attempt < max_retries - 1:
                            print(f"\nNetwork error processing item. Retrying in 2 seconds... (Attempt {attempt + 2}/{max_retries})")
                            time.sleep(2)
                        else:
                            print(f"\nFailed to process an item after {max_retries} attempts due to a network error. Skipping.")
                    else:
                        # For non-network errors, print and skip immediately
                        print(f"\nAn unexpected error occurred: {e}. Skipping this item.")
                        break

            if corrected_text.strip() != value.strip():
                # Only prompt for review if a meaningful diff exists
                if self._print_diff(value, corrected_text):
                    prompt = "\nAccept (a), provide different value (d), reject (r), or accept and stop (s)? "
                    while True:
                        user_choice = input(prompt).lower()
                        if user_choice == 'a':
                            final_corrections[value] = corrected_text
                            break
                        elif user_choice == 'd':
                            new_correction = input(f"Enter the correct text: ")
                            final_corrections[value] = new_correction
                            break
                        elif user_choice == 'r':
                            break
                        elif user_choice == 's':
                            # Accept the current correction and set flag to stop
                            final_corrections[value] = corrected_text
                            stop_processing = True
                            break
                        else:
                            print("Invalid choice. Please enter 'a', 'd', 'r', or 's'.")
            
            if stop_processing:
                break

        if not final_corrections:
            print("\nNo corrections were approved. Exiting workflow.")
            return

        # Ask user if they want to propagate changes to other sources
        propagate_prompt = "\nDo you want to apply these corrections to other data sources (e.g., a GitHub repository)? (yes/no): "
        propagate = input(propagate_prompt).lower()
        if propagate in ['y', 'yes']:
            source = input("Please specify the data source (e.g., GitHub repository URL): ")
            if "github.com" in source:
                try:
                    # Initialize the GitHub issue filer agent
                    github_agent = GitHubIssueFilerAgent()
                    
                    print("Agent is selecting the best context column for the GitHub issue...")
                    cols = [c['name'] for c in self.syn.getColumns(self.view_synapse_id)]
                    
                    context_col = self._select_context_column_with_agent(cols, actual_column_name)
                    print(f"Agent selected '{context_col}' as the context column.")

                    if not context_col:
                        print(f"Warning: Agent could not select a context column. Cannot propagate changes for GitHub update.")
                    else:
                        # The data is already in all_data_df, no need to re-query
                        corrections_with_ids = []
                        for original, corrected in final_corrections.items():
                            # Find all study IDs associated with the original (uncorrected) text
                            matching_rows = all_data_df[all_data_df[actual_column_name] == original]
                            context_values = matching_rows[context_col].unique().tolist() if context_col in all_data_df.columns else []
                            corrections_with_ids.append({
                                "original": original,
                                "corrected": corrected,
                                "context": context_values,
                            })
                        
                        if corrections_with_ids:
                            title = f"Data Corrections for column '{column_name}'"
                            body = self._create_github_issue_body(corrections_with_ids, column_name)
                            issue_url = github_agent.file_issue(title, body, source)
                            if issue_url:
                                print(f"✅ Created GitHub issue for freetext corrections: {issue_url}")
                
                except Exception as e:
                    print(f"❌ Error creating GitHub issue: {e}")
                    print("You may need to check your GitHub CLI authentication or repository access.")

        # 5. Final Plan and Execution
        print("\n--- Final Plan ---")
        print("The following corrections will be applied to Synapse:")
        for current, new in final_corrections.items():
            print(f"  - Change '{current}' to     '{new}'")
        
        approval = input("\nDo you approve this plan? (yes/no): ").lower()
        if approval not in ['y', 'yes']:
            print("\nPlan rejected. No changes were made.")
            return

        print("Executing the approved correction plan...")

        # Find the entities to update. We already have the data in `all_data_df`.
        entities_to_update = all_data_df[all_data_df[actual_column_name].isin(final_corrections.keys())].copy()
        
        # Create the new column data to be updated
        update_data = entities_to_update[[actual_row_id_col]].copy()
        update_data[actual_column_name] = entities_to_update[actual_column_name].map(final_corrections)

        if update_data.empty:
            print("Could not find any matching rows to update. This might be a data consistency issue.")
            return

        print(f"Preparing to update {len(update_data)} rows...")

        # Use synapse_agent to handle the table update
        print(f"Using Synapse agent to update {len(update_data)} rows...")
        
        # The synapse_agent will handle proper column selection and etag management
        update_result = self.synapse_agent.tools[1]._run(  # UpdateTableTool
            table_id=self.view_synapse_id,
            updates_df=update_data
        )
        print(f"Synapse agent result: {update_result}")

        print("\nFree-text correction process finished.")

    def _create_github_issue_body(self, corrections_with_ids, column_name):
        """Create a formatted body for the GitHub issue."""
        body_lines = [
            f"## Free-text Corrections for Column: {column_name}",
            "",
            f"The following corrections were applied to the `{column_name}` column:",
            ""
        ]
        
        for correction in corrections_with_ids:
            body_lines.append(f"**Original:** {correction['original']}")
            body_lines.append(f"**Corrected:** {correction['corrected']}")
            if correction.get('context'):
                body_lines.append(f"**Context:** {', '.join(correction['context'])}")
            body_lines.append("")
        
        return "\n".join(body_lines)