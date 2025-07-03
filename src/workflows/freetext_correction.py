from crewai import Crew, Process
from src.agents.freetext_corrector import get_freetext_corrector_agent
from src.tasks.freetext_tasks import create_freetext_correction_task
from tqdm import tqdm
import concurrent.futures
import difflib
import re
import time
import synapseclient
import pandas as pd

class FreetextCorrectionWorkflow:
    def __init__(self, syn, llm, views):
        self.syn = syn
        self.llm = llm
        self.views = views
        self.freetext_agent = get_freetext_corrector_agent(llm=self.llm)

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
        
        # Prompt user to select a table
        view_choices = list(self.views.keys())
        if not view_choices:
            print("No views configured in config.yaml. Exiting.")
            return

        print("\nPlease select a table to work on:")
        for i, view_name in enumerate(view_choices):
            print(f"{i+1}. {view_name} ({self.views[view_name]})")

        while True:
            try:
                choice = int(input("Enter the number of your choice: ")) - 1
                if 0 <= choice < len(view_choices):
                    selected_view_name = view_choices[choice]
                    self.view_synapse_id = self.views[selected_view_name]
                    print(f"\nYou have selected: {selected_view_name}")
                    break
                else:
                    print("Invalid choice. Please enter a number from the list.")
            except ValueError:
                print("Invalid input. Please enter a number.")

        # 1. Ask user for the column name to correct
        try:
            view = self.syn.get(self.view_synapse_id)
            columns = [c['name'] for c in self.syn.getColumns(view)]
        except Exception as e:
            print(f"Error fetching columns for view '{self.view_synapse_id}': {e}")
            return

        print("\nAvailable columns to check:")
        for i, col in enumerate(columns):
            print(f"{i+1}. {col}")
        
        while True:
            try:
                choice = int(input("Enter the number of the column you want to correct: ")) - 1
                if 0 <= choice < len(columns):
                    column_name = columns[choice]
                    break
                else:
                    print("Invalid choice. Please enter a number from the list.")
            except ValueError:
                print("Invalid input. Please enter a number.")

        print(f"\n--- Checking column: '{column_name}' ---")

        # Ask user for the row identifier column
        print("\nPlease select the column that uniquely identifies rows (e.g., ROW_ID):")
        for i, col in enumerate(columns):
            print(f"{i+1}. {col}")

        while True:
            try:
                choice = int(input("Enter the number of the identifier column: ")) - 1
                if 0 <= choice < len(columns):
                    row_id_col = columns[choice]
                    print(f"Using '{row_id_col}' as the row identifier.")
                    break
                else:
                    print("Invalid choice. Please enter a number from the list.")
            except ValueError:
                print("Invalid input. Please enter a number.")

        # 2. Query for unique, non-null values
        try:
            # Download the necessary columns to perform filtering in memory
            query = f'SELECT "{row_id_col}", "{column_name}" FROM {self.view_synapse_id} WHERE "{column_name}" IS NOT NULL'
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
                    prompt = "Accept (a), provide different value (d), reject (r), or accept and stop (s)? "
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
        self.follow_up_tasks = []
        propagate_prompt = "\nDo you want to apply these corrections to other data sources (e.g., a GitHub repository)? (yes/no): "
        propagate = input(propagate_prompt).lower()
        if propagate in ['y', 'yes']:
            source = input("Please specify the data source (e.g., GitHub repository URL): ")
            if "github.com" in source:
                corrections_with_ids = []
                print("Gathering study identifiers for corrections...")
                cols = [c['name'] for c in self.syn.getColumns(self.view_synapse_id)]
                study_id_col = 'studyId' if 'studyId' in cols else None

                if not study_id_col:
                    print(f"Warning: No 'studyId' column found in {self.view_synapse_id}. Cannot propagate changes for GitHub update.")
                else:
                    # The data is already in all_data_df, no need to re-query
                    corrections_with_ids = []
                    for original, corrected in final_corrections.items():
                        # Find all study IDs associated with the original (uncorrected) text
                        matching_rows = all_data_df[all_data_df[actual_column_name] == original]
                        study_ids = matching_rows[study_id_col].unique().tolist() if study_id_col in all_data_df.columns else []
                        corrections_with_ids.append({
                            "original": original,
                            "corrected": corrected,
                            "study_ids": study_ids,
                        })
                
                self.follow_up_tasks.append({
                    'type': 'github_issue',
                    'repo_url': source,
                    'corrections': corrections_with_ids
                })

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
        entities_to_update = all_data_df[all_data_df[actual_column_name].isin(final_corrections.keys())]
        
        if entities_to_update.empty:
            print("Could not find any matching rows to update. This might be a data consistency issue.")
            return

        updates = []
        print("Gathering all entities to update...")
        for _, row in tqdm(entities_to_update.iterrows(), total=len(entities_to_update)):
            original_text = row[actual_column_name]
            corrected_text = final_corrections[original_text]
            updates.append({
                row_id_col: row[actual_row_id_col],
                column_name: corrected_text
            })

        if not updates:
            print("No updates to apply.")
            return

        # Convert to a DataFrame and store in Synapse
        update_df = pd.DataFrame(updates)
        try:
            self.syn.store(synapseclient.Table(self.view_synapse_id, update_df))
            print(f"\nSuccessfully applied {len(updates)} corrections to '{column_name}' in '{self.view_synapse_id}'.")
        except Exception as e:
            print(f"\nAn error occurred while storing the updates to Synapse: {e}")
            print("Please check the Synapse table and try again.")

        print("\nFree-text correction process finished.")

        if hasattr(self, 'follow_up_tasks') and self.follow_up_tasks:
            return self.follow_up_tasks