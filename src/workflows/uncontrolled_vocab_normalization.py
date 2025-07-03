from crewai import Crew, Process
from src.agents.uncontrolled_vocab_normalizer import get_uncontrolled_vocab_normalizer_agent
from src.tasks.normalization_tasks import create_normalization_task
from tqdm import tqdm
import concurrent.futures
import difflib
import re
import time
import synapseclient
import pandas as pd
import json

class UncontrolledVocabNormalizationWorkflow:
    def __init__(self, syn, llm, views):
        self.syn = syn
        self.llm = llm
        self.views = views
        self.normalizer_agent = get_uncontrolled_vocab_normalizer_agent(llm=self.llm)

    def _parse_agent_output(self, raw_output: str) -> dict:
        """
        Parses the agent's raw output to extract the corrected text from a JSON markdown block.
        If no block is found, it returns an empty dictionary.
        """
        match = re.search(r"```json\n(.*?)\n```", raw_output, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1).strip())
            except json.JSONDecodeError:
                print("Error: Agent returned malformed JSON.")
                return {}
        # Fallback to raw if the agent fails to follow instructions
        print("Warning: Agent did not return a JSON block. Unable to parse corrections.")
        return {}

    def _print_diff(self, original, corrected):
        """
        Calculates and prints a colored, character-by-character diff.
        Returns True if differences were found, False otherwise.
        """
        diff = difflib.unified_diff(
            original.splitlines(),
            corrected.splitlines(),
            fromfile='Original',
            tofile='Corrected',
            n=5,
            lineterm=''
        )
        
        diff_lines = list(diff)
        if not diff_lines:
            return False

        print("\n--- Proposed Normalization ---")
        print("Showing differences between original and normalized value (+/-):")
        
        for line in diff_lines:
            if line.startswith('+') and not line.startswith('+++'):
                print(f'\033[92m{line}\033[0m', end='')
            elif line.startswith('-') and not line.startswith('---'):
                print(f'\033[91m{line}\033[0m', end='')
            elif line.startswith('@@'):
                print(f'\033[96m{line}\033[0m', end='')
            else:
                print(line, end='')
        
        return True

    def run(self):
        print("\n--- Uncontrolled Vocabulary Normalization Workflow ---")
        
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

        print("\nAvailable columns to normalize:")
        for i, col in enumerate(columns):
            print(f"{i+1}. {col}")
        
        while True:
            try:
                choice = int(input("Enter the number of the column you want to normalize: ")) - 1
                if 0 <= choice < len(columns):
                    column_name = columns[choice]
                    break
                else:
                    print("Invalid choice. Please enter a number from the list.")
            except ValueError:
                print("Invalid input. Please enter a number.")

        print(f"\n--- Normalizing column: '{column_name}' ---")

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
            query = f'SELECT "{row_id_col}", "{column_name}" FROM {self.view_synapse_id} WHERE "{column_name}" IS NOT NULL'
            all_data_df = self.syn.tableQuery(query, resultsAs="csv", includeRowIdAndRowVersion=False).asDataFrame()

            actual_row_id_col = next((c for c in all_data_df.columns if c.lower() == row_id_col.lower()), None)
            actual_column_name = next((c for c in all_data_df.columns if c.lower() == column_name.lower()), None)

            if not actual_row_id_col or not actual_column_name:
                print(f"Error: Could not find specified columns ('{row_id_col}', '{column_name}') in downloaded table.")
                print(f"Available columns: {list(all_data_df.columns)}")
                return

            # Handle list-type columns by flattening them to get unique values
            all_values = all_data_df[actual_column_name].dropna()
            unique_items = set()
            for item in all_values:
                if isinstance(item, list):
                    for sub_item in item:
                        if isinstance(sub_item, str):
                            unique_items.add(sub_item.strip())
                elif isinstance(item, str):
                    unique_items.add(item.strip())
            
            unique_values = sorted(list(unique_items))

        except Exception as e:
            print(f"Error querying for unique values in '{column_name}': {e}")
            return
            
        if not unique_values:
            print(f"No non-null values found in column '{column_name}'.")
            return
            
        print(f"\n--- Found {len(unique_values)} unique values to normalize in '{column_name}' ---")
        for val in unique_values:
            print(f"- {val}")

        print("\nPlease provide any normalization rules you'd like to apply.")
        print("For example: 'remove all instances of PhD', 'replace 'Bob' with 'Robert''")
        user_rules = input("Enter rules (or press Enter to skip): ")

        # 3. Create and run a single task for all values
        print("\n--- Sending all terms to the agent for normalization... ---")
        print("This may take several minutes depending on the size of the list...")
        task = create_normalization_task(self.normalizer_agent, unique_values, user_rules)
        crew = Crew(
            agents=[self.normalizer_agent],
            tasks=[task],
            process=Process.sequential
        )
        
        raw_output = crew.kickoff().raw
        final_corrections = self._parse_agent_output(raw_output)

        if not final_corrections:
            print("\nAgent did not propose any normalizations. Exiting workflow.")
            return

        # 4. Review proposed changes with the user
        print("\n--- Proposed Normalizations ---")
        for original, corrected in final_corrections.items():
            print(f"- Change '{original}' to '{corrected}'")

        approval = input("\nDo you approve this plan? (yes/no): ").lower()
        if approval not in ['y', 'yes']:
            print("\nPlan rejected. No changes were made.")
            return

        print("Executing the approved normalization plan...")

        updates = []
        print("Gathering all entities to update...")

        for _, row in tqdm(all_data_df.iterrows(), total=len(all_data_df)):
            current_value = row[actual_column_name]
            row_id = row[actual_row_id_col]
            needs_update = False
            
            if pd.isna(current_value):
                continue

            if isinstance(current_value, list):
                # Handle list-type columns, replacing only the relevant items
                new_list = []
                for item in current_value:
                    if item in final_corrections:
                        new_list.append(final_corrections[item])
                        needs_update = True
                    else:
                        new_list.append(item)
                if needs_update:
                    updates.append({
                        row_id_col: row_id,
                        column_name: new_list
                    })
            elif isinstance(current_value, str) and current_value in final_corrections:
                # Handle single string value columns
                updates.append({
                    row_id_col: row_id,
                    column_name: final_corrections[current_value]
                })

        if not updates:
            print("No updates to apply.")
            return

        update_df = pd.DataFrame(updates)
        try:
            self.syn.store(synapseclient.Table(self.view_synapse_id, update_df))
            print(f"\nSuccessfully applied {len(updates)} normalizations to '{column_name}' in '{self.view_synapse_id}'.")
        except Exception as e:
            print(f"\nAn error occurred while storing the updates to Synapse: {e}")
            print("Please check the Synapse table and try again.")

        print("\nUncontrolled Vocabulary Normalization process finished.")

        return None