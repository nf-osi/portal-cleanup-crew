from crewai import Crew, Process
from src.agents.uncontrolled_vocab_normalizer import get_uncontrolled_vocab_normalizer_agent
from src.agents.synapse_agent import get_synapse_agent
from src.tasks.normalization_tasks import create_normalization_task, create_synapse_update_task
from tqdm import tqdm
import concurrent.futures
import difflib
import re
import time
import synapseclient
from synapseclient import Table
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

                    # Determine if the selected entity is a Table or a View
                    entity = self.syn.get(self.view_synapse_id)
                    self.is_view = isinstance(entity, synapseclient.EntityViewSchema)
                    print(f"Detected entity type: {'View' if self.is_view else 'Table'}")
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
            query_results = self.syn.tableQuery(query, resultsAs="csv", includeRowIdAndRowVersion=True)
            all_data_df = query_results.asDataFrame()
            original_etag = query_results.etag

            # The Synapse client sometimes renames columns when downloading.
            # We need to find the actual column names in the dataframe.
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
        all_corrections = self._parse_agent_output(raw_output)

        # Filter out corrections where the original and corrected values are the same
        final_corrections = {
            original: corrected 
            for original, corrected in all_corrections.items() 
            if original != corrected
        }

        if not final_corrections:
            print("\nAgent did not propose any valid normalizations. Exiting workflow.")
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

        # Following Synapse documentation pattern for updating table data
        # We already have the data in all_data_df from the earlier query
        print("Applying corrections to the data...")
        
        # Make a copy for updates
        update_df = all_data_df.copy()
        
        # Apply corrections by string replacement
        for old_value, new_value in final_corrections.items():
            # Convert to string, apply replacement, handle NaN values
            update_df[actual_column_name] = update_df[actual_column_name].astype(str).str.replace(old_value, new_value, regex=False)
            # Convert 'nan' strings back to actual NaN
            update_df[actual_column_name] = update_df[actual_column_name].replace('nan', pd.NA)
        
        # Find rows that actually changed by comparing string representations
        original_str = all_data_df[actual_column_name].astype(str)
        updated_str = update_df[actual_column_name].astype(str)
        changed_mask = original_str != updated_str
        changed_rows = changed_mask.sum()
        
        if changed_rows == 0:
            print("No changes to apply after normalization.")
            return
            
        print(f"Found {changed_rows} rows to update.")
        
        # Store the updated DataFrame back to Synapse
        print("Storing updated data back to Synapse...")
        
        # Only update the rows that actually changed
        changed_df = update_df[changed_mask].copy()
        print(f"Preparing to update {len(changed_df)} rows (out of {len(update_df)} total rows)")
        
        table = Table(self.view_synapse_id, changed_df)
        updated_table = self.syn.store(table)
        print("Table updated successfully!")

        # Ask user if they want to propagate changes to other sources
        self.follow_up_tasks = []
        propagate_prompt = "\nDo you want to create a GitHub issue with these normalizations? (yes/no): "
        propagate = input(propagate_prompt).lower()
        if propagate in ['y', 'yes']:
            source = input("Please specify the data source (e.g., GitHub repository URL): ")
            if "github.com" in source:
                corrections_with_ids = []
                print("Gathering identifiers for the GitHub issue...")
                
                # Ask user for the identifier column
                all_cols = [c['name'] for c in self.syn.getColumns(self.view_synapse_id)]
                print("\nPlease select a column to include in the issue for context (e.g., studyId):")
                for i, col in enumerate(all_cols):
                    print(f"{i+1}. {col}")
                
                issue_id_col_name = None
                while True:
                    try:
                        choice = int(input("Enter the number of the identifier column: ")) - 1
                        if 0 <= choice < len(all_cols):
                            issue_id_col_name = all_cols[choice]
                            break
                        else:
                            print("Invalid choice. Please enter a number from the list.")
                    except ValueError:
                        print("Invalid input. Please enter a number.")
                
                # The data is already in all_data_df, no need to re-query
                # We just need to add the identifier column if it's not already there.
                actual_issue_id_col = next((c for c in all_data_df.columns if c.lower() == issue_id_col_name.lower()), None)
                if not actual_issue_id_col:
                    print(f"Warning: Could not find identifier column '{issue_id_col_name}' in the data. Re-querying to include it.")
                    try:
                        query = f'SELECT "{row_id_col}", "{column_name}", "{issue_id_col_name}" FROM {self.view_synapse_id} WHERE "{column_name}" IS NOT NULL'
                        all_data_df = self.syn.tableQuery(query, resultsAs="csv").asDataFrame()
                        actual_issue_id_col = next((c for c in all_data_df.columns if c.lower() == issue_id_col_name.lower()), None)
                    except Exception as e:
                        print(f"Error re-querying data: {e}. Cannot create GitHub issue.")
                        actual_issue_id_col = None

                if actual_issue_id_col:
                    for original, corrected in final_corrections.items():
                        # Find all study IDs associated with the original (uncorrected) term
                        mask = all_data_df[actual_column_name].apply(lambda x: original in x if isinstance(x, list) else original == x)
                        matching_rows = all_data_df[mask]
                        ids = matching_rows[actual_issue_id_col].unique().tolist()
                        corrections_with_ids.append({
                            "original": original,
                            "corrected": corrected,
                            "study_ids": ids,
                            "column_name": column_name
                        })
                
                if corrections_with_ids:
                    self.follow_up_tasks.append({
                        'type': 'github_issue',
                        'repo_url': source,
                        'corrections': corrections_with_ids
                    })

        return self.follow_up_tasks

    def _standardize_cell_value_to_list(self, cell_value):
        """Helper to consistently handle single values, lists, and string-encoded lists."""
        if pd.isna(cell_value):
            return []
        
        if isinstance(cell_value, str):
            try:
                # Handle cases where list-like columns are returned as JSON strings
                parsed_list = json.loads(cell_value)
                if isinstance(parsed_list, list):
                    return [str(item) for item in parsed_list]
                return [cell_value]
            except (json.JSONDecodeError, TypeError):
                return [cell_value]
        elif isinstance(cell_value, list):
            return [str(item) for item in cell_value]
        
        return []

    def _get_unique_values_from_column(self, all_data_df, column_name):
        """Extracts all unique, non-null items from a column, flattening lists."""
        unique_items = set()
        for _, row in all_data_df.iterrows():
            values_in_cell = self._standardize_cell_value_to_list(row[column_name])
            for item in values_in_cell:
                unique_items.add(item.strip())
        
        return sorted(list(unique_items))

    def _get_row_id_and_etag(self, df):
        if 'ROW_ID' in df.columns and 'ROW_ETAG' in df.columns:
            return df['ROW_ID'], df['ROW_ETAG']
        else:
            return None, None

    def run_for_specific_column(self, view_name, view_id, column_name, row_id_col, user_rules=""):
        """
        Run vocabulary normalization for a specific column and table.
        This method is designed for semi-autonomous operation.
        
        Args:
            view_name: Name of the view/table
            view_id: Synapse ID of the view/table
            column_name: Name of the column to normalize
            row_id_col: Name of the row identifier column
            user_rules: Optional normalization rules from user
        
        Returns:
            List of follow-up tasks
        """
        print(f"\n--- Normalizing column: '{column_name}' in {view_name} ---")
        
        self.view_synapse_id = view_id
        
        # Determine if the selected entity is a Table or a View
        entity = self.syn.get(self.view_synapse_id)
        self.is_view = isinstance(entity, synapseclient.EntityViewSchema)
        
        # Query for unique, non-null values
        try:
            query = f'SELECT "{row_id_col}", "{column_name}" FROM {self.view_synapse_id} WHERE "{column_name}" IS NOT NULL'
            query_results = self.syn.tableQuery(query, resultsAs="csv", includeRowIdAndRowVersion=True)
            all_data_df = query_results.asDataFrame()
            original_etag = query_results.etag

            # Find actual column names in the dataframe
            actual_row_id_col = next((c for c in all_data_df.columns if c.lower() == row_id_col.lower()), None)
            actual_column_name = next((c for c in all_data_df.columns if c.lower() == column_name.lower()), None)

            if not actual_row_id_col or not actual_column_name:
                print(f"Error: Could not find specified columns ('{row_id_col}', '{column_name}') in downloaded table.")
                print(f"Available columns: {list(all_data_df.columns)}")
                return []

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
            return []
            
        if not unique_values:
            print(f"No non-null values found in column '{column_name}'.")
            return []
            
        print(f"\nFound {len(unique_values)} unique values to normalize in '{column_name}':")
        for val in unique_values[:10]:  # Show first 10 values
            print(f"  - {val}")
        if len(unique_values) > 10:
            print(f"  ... and {len(unique_values) - 10} more values")

        # Create and run the normalization task
        print("\n--- Sending all terms to the agent for normalization... ---")
        print("This may take several minutes depending on the size of the list...")
        task = create_normalization_task(self.normalizer_agent, unique_values, user_rules)
        crew = Crew(
            agents=[self.normalizer_agent],
            tasks=[task],
            process=Process.sequential
        )
        
        raw_output = crew.kickoff().raw
        all_corrections = self._parse_agent_output(raw_output)

        # Filter out corrections where the original and corrected values are the same
        final_corrections = {
            original: corrected 
            for original, corrected in all_corrections.items() 
            if original != corrected
        }

        if not final_corrections:
            print("\nAgent did not propose any valid normalizations.")
            return []

        # Review proposed changes with the user
        print("\n--- Proposed Normalizations ---")
        for original, corrected in final_corrections.items():
            print(f"  - Change '{original}' to '{corrected}'")

        approval = input("\nDo you approve this plan? (yes/no): ").lower()
        if approval not in ['y', 'yes']:
            print("\nPlan rejected. No changes were made.")
            return []

        print("Executing the approved normalization plan...")
        
        # Apply corrections using the same logic as the main workflow
        # Make a copy for updates
        update_df = all_data_df.copy()
        
        # Apply corrections by string replacement
        for old_value, new_value in final_corrections.items():
            update_df[actual_column_name] = update_df[actual_column_name].astype(str).str.replace(old_value, new_value, regex=False)
            update_df[actual_column_name] = update_df[actual_column_name].replace('nan', pd.NA)
        
        # Find rows that actually changed
        original_str = all_data_df[actual_column_name].astype(str)
        updated_str = update_df[actual_column_name].astype(str)
        changed_mask = original_str != updated_str
        changed_rows = changed_mask.sum()
        
        if changed_rows == 0:
            print("No changes to apply after normalization.")
            return []
            
        print(f"Found {changed_rows} rows to update.")
        
        # Store the updated DataFrame back to Synapse
        print("Storing updated data back to Synapse...")
        
        # Only update the rows that actually changed
        changed_df = update_df[changed_mask].copy()
        print(f"Preparing to update {len(changed_df)} rows (out of {len(update_df)} total rows)")
        
        table = Table(self.view_synapse_id, changed_df)
        updated_table = self.syn.store(table)
        print("Table updated successfully!")
        
        print(f"\nCompleted normalization for column '{column_name}' in {view_name}.")
        return []  # Could add follow-up tasks here if needed