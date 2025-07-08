from crewai import Crew, Process
from src.agents.uncontrolled_vocab_normalizer import get_uncontrolled_vocab_normalizer_agent
from src.agents.synapse_agent import get_synapse_agent
from src.agents.github_issue_filer import GitHubIssueFilerAgent
from src.tasks.normalization_tasks import create_normalization_task, create_synapse_update_task
from tqdm import tqdm
import concurrent.futures
import difflib
import re
import time
import synapseclient
import pandas as pd
import json
from src.utils.cli_utils import prompt_for_view_and_column

class UncontrolledVocabNormalizationWorkflow:
    def __init__(self, syn, llm, views, orchestrator=None):
        self.syn = syn
        self.llm = llm
        self.views = views
        self.orchestrator = orchestrator
        self.normalizer_agent = get_uncontrolled_vocab_normalizer_agent(llm=self.llm)
        self.synapse_agent = get_synapse_agent(llm=self.llm, syn=self.syn)


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
        
        view_synapse_id, column_name, row_id_col = prompt_for_view_and_column(self.syn, self.views)
        if not view_synapse_id:
            return # User cancelled

        self.view_synapse_id = view_synapse_id
        
        print(f"\n--- Normalizing column: '{column_name}' ---")

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

        # 4. Review proposed changes individually with the user
        print("\n--- Reviewing Proposed Normalizations ---")
        approved_corrections = {}
        
        for original, corrected in final_corrections.items():
            self._review_proposed_normalization(column_name, original, corrected, approved_corrections)

        if not approved_corrections:
            print("\nNo normalizations were approved. Exiting workflow.")
            return

        print(f"\nExecuting the approved normalization plan with {len(approved_corrections)} changes...")

        # Following Synapse documentation pattern for updating table data
        # We already have the data in all_data_df from the earlier query
        print("Applying corrections to the data...")
        
        # Make a copy for updates
        update_df = all_data_df.copy()
        
        # Apply corrections using JSON-aware replacement
        try:
            update_df[actual_column_name] = update_df[actual_column_name].apply(
                lambda x: self._apply_corrections_to_cell(x, approved_corrections)
            )
        except Exception as e:
            print(f"Error applying corrections: {e}")
            return
        
        # Find rows that actually changed by comparing string representations
        try:
            original_str = all_data_df[actual_column_name].astype(str)
            updated_str = update_df[actual_column_name].astype(str)
            changed_mask = original_str != updated_str
            changed_rows = changed_mask.sum()
        except Exception as e:
            print(f"Error comparing values: {e}")
            # Fallback: assume all rows changed
            changed_mask = pd.Series([True] * len(update_df))
            changed_rows = len(update_df)
        
        if changed_rows == 0:
            print("No changes to apply after normalization.")
            return
            
        print(f"Found {changed_rows} rows to update.")
        
        # Store the updated DataFrame back to Synapse
        print("Storing updated data back to Synapse...")
        
        # Only update the rows that actually changed
        changed_df = update_df[changed_mask].copy()
        print(f"Preparing to update {len(changed_df)} rows (out of {len(update_df)} total rows)")
        
        # Use synapse_agent to handle the table update
        print(f"Using Synapse agent to update {len(changed_df)} rows...")
        
        # The synapse_agent will handle proper column selection and etag management
        update_result = self.synapse_agent.tools[1]._run(  # UpdateTableTool
            table_id=self.view_synapse_id,
            updates_df=changed_df
        )
        print(f"Synapse agent result: {update_result}")

        # Ask user if they want to propagate changes to other sources
        propagate_prompt = "\nDo you want to create a GitHub issue with these normalizations? (yes/no): "
        propagate = input(propagate_prompt).lower()
        if propagate in ['y', 'yes']:
            source = input("Please specify the data source (e.g., GitHub repository URL): ")
            if "github.com" in source:
                try:
                    # Initialize the GitHub issue filer agent
                    github_agent = GitHubIssueFilerAgent()
                    
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
                        corrections_with_ids = []
                        for original, corrected in approved_corrections.items():
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
                            title = f"Vocabulary Normalizations for column '{column_name}'"
                            body = self._create_github_issue_body(corrections_with_ids, column_name)
                            issue_url = github_agent.file_issue(title, body, source)
                            if issue_url:
                                print(f"✅ Created GitHub issue for vocabulary normalizations: {issue_url}")
                
                except Exception as e:
                    print(f"❌ Error creating GitHub issue: {e}")
                    print("You may need to check your GitHub CLI authentication or repository access.")

        return []

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

    def _review_proposed_normalization(self, column_name, original, corrected, approved_corrections):
        """
        Reviews a single proposed normalization with the user.
        Provides options to accept, reject, provide different value, or consult ontology expert.
        """
        while True:
            print(f"\nAgent suggests changing '{original}' to '{corrected}'")
            
            prompt = "Accept (a), provide different value (d), ask ontology expert (o), or reject (r)? "
            action = input(prompt).lower().strip()

            if action == 'a':
                approved_corrections[original] = corrected
                print(f"    Accepted: '{original}' → '{corrected}'")
                break
            elif action == 'r':
                print(f"    Rejected suggestion for '{original}'.")
                break
            elif action == 'd':
                new_val_manual = input(f"    Enter the correct value for '{original}': ").strip()
                if new_val_manual:
                    approved_corrections[original] = new_val_manual
                    print(f"    Will correct '{original}' to '{new_val_manual}'.")
                else:
                    print("    No value provided, skipping.")
                break
            elif action == 'o':
                suggestion = self._get_expert_suggestion(column_name, original)
                if suggestion:
                    print(f"    🎓 Expert suggests: '{suggestion['new_value']}' (URI: {suggestion.get('uri', 'N/A')})")
                    if input("    Accept expert's suggestion? (y/n): ").lower() == 'y':
                        approved_corrections[original] = suggestion['new_value']
                        print(f"    Accepted: '{original}' → '{suggestion['new_value']}'")
                else:
                    print("    Expert could not provide a suggestion.")
                break
            else:
                print("    Invalid action. Please enter 'a', 'd', 'o', or 'r'.")

    def _get_expert_suggestion(self, column_name, value):
        """Gets a suggestion from the ontology expert."""
        if not self.orchestrator:
            print("    Error: Orchestrator not available for expert consultation.")
            return None
        
        suggestion = self.orchestrator._consult_ontology_expert(column_name, value)
        return suggestion

    def _apply_corrections_to_cell(self, cell_value, corrections):
        """
        Apply corrections to a cell value, handling strings, lists, and JSON arrays.
        
        Args:
            cell_value: The original cell value (could be string, list, or JSON string)
            corrections: Dictionary of {original: corrected} values
            
        Returns:
            The corrected cell value in the same format as the input
        """
        try:
            # Handle NaN/None values
            if pd.isna(cell_value) or cell_value is None:
                return cell_value
            
            # Handle actual Python lists directly
            if isinstance(cell_value, list):
                corrected_list = []
                for item in cell_value:
                    item_str = str(item).strip()
                    corrected_item = corrections.get(item_str, item_str)
                    corrected_list.append(corrected_item)
                return corrected_list
            
            # Convert to string for further processing
            cell_str = str(cell_value)
            
            # Handle the special case of 'nan' string
            if cell_str == 'nan':
                return pd.NA
                
            # Try to parse as JSON array
            try:
                parsed_list = json.loads(cell_str)
                if isinstance(parsed_list, list):
                    # Apply corrections to each item in the list
                    corrected_list = []
                    for item in parsed_list:
                        item_str = str(item).strip()
                        corrected_item = corrections.get(item_str, item_str)
                        corrected_list.append(corrected_item)
                    
                    # Return as JSON string (same format as input)
                    return json.dumps(corrected_list)
                    
            except (json.JSONDecodeError, TypeError):
                # Not a JSON array, treat as simple string
                pass
            
            # Handle as simple string - apply corrections
            cell_str_stripped = cell_str.strip()
            if cell_str_stripped in corrections:
                return corrections[cell_str_stripped]
            else:
                return cell_value  # Return original if no correction needed
                    
        except Exception as e:
            # Silent fallback - return original value without warning
            return cell_value

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
            
            print(f"Queried DataFrame columns: {list(all_data_df.columns)}")
            print(f"DataFrame shape: {all_data_df.shape}")

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

        # Review proposed changes individually with the user
        print("\n--- Reviewing Proposed Normalizations ---")
        approved_corrections = {}
        
        for original, corrected in final_corrections.items():
            self._review_proposed_normalization(column_name, original, corrected, approved_corrections)

        if not approved_corrections:
            print("\nNo normalizations were approved.")
            return []

        print(f"\nExecuting the approved normalization plan with {len(approved_corrections)} changes...")
        
        # Apply corrections using the same logic as the main workflow
        # Make a copy for updates
        update_df = all_data_df.copy()
        
        # Apply corrections using JSON-aware replacement
        try:
            update_df[actual_column_name] = update_df[actual_column_name].apply(
                lambda x: self._apply_corrections_to_cell(x, approved_corrections)
            )
        except Exception as e:
            print(f"Error applying corrections: {e}")
            return []
        
        # Find rows that actually changed
        try:
            original_str = all_data_df[actual_column_name].astype(str)
            updated_str = update_df[actual_column_name].astype(str)
            changed_mask = original_str != updated_str
            changed_rows = changed_mask.sum()
        except Exception as e:
            print(f"Error comparing values: {e}")
            # Fallback: assume all rows changed
            changed_mask = pd.Series([True] * len(update_df))
            changed_rows = len(update_df)
        
        if changed_rows == 0:
            print("No changes to apply after normalization.")
            return []
            
        print(f"Found {changed_rows} rows to update.")
        
        # Store the updated DataFrame back to Synapse
        print("Storing updated data back to Synapse...")
        
        # Only update the rows that actually changed
        changed_df = update_df[changed_mask].copy()
        print(f"Preparing to update {len(changed_df)} rows (out of {len(update_df)} total rows)")
        
        # Use synapse_agent to handle the table update
        print(f"Using Synapse agent to update {len(changed_df)} rows...")
        
        # The synapse_agent will handle proper column selection and etag management
        update_result = self.synapse_agent.tools[1]._run(  # UpdateTableTool
            table_id=self.view_synapse_id,
            updates_df=changed_df
        )
        print(f"Synapse agent result: {update_result}")
        
        print(f"\nCompleted normalization for column '{column_name}' in {view_name}.")
        return []  # Could add follow-up tasks here if needed

    def _create_github_issue_body(self, corrections_with_ids, column_name):
        """Create a formatted body for the GitHub issue."""
        body_lines = [
            f"## Vocabulary Normalizations for Column: {column_name}",
            "",
            f"The following normalizations were applied to the `{column_name}` column:",
            ""
        ]
        
        for correction in corrections_with_ids:
            body_lines.append(f"**Original:** {correction['original']}")
            body_lines.append(f"**Normalized:** {correction['corrected']}")
            if correction.get('study_ids'):
                body_lines.append(f"**Study IDs:** {', '.join(correction['study_ids'])}")
            body_lines.append("")
        
        return "\n".join(body_lines)