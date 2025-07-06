from crewai import Agent, Crew, Process, Task
import yaml
from ..agents.annotation_corrector import get_annotation_corrector_agent
from src.utils.llm_utils import get_llm
import os
import synapseclient
import json
import pandas as pd
from synapseclient import Table

class CorrectionWorkflow:
    def __init__(self, syn, llm, view_synapse_id, data_model_path):
        self.syn = syn
        self.llm = llm
        self.view_synapse_id = view_synapse_id
        self.data_model_path = data_model_path
        self.corrector_agent = get_annotation_corrector_agent(llm=self.llm, syn=self.syn)
        self.skipped_values = {}

    def _parse_corrections(self, plan_output):
        """
        Parses the correction plan from the crew's output.
        """
        try:
            # The crew output object may have the JSON directly
            if plan_output.json_dict:
                plan = plan_output.json_dict
                return plan.get('corrections', []), plan.get('unmappable', [])
            
            # Fallback to parsing the raw string output
            plan_raw = plan_output.raw
            if '```json' in plan_raw:
                plan_raw = plan_raw.split('```json')[1].split('```')[0]
            
            plan = json.loads(plan_raw)
            return plan.get('corrections', []), plan.get('unmappable', [])

        except (json.JSONDecodeError, AttributeError, IndexError) as e:
            print(f"Error: Could not parse the correction plan from the agent. Details: {e}")
            print(f"Raw plan: {getattr(plan_output, 'raw', plan_output)}")
            return [], []

    def _get_column_correction_plan(self, column_name):
        """
        Get the correction plan for a specific column without user interaction.
        This method is designed for autonomous operation.
        
        Args:
            column_name: Name of the column to analyze
            
        Returns:
            Dict with 'corrections' and 'unmappable' keys, or None if no plan generated
        """
        try:
            # First, do a quick check if this column exists in the data model
            # to avoid running the full agent workflow unnecessarily
            from src.tools.jsonld_tools import JsonLdGetValidValuesTool
            jsonld_tool = JsonLdGetValidValuesTool()
            data_model_values = jsonld_tool._run(self.data_model_path, column_name)
            
            # If the column is not in the data model, return empty result immediately
            if "not found in the data model" in str(data_model_values) or "No valid values" in str(data_model_values):
                return {
                    'corrections': [],
                    'unmappable': [],
                    'raw_plan': f"Column '{column_name}' not found in data model - skipped"
                }
            
            print(f"🔍 DEBUG: Data model values for '{column_name}': {data_model_values}")
            
            # If we get here, the column has controlled vocabulary, so proceed with full analysis
            
            # Create the planning task for this column
            plan_task = Task(
                description=(
                    f"Autonomously find all invalid annotations in the column '{column_name}' of the Synapse view '{self.view_synapse_id}' based on the data model at '{self.data_model_path}'.\n"
                    "To do this, you MUST use the tools available to you. An authenticated 'synapseclient' instance is available as the 'syn' object within the 'Synapse Python Code Executor Tool'.\n\n"
                    "Follow these steps methodically:\n"
                    f"1. Get the list of valid values for the column's attribute name from the JSON-LD data model using the 'JSON-LD Get Valid Values Tool'. The `attribute_name` is '{column_name}'. If the tool returns an error that the attribute is not found, you can assume that this column does not have controlled vocabulary and you should stop.\n"
                    f'2. Get the unique values for the column from the Synapse view by executing a query like: `result = syn.tableQuery(f"SELECT DISTINCT \\"{column_name}\\" FROM {self.view_synapse_id} WHERE \\"{column_name}\\" IS NOT NULL").asDataFrame()`. \n'
                    "3. Compare the unique values from the view with the valid values from the data model. Identify any discrepancies, including typos, capitalization issues, or values that need to be mapped to a different term in the data model.\n"
                    "4. **For each correction, provide a confidence score from 0.0 to 1.0 indicating how certain you are about the correction:**\n"
                    "   - 1.0 = Completely certain (exact match, obvious typo fix, clear mapping)\n"
                    "   - 0.8-0.9 = Very confident (minor differences, standard mappings)\n"
                    "   - 0.6-0.7 = Moderately confident (reasonable interpretation)\n"
                    "   - 0.4-0.5 = Low confidence (ambiguous, multiple possible mappings)\n"
                    "   - 0.0-0.3 = Very uncertain (unclear how to map)\n"
                    "5. **Present a complete plan with corrections and confidence scores. Do not ask for approval, just present the plan.**"
                ),
                agent=self.corrector_agent,
                expected_output="A JSON object containing: 'corrections' (list of objects with 'current_value', 'new_value', and 'confidence'), and 'unmappable' (list of values that cannot be mapped)."
            )

            # Create and run the crew
            crew = Crew(
                agents=[self.corrector_agent],
                tasks=[plan_task],
                process=Process.sequential,
                verbose=False  # Reduce verbosity for autonomous operation
            )
            plan_output = crew.kickoff()
            
            # Parse the plan
            corrections, unmappable = self._parse_corrections(plan_output)
            
            return {
                'corrections': corrections,
                'unmappable': unmappable,
                'raw_plan': plan_output.raw
            }
            
        except Exception as e:
            print(f"Error getting correction plan for column '{column_name}': {e}")
            return None

    def apply_annotation_corrections(self, corrections_by_column):
        """
        Apply annotation corrections to the table/view.
        This method is designed for autonomous operation.
        
        Args:
            corrections_by_column: Dict mapping column names to lists of corrections
            
        Returns:
            Boolean indicating success
        """
        try:
            if not corrections_by_column:
                print("No corrections to apply.")
                return True
            
            # Query the table to get all data
            columns_to_query = list(corrections_by_column.keys())
            columns_str = ', '.join([f'"{col}"' for col in columns_to_query])
            
            query = f'SELECT id, {columns_str} FROM {self.view_synapse_id}'
            query_results = self.syn.tableQuery(query, resultsAs="csv", includeRowIdAndRowVersion=True)
            all_data_df = query_results.asDataFrame()
            
            # Find actual column names (Synapse may modify them)
            actual_id_col = next((c for c in all_data_df.columns if c.lower() == 'id'), None)
            if not actual_id_col:
                print("Error: Could not find 'id' column in query result")
                return False
            
            # Apply corrections to the DataFrame
            update_df = all_data_df.copy()
            total_changes = 0
            
            for column_name, corrections in corrections_by_column.items():
                actual_column_name = next((c for c in all_data_df.columns if c.lower() == column_name.lower()), None)
                if not actual_column_name:
                    print(f"Warning: Could not find column '{column_name}' in query result")
                    continue
                
                print(f"Applying {len(corrections)} corrections to column '{column_name}'...")
                
                for correction in corrections:
                    old_value = correction['current_value']
                    new_value = correction['new_value']
                    
                    if new_value == '':
                        # Set to NaN for null values
                        update_df.loc[update_df[actual_column_name] == old_value, actual_column_name] = pd.NA
                    else:
                        # Replace with new value
                        update_df.loc[update_df[actual_column_name] == old_value, actual_column_name] = new_value
                    
                    total_changes += 1
            
            # Find rows that actually changed
            changed_mask = pd.Series([False] * len(update_df))
            for column_name in corrections_by_column.keys():
                actual_column_name = next((c for c in all_data_df.columns if c.lower() == column_name.lower()), None)
                if actual_column_name:
                    original_str = all_data_df[actual_column_name].astype(str)
                    updated_str = update_df[actual_column_name].astype(str)
                    changed_mask = changed_mask | (original_str != updated_str)
            
            changed_rows = changed_mask.sum()
            
            if changed_rows == 0:
                print("No changes to apply after corrections.")
                return True
                
            print(f"Found {changed_rows} rows to update.")
            
            # Store only the changed rows back to Synapse
            print("Storing updated data back to Synapse...")
            changed_df = update_df[changed_mask].copy()
            print(f"Preparing to update {len(changed_df)} rows (out of {len(update_df)} total rows)")
            
            table = Table(self.view_synapse_id, changed_df)
            updated_table = self.syn.store(table)
            print("Table updated successfully!")
            print(f"Summary: {changed_rows} entities updated successfully.")
            
            return True
            
        except Exception as e:
            print(f"Error applying annotation corrections: {e}")
            return False

    def run(self):
        # Implementation from _handle_annotation_correction will go here
        view = self.syn.get(self.view_synapse_id)
        columns = [c['name'] for c in self.syn.getColumns(view)]
        print(f"Found {len(columns)} columns to check in {self.view_synapse_id}.")
        data_model_path = self.data_model_path

        for column_name in columns:
            print(f"\\n--- Checking column: '{column_name}' ---")
            
            # Stage 1: Planning
            plan_task = Task(
                description=(
                    f"Autonomously find all invalid annotations in the column '{column_name}' of the Synapse view '{self.view_synapse_id}' based on the data model at '{data_model_path}'.\n"
                    "To do this, you MUST use the tools available to you. An authenticated 'synapseclient' instance is available as the 'syn' object within the 'Synapse Python Code Executor Tool'.\n\n"
                    "Follow these steps methodically:\n"
                    f"1. Get the list of valid values for the column's attribute name from the JSON-LD data model using the 'JSON-LD Get Valid Values Tool'. The `attribute_name` is '{column_name}'. If the tool returns an error that the attribute is not found, you can assume that this column does not have controlled vocabulary and you should stop.\n"
                    f'2. Get the unique values for the column from the Synapse view by executing a query like: `result = syn.tableQuery(f"SELECT DISTINCT \\"{column_name}\\" FROM {self.view_synapse_id} WHERE \\"{column_name}\\" IS NOT NULL").asDataFrame()`. \n'
                    "3. Compare the unique values from the view with the valid values from the data model. Identify any discrepancies, including typos, capitalization issues, or values that need to be mapped to a different term in the data model.\n"
                    "4. **For each correction, provide a confidence score from 0.0 to 1.0 indicating how certain you are about the correction:**\n"
                    "   - 1.0 = Completely certain (exact match, obvious typo fix, clear mapping)\n"
                    "   - 0.8-0.9 = Very confident (minor differences, standard mappings)\n"
                    "   - 0.6-0.7 = Moderately confident (reasonable interpretation)\n"
                    "   - 0.4-0.5 = Low confidence (ambiguous, multiple possible mappings)\n"
                    "   - 0.0-0.3 = Very uncertain (unclear how to map)\n"
                    "5. **Present a complete plan with corrections and confidence scores. Do not ask for approval, just present the plan.**"
                ),
                agent=self.corrector_agent,
                expected_output="A JSON object containing: 'corrections' (list of objects with 'current_value', 'new_value', and 'confidence'), and 'unmappable' (list of values that cannot be mapped)."
            )

            crew = Crew(
                agents=[self.corrector_agent],
                tasks=[plan_task],
                process=Process.sequential,
                verbose=True
            )
            plan_output = crew.kickoff()
            
            print(f"\\nAgent's Raw Plan:\\n{plan_output.raw}")

            # Stage 2: Plan Review and Interactive Correction
            corrections, unmappable = self._parse_corrections(plan_output)
            
            if not corrections and not unmappable:
                print("No errors found.")
                continue

            # Deduplicate corrections - each unique correction should only be shown once
            unique_corrections = {}
            for c in corrections:
                current_value = c['current_value']
                suggested_value = c['new_value']
                unique_corrections[current_value] = suggested_value
            
            # Deduplicate unmappable values
            unique_unmappable = list(set(unmappable))

            # Show all proposed corrections at once
            final_corrections = {}
            
            if unique_corrections:
                print("\\n--- Review Proposed Corrections ---")
                for current_value, suggested_value in unique_corrections.items():
                    prompt = (
                        f"  - Agent suggests changing '{current_value}' to '{suggested_value}'.\\n"
                        f"    Accept (a), provide a different value (d), or reject this correction (r)? "
                    )
                    
                    while True:
                        choice = input(prompt).lower()
                        if choice == 'a':
                            final_corrections[current_value] = suggested_value
                            break
                        elif choice == 'd':
                            new_correction = input(f"    Enter the correct value for '{current_value}': ")
                            final_corrections[current_value] = new_correction
                            break
                        elif choice == 'r':
                            unique_unmappable.append(current_value)
                            break
                        else:
                            print("    Invalid choice. Please enter 'a', 'd', or 'r'.")

            if unique_unmappable:
                print("\\n--- Unmappable Values Found ---")
                print("The following values could not be automatically mapped to the data model:")
                for value in unique_unmappable:
                    print(f"- {value}")
                print("\\nYou can provide corrections for these in the next step.")

            # Handle unmappable values if any
            if unique_unmappable:
                print("\\n--- Handle Unmappable Values ---")
                print("Please provide a correction, type 'skip', or type 'null' to clear the value.")
                for value in unique_unmappable:
                    correction = input(f"  - How should '{value}' be corrected? ")
                    if correction.lower() == 'skip':
                        if column_name not in self.skipped_values:
                            self.skipped_values[column_name] = []
                        self.skipped_values[column_name].append(value)
                    elif correction.lower() == 'null':
                        final_corrections[value] = '' # Use empty string for null
                    else:
                        final_corrections[value] = correction

            if not final_corrections:
                print("\\nNo corrections to apply. Moving to the next column.")
                continue

            # Stage 3: Final Approval
            print("\\n--- Final Plan ---")
            if final_corrections:
                print("The following corrections will be applied:")
                for current, new in final_corrections.items():
                    if new == '':
                        print(f"  - Remove annotation for '{current}'")
                    else:
                        print(f"  - Change '{current}' to '{new}'")
            
            approval = input("\\nDo you approve this plan? (yes/no): ").lower()
            if approval in ['y', 'yes']:
                print("\\nExecuting the approved correction plan...")
                
                # Use table/view update approach instead of individual entity updates
                print("Applying corrections to the table data...")
                
                # 1. Query the table to get all data for this column
                try:
                    query = f'SELECT id, "{column_name}" FROM {self.view_synapse_id} WHERE "{column_name}" IS NOT NULL'
                    query_results = self.syn.tableQuery(query, resultsAs="csv", includeRowIdAndRowVersion=True)
                    all_data_df = query_results.asDataFrame()
                    
                    # Find actual column names (Synapse may modify them)
                    actual_id_col = next((c for c in all_data_df.columns if c.lower() == 'id'), None)
                    actual_column_name = next((c for c in all_data_df.columns if c.lower() == column_name.lower()), None)
                    
                    if not actual_id_col or not actual_column_name:
                        print(f"Error: Could not find columns 'id' or '{column_name}' in query result")
                        print(f"Available columns: {list(all_data_df.columns)}")
                        continue
                        
                except Exception as e:
                    print(f"Error querying table data: {e}")
                    continue
                
                # 2. Apply corrections to the DataFrame
                update_df = all_data_df.copy()
                
                for old_value, new_value in final_corrections.items():
                    if new_value == '':
                        # Set to NaN for null values
                        update_df.loc[update_df[actual_column_name] == old_value, actual_column_name] = pd.NA
                    else:
                        # Replace with new value
                        update_df.loc[update_df[actual_column_name] == old_value, actual_column_name] = new_value
                
                # 3. Find rows that actually changed
                original_str = all_data_df[actual_column_name].astype(str)
                updated_str = update_df[actual_column_name].astype(str)
                changed_mask = original_str != updated_str
                changed_rows = changed_mask.sum()
                
                if changed_rows == 0:
                    print("No changes to apply after corrections.")
                    continue
                    
                print(f"Found {changed_rows} rows to update.")
                
                # 4. Store only the changed rows back to Synapse
                print("Storing updated data back to Synapse...")
                changed_df = update_df[changed_mask].copy()
                print(f"Preparing to update {len(changed_df)} rows (out of {len(update_df)} total rows)")
                
                try:
                    table = Table(self.view_synapse_id, changed_df)
                    updated_table = self.syn.store(table)
                    print("Table updated successfully!")
                    print(f"Summary: {changed_rows} entities updated successfully, 0 failed.")
                except Exception as e:
                    print(f"Update failed: {e}")
                    print("Some updates may not have been applied.")

            else:
                print("\\nPlan for this column rejected. No changes were made.")
        
        if self.skipped_values:
            print("\\n--- Skipped Values Summary ---")
            print("The following values were skipped during the process and were not changed:")
            for column, values in self.skipped_values.items():
                print(f"  - Column '{column}': {', '.join(map(str, values))}")
        
        print("\\nAnnotation correction process finished.") 