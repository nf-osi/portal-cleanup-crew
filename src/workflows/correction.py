from crewai import Agent, Crew, Process, Task
import yaml
from src.agents.annotation_corrector import get_annotation_corrector_agent
from src.utils.llm_utils import get_llm
import os
import synapseclient
import json
from tqdm import tqdm
import concurrent.futures

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
                    "4. **Develop a detailed plan to correct all the identified discrepancies. This plan should be a list of all the values that will be changed, and what they will be changed to. For values you cannot map, list them separately as 'unmappable'.**\n"
                    "5. **Present this complete plan as your final answer. Do not ask for approval, just present the plan.**"
                ),
                agent=self.corrector_agent,
                expected_output="A JSON object containing two keys: 'corrections' (a list of objects with 'current_value' and 'new_value') and 'unmappable' (a list of values the agent could not map)."
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

            final_corrections = {}
            
            if corrections:
                print("\\n--- Review Proposed Corrections ---")
                for c in corrections:
                    current_value = c['current_value']
                    suggested_value = c['new_value']
                    
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
                            unmappable.append(current_value)
                            break
                        else:
                            print("    Invalid choice. Please enter 'a', 'd', or 'r'.")

            if unmappable:
                print("\\n--- Handle Unmappable Values ---")
                print("Please provide a correction, type 'skip', or type 'null' to clear the value.")
                for value in unmappable:
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
                print("\\nNo corrections were approved or provided. Moving to the next column.")
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
                
                # --- Refactored for Parallel Execution ---
                
                # 1. Gather all updates into a single list
                updates_to_process = []
                print("Gathering all entities to update...")
                for current_val, new_val in final_corrections.items():
                    safe_current_val = str(current_val).replace("'", "''")
                    query = "SELECT id FROM " + self.view_synapse_id + " WHERE \"" + column_name + "\" = '" + safe_current_val + "'"
                    try:
                        results_df = self.syn.tableQuery(query, resultsAs="csv").asDataFrame()
                        if not results_df.empty:
                            for entity_id in results_df['id']:
                                updates_to_process.append({'entity_id': entity_id, 'new_value': new_val})
                    except Exception as e:
                        print(f"Error querying for entities to update for value '{current_val}': {e}")
                
                if not updates_to_process:
                    print("Could not find any entities to update for the approved plan.")
                    continue

                # 2. Define the worker function for a single update
                def _update_entity(update_info):
                    entity_id = update_info['entity_id']
                    new_value = update_info['new_value']
                    try:
                        entity = self.syn.get(entity_id, downloadFile=False)
                        if new_value == '':
                            if column_name in entity.annotations:
                                del entity.annotations[column_name]
                        else:
                            entity.annotations[column_name] = new_value
                        self.syn.store(entity)
                        return True
                    except Exception as e:
                        # Print error for the specific failed entity
                        print(f"\\nFailed to update entity {entity_id}: {e}")
                        return False

                # 3. Run updates in parallel with a progress bar
                successful_updates = 0
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    # Using tqdm to create a progress bar
                    results = list(tqdm(executor.map(_update_entity, updates_to_process), total=len(updates_to_process), desc=f"Updating '{column_name}'"))
                
                successful_updates = sum(results)
                failed_updates = len(results) - successful_updates

                print("\\nExecution finished.")
                print(f"Summary: {successful_updates} entities updated successfully, {failed_updates} failed.")

            else:
                print("\\nPlan for this column rejected. No changes were made.")
        
        if self.skipped_values:
            print("\\n--- Skipped Values Summary ---")
            print("The following values were skipped during the process and were not changed:")
            for column, values in self.skipped_values.items():
                print(f"  - Column '{column}': {', '.join(map(str, values))}")
        
        print("\\nAnnotation correction process finished.") 