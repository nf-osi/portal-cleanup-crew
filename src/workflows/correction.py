from crewai import Agent, Crew, Process, Task
import yaml
from ..agents.annotation_corrector import get_annotation_corrector_agent
from ..agents.github_issue_filer import GitHubIssueFilerAgent
from src.utils.llm_utils import get_llm
import os
import synapseclient
import json
import pandas as pd
from synapseclient import Table
from ..utils.synapse_utils import update_table_column_with_corrections

class CorrectionWorkflow:
    def __init__(self, syn, llm, view_synapse_id, data_model_path, orchestrator=None):
        self.syn = syn
        self.llm = llm
        self.view_synapse_id = view_synapse_id
        self.data_model_path = data_model_path
        self.orchestrator = orchestrator
        self.corrector_agent = get_annotation_corrector_agent(llm=self.llm, syn=self.syn)
        self.skipped_values = {}
        self.follow_up_tasks = []

    def _parse_corrections(self, plan_output):
        """
        Parses the correction plan from the crew's output.
        """
        try:
            if hasattr(plan_output, 'raw'):
                raw_output = plan_output.raw
            elif isinstance(plan_output, str):
                raw_output = plan_output
            else:
                raw_output = str(plan_output)

            # Extract json from markdown code block if present
            if '```json' in raw_output:
                json_str = raw_output.split('```json')[1].split('```')[0]
            else:
                json_str = raw_output
            
            plan = json.loads(json_str)
            return plan.get('corrections', []), plan.get('unmappable', [])

        except (json.JSONDecodeError, AttributeError, IndexError) as e:
            print(f"Error: Could not parse the correction plan from the agent. Details: {e}")
            print(f"Raw plan: {getattr(plan_output, 'raw', plan_output)}")
            return [], []

    def _create_plan_task(self, column_name):
        """Creates the planning task for a given column."""
        return Task(
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

    def _get_column_correction_plan(self, column_name):
        """
        Get the correction plan for a specific column without user interaction.
        This method is designed for autonomous operation.
        """
        try:
            from src.tools.jsonld_tools import JsonLdGetValidValuesTool
            jsonld_tool = JsonLdGetValidValuesTool()
            data_model_values = jsonld_tool._run(self.data_model_path, column_name)
            
            if "not found in the data model" in str(data_model_values) or "No valid values" in str(data_model_values):
                return [], []
            
            plan_task = self._create_plan_task(column_name)

            crew = Crew(
                agents=[self.corrector_agent],
                tasks=[plan_task],
                process=Process.sequential,
                verbose=True
            )
            plan_output = crew.kickoff()
            
            print(f"Agent's Raw Plan:\n{plan_output.raw}")

            corrections, unmappable = self._parse_corrections(plan_output)
            
            if unmappable and self.orchestrator:
                expert_corrections = []
                for value in unmappable:
                    expert_suggestion = self.orchestrator._consult_ontology_expert(column_name, value)
                    if expert_suggestion:
                        expert_corrections.append(expert_suggestion)
                
                # We can decide here whether to merge automatically or treat them separately
                # For now, let's merge them into the main corrections list to be reviewed
                corrections.extend(expert_corrections)
                # Clear unmappable as they are now converted to potential corrections
                unmappable = [u for u in unmappable if u not in [c['current_value'] for c in expert_corrections]]


            return corrections, unmappable
            
        except Exception as e:
            print(f"Error getting correction plan for column '{column_name}': {e}")
            return [], []

    def run(self):
        """Runs the full annotation correction workflow for a single table."""
        try:
            entity = self.syn.get(self.view_synapse_id)
            columns = [c['name'] for c in self.syn.getColumns(entity.id)]
            print(f"Found {len(columns)} columns to check in {self.view_synapse_id}.")
        except Exception as e:
            print(f"Error getting columns for view {self.view_synapse_id}: {e}")
            return []

        # Let the user choose the column
        print("\nPlease select a column to work on:")
        for i, col_name in enumerate(columns, 1):
            print(f"  {i}. {col_name}")
        print(f"  {len(columns) + 1}. All columns")

        try:
            choice = int(input("Enter the number of your choice: "))
            
            if 1 <= choice <= len(columns):
                # Run on a single selected column
                selected_columns = [columns[choice - 1]]
            elif choice == len(columns) + 1:
                # Run on all columns
                selected_columns = columns
            else:
                print("Invalid choice. Exiting workflow.")
                return []
        except ValueError:
            print("Invalid input. Please enter a number. Exiting workflow.")
            return []

        for column_name in selected_columns:
            print(f"\n--- Checking column: '{column_name}' ---")
            
            proposed_corrections, unmappable_values = self._get_column_correction_plan(column_name)

            if not proposed_corrections and not unmappable_values:
                print("No errors found.")
                continue

            # This is the interactive review step
            approved_for_column, follow_ups_for_column = self._review_and_finalize_corrections(
                column_name, 
                proposed_corrections, 
                unmappable_values
            )

            # After reviewing, confirm and apply changes for this column before moving to the next
            if approved_for_column:
                if self._confirm_and_apply_for_column(column_name, approved_for_column, follow_ups_for_column):
                    # If confirmed, add follow-ups to the main list
                    self.follow_up_tasks.extend(follow_ups_for_column)
                else:
                    print(f"Changes for column '{column_name}' were discarded.")
            else:
                print(f"No changes were approved for column '{column_name}'.")

        print("\nWorkflow execution completed for all columns.")
        
        # Check if any corrections were applied or new terms were suggested
        has_applied_corrections = hasattr(self, 'applied_corrections_by_column') and self.applied_corrections_by_column
        has_new_terms = hasattr(self, 'new_terms_for_github') and self.new_terms_for_github
        
        if has_applied_corrections or has_new_terms:
            # Ask user if they want to create GitHub issues
            propagate_prompt = "\nDo you want to create GitHub issues documenting these changes? (yes/no): "
            propagate = input(propagate_prompt).lower()
            if propagate in ['y', 'yes']:
                source = input("Please specify the GitHub repository URL: ")
                if "github.com" in source:
                    
                    try:
                        # Initialize the GitHub issue filer agent
                        github_agent = GitHubIssueFilerAgent()
                        
                        # Create issue for regular corrections if any
                        if has_applied_corrections:
                            corrections_summary = self._create_corrections_summary()
                            if corrections_summary:
                                title = f"Annotation Corrections Applied to {self.view_synapse_id}"
                                issue_url = github_agent.file_issue(title, corrections_summary, source)
                                if issue_url:
                                    print(f"✅ Created GitHub issue for annotation corrections: {issue_url}")
                        
                        # Create issues for new term proposals if any
                        if has_new_terms:
                            successful_issues = 0
                            for term_info in self.new_terms_for_github:
                                title = f"New Term Proposal for '{term_info['column_name']}': {term_info['new_term']}"
                                body = (
                                    f"The curation agent proposed adding a new term to the data model for the attribute `{term_info['column_name']}`.\n\n"
                                    f"**Proposed Term:** {term_info['new_term']}\n"
                                    f"**Proposed URI:** {term_info['uri']}\n\n"
                                    "Please review this proposal and, if appropriate, add it to the JSON-LD data model."
                                )
                                
                                issue_url = github_agent.file_issue(title, body, source)
                                if issue_url:
                                    successful_issues += 1
                            
                            print(f"✅ Created {successful_issues}/{len(self.new_terms_for_github)} GitHub issues for new term proposals.")
                    
                    except Exception as e:
                        print(f"❌ Error creating GitHub issues: {e}")
                        print("You may need to check your GitHub CLI authentication or repository access.")
        
        return self.follow_up_tasks


    def _confirm_and_apply_for_column(self, column_name, approved_corrections, follow_up_tasks):
        """Shows a summary of changes for a single column and asks for final confirmation."""
        print(f"\n--- Summary for Column: '{column_name}' ---")
        total_changes = 0
        for corr in approved_corrections:
            print(f"  - Change '{corr['current_value']}' to '{corr['new_value']}'")
            total_changes += 1
        
        if follow_up_tasks:
            print(f"  - This will also create {len(follow_up_tasks)} follow-up task(s) (e.g., new term proposals).")

        if total_changes == 0:
            print("No changes were approved.")
            return False

        confirm = input(f"\nApply {total_changes} change(s) to column '{column_name}'? (y/n): ").lower().strip()
        if confirm == 'y':
            print("Applying changes...")
            update_table_column_with_corrections(self.syn, self.view_synapse_id, column_name, approved_corrections)
            
            # Track applied corrections for GitHub issue creation
            if not hasattr(self, 'applied_corrections_by_column'):
                self.applied_corrections_by_column = {}
            self.applied_corrections_by_column[column_name] = approved_corrections
            
            return True
        else:
            return False

    def apply_annotation_corrections(self, corrections_by_column):
        """
        Apply annotation corrections to the table/view.
        This method is designed for autonomous operation.
        """
        try:
            if not corrections_by_column:
                print("No corrections to apply.")
                return True
            
            # This logic can be simplified as follow-up tasks are now handled separately
            # The GitHub issue creation is part of the follow-up task processing

            columns_to_update = list(corrections_by_column.keys())
            
            for column_name in columns_to_update:
                update_table_column_with_corrections(self.syn, self.view_synapse_id, column_name, corrections_by_column[column_name])

            print("\nAll corrections applied successfully.")
            return True

        except Exception as e:
            print(f"An error occurred during the application of corrections: {e}")
            return False

    def _review_and_finalize_corrections(self, column_name, proposed_corrections, unmappable_values):
        final_approved = []
        follow_up_tasks = []

        self._review_proposed_corrections(column_name, proposed_corrections, final_approved, follow_up_tasks)
        self._handle_unmappable_values(column_name, unmappable_values, final_approved, follow_up_tasks)
        
        return final_approved, follow_up_tasks

    def _review_proposed_corrections(self, column_name, corrections, approved_corrections, follow_up_tasks):
        if not corrections:
            return

        print("\n--- Review Proposed Corrections ---")

        for correction in corrections:
            while True:
                current_value = correction['current_value']
                new_value = correction['new_value']
                confidence = correction.get('confidence', 0)
                
                prompt = (f"\n- Agent suggests changing '{current_value}' to '{new_value}' (Confidence: {confidence:.2f}).\n"
                          "  Accept (a), provide a different value (d), ask ontology expert agent for a suggestion (o) or reject this correction (r)? ")
                
                action = input(prompt).lower().strip()

                if action == 'a':
                    approved_corrections.append(correction)
                    print(f"    Accepted: '{current_value}' → '{new_value}'")
                    if correction.get('is_new_term'):
                        task = self._create_new_term_issue(column_name, new_value, correction.get('uri'))
                        if task:
                            follow_up_tasks.append(task)
                    break
                elif action == 'r':
                    self.skipped_values.setdefault(column_name, []).append(current_value)
                    print(f"    Rejected suggestion for '{current_value}'.")
                    break
                elif action == 'd':
                    new_val_manual = input(f"    Enter the correct value for '{current_value}': ").strip()
                    if new_val_manual:
                        approved_corrections.append({'current_value': current_value, 'new_value': new_val_manual})
                        print(f"    Will correct '{current_value}' to '{new_val_manual}'.")
                        
                        # After getting manual correction, check if it's a new term
                        if self._is_new_term(column_name, new_val_manual):
                            if input(f"    '{new_val_manual}' seems to be a new term. Propose adding it to the data model? (y/n): ").lower() == 'y':
                                task = self._create_new_term_issue(column_name, new_val_manual, "N/A (user-provided)")
                                if task:
                                    follow_up_tasks.append(task)
                    else:
                        print("    No value provided, skipping.")
                    break
                elif action == 'o': # Ask expert
                    suggestion = self._get_expert_suggestion(column_name, current_value)
                    if suggestion:
                        print(f"    🎓 Expert suggests: '{suggestion['new_value']}' (URI: {suggestion.get('uri', 'N/A')})")
                        if input("    Accept expert's suggestion? (y/n): ").lower() == 'y':
                            approved_corrections.append(suggestion)
                            print(f"    Accepted: '{current_value}' → '{suggestion['new_value']}'")
                            if suggestion.get('is_new_term'):
                                task = self._create_new_term_issue(column_name, suggestion['new_value'], suggestion.get('uri'))
                                if task:
                                    follow_up_tasks.append(task)
                    else:
                        print("    Expert could not provide a suggestion.")
                    break
                else:
                    print("    Invalid action.")

    def _get_expert_suggestion(self, column_name, value):
        """Gets a suggestion from the ontology expert, but does not handle user interaction."""
        if not self.orchestrator:
            print("    Error: Orchestrator not available for expert consultation.")
            return None
        
        suggestion = self.orchestrator._consult_ontology_expert(column_name, value)
        return suggestion


    def _handle_unmappable_values(self, column_name, unmappable_values, approved_corrections, follow_up_tasks):
        """
        Interactively handles values that the agent could not map.
        """
        if not unmappable_values:
            return

        print("\n--- Handle Unmappable Values ---")
        print("For each value, provide a correction, or type:\n"
              "  'o' to ask the Ontology Expert for a suggestion\n"
              "  'skip' to do nothing\n"
              "  'null' to clear the value")

        for value in unmappable_values:
            while True:
                action = input(f"  - How should '{value}' be corrected? ").strip()

                if action.lower() == 'skip':
                    print(f"    Skipped '{value}'.")
                    self.skipped_values.setdefault(column_name, []).append(value)
                    break
                elif action.lower() == 'null':
                    approved_corrections.append({'current_value': value, 'new_value': ''})
                    print(f"    Will clear values of '{value}'.")
                    break
                elif action.lower() == 'o':
                    suggestion = self._get_expert_suggestion(column_name, value)
                    if suggestion:
                        print(f"    🎓 Expert suggests: '{suggestion['new_value']}' (URI: {suggestion.get('uri', 'N/A')})")
                        if input("    Accept this suggestion? (y/n): ").lower() == 'y':
                            approved_corrections.append(suggestion)
                            print(f"    Accepted suggestion for '{value}'.")
                            if suggestion.get('is_new_term'):
                                task = self._create_new_term_issue(column_name, suggestion['new_value'], suggestion.get('uri'))
                                if task:
                                    follow_up_tasks.append(task)
                    else:
                        print("    Expert could not provide a suggestion.")
                    break
                elif action:
                    approved_corrections.append({'current_value': value, 'new_value': action})
                    print(f"    Will correct '{value}' to '{action}'.")
                    
                    # After getting manual correction, check if it's a new term
                    if self._is_new_term(column_name, action):
                        if input(f"    '{action}' seems to be a new term. Propose adding it to the data model? (y/n): ").lower() == 'y':
                            task = self._create_new_term_issue(column_name, action, "N/A (user-provided)")
                            if task:
                                follow_up_tasks.append(task)
                    break
                else:
                    print("    Invalid action.")

    def _is_new_term(self, column_name, value_to_check):
        """
        Checks if a value is new for a given column by comparing against the data model.
        This now primarily checks string values, and the expert handles URIs.
        """
        from src.tools.jsonld_tools import JsonLdGetValidValuesTool
        jsonld_tool = JsonLdGetValidValuesTool()
        valid_values = jsonld_tool._run(self.data_model_path, column_name)
        
        if isinstance(valid_values, list) and valid_values:
            # Handle case where valid values are dictionaries (with URIs)
            if isinstance(valid_values[0], dict):
                string_values = [v.get('name') for v in valid_values if v.get('name')]
                return value_to_check not in string_values
            # Handle case where valid values are just strings
            else:
                return value_to_check not in valid_values
        return True # It's new if the data model has no values for it

    def _create_new_term_issue(self, column_name, new_term, uri):
        """Creates a GitHub issue follow-up task for a new term proposal."""
        # Store new term for later GitHub issue creation (will prompt user for repo)
        if not hasattr(self, 'new_terms_for_github'):
            self.new_terms_for_github = []
        
        self.new_terms_for_github.append({
            'column_name': column_name,
            'new_term': new_term,
            'uri': uri
        })
        
        print(f"    -> Will create a follow-up task to propose adding '{new_term}' to the data model.")
        return None  # We'll create the actual task later when we have the repo URL

    def _apply_corrections(self, column_name, corrections_to_apply):
        """Applies a list of corrections to a given column in the Synapse table."""
        if not corrections_to_apply:
            return
        
        print(f"\nApplying {len(corrections_to_apply)} corrections to column '{column_name}'...")
        update_table_column_with_corrections(
            self.syn, 
            self.view_synapse_id, 
            column_name, 
            corrections_to_apply
        )

    def _create_corrections_summary(self):
        """Create a formatted summary of all corrections for GitHub issue body."""
        if not hasattr(self, 'applied_corrections_by_column') or not self.applied_corrections_by_column:
            return None
            
        body_lines = [
            f"## Annotation Corrections Applied to {self.view_synapse_id}",
            "",
            "The following annotation corrections were applied to fix invalid values based on the data model:",
            ""
        ]
        
        total_corrections = 0
        for column_name, corrections in self.applied_corrections_by_column.items():
            if corrections:
                body_lines.append(f"### Column: `{column_name}`")
                body_lines.append("")
                for correction in corrections:
                    body_lines.append(f"- **Changed:** `{correction['current_value']}` → `{correction['new_value']}`")
                    total_corrections += 1
                body_lines.append("")
        
        body_lines.insert(3, f"**Total corrections applied:** {total_corrections}")
        body_lines.insert(4, "")
        
        return "\n".join(body_lines)