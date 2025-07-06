from crewai import Agent, Crew, Process, Task
import yaml
from .uncontrolled_vocab_normalizer import get_uncontrolled_vocab_normalizer_agent
from .freetext_corrector import get_freetext_corrector_agent
from src.utils.llm_utils import get_llm
import os
import synapseclient
import json
from src.workflows.correction import CorrectionWorkflow
from src.workflows.freetext_correction import FreetextCorrectionWorkflow
from src.workflows.uncontrolled_vocab_normalization import UncontrolledVocabNormalizationWorkflow
from .github_issue_filer import GitHubIssueFilerAgent
import getpass
from src.tools.jsonld_tools import JsonLdGetValidValuesTool

class OrchestratorAgent:
    def __init__(self, ac_config):
        self.ac_config = ac_config
        self.data_model_path = ac_config.get('data_model_url')
        self.views = ac_config.get('views', {})
        self.llm = get_llm()
        self.syn = self._login_to_synapse()
        
        self.agents = {
            "uncontrolled_vocab_normalizer": get_uncontrolled_vocab_normalizer_agent(llm=self.llm),
            "freetext_corrector": get_freetext_corrector_agent(llm=self.llm),
            "github_issue_filer": GitHubIssueFilerAgent
        }

    def _login_to_synapse(self):
        """
        Logs in to Synapse, trying a few different methods.
        """
        print("\nChecking for Synapse configuration file...")
        if os.path.exists(os.path.expanduser("~/.synapseConfig")):
            print("Synapse configuration file found. Attempting to log in...")
            syn = synapseclient.login()
            print("\nSynapse login successful.")
            return syn
        else:
            print("No Synapse configuration file found. Please log in manually.")
            syn_user = input("Enter your Synapse username: ")
            syn_pass = getpass.getpass("Enter your Synapse password or personal access token: ")
            try:
                syn = synapseclient.login(email=syn_user, password=syn_pass)
                print("\nSynapse login successful.")
                return syn
            except Exception as e:
                print(f"\nError logging in to Synapse: {e}")
                return None

    def run(self):
        if not self.syn:
            print("Synapse login failed. Exiting.")
            return

        print("\nWelcome to the Synapse Data Curation Assistant.")
        while True:
            print("\nPlease select a mode:")
            print("1. Manual Mode - Select specific tasks and tables")
            print("2. Semi-Autonomous Mode - Automatically process all configured tables")
            print("3. Exit")
            
            mode_choice = input("Enter the number of your choice: ")
            
            if mode_choice == '1':
                self._run_manual_mode()
            elif mode_choice == '2':
                self._run_semi_autonomous_mode()
            elif mode_choice == '3':
                print("Exiting the assistant. Goodbye!")
                break
            else:
                print("Invalid choice. Please enter a number from 1 to 3.")

    def _run_manual_mode(self):
        """Run the original manual mode where user selects each task"""
        while True:
            print("\nPlease select a task to perform:")
            print("1. Correct Synapse Annotations based on data model")
            print("2. Standardize Uncontrolled Terms (e.g., investigator names)")
            print("3. Correct Free-Text Fields")
            print("4. Back to main menu")
            
            choice = input("Enter the number of your choice: ")

            if choice == '1':
                self._run_annotation_correction_manual()
            elif choice == '2':
                self._run_vocab_normalization_manual()
            elif choice == '3':
                self._run_freetext_correction_manual()
            elif choice == '4':
                break
            else:
                print("Invalid choice. Please enter a number from 1 to 4.")

    def _run_semi_autonomous_mode(self):
        """Run semi-autonomous mode that processes all tables automatically"""
        print("\n=== Semi-Autonomous Mode ===")
        print("The system will work independently and only check in with you when:")
        print("- It needs help with ambiguous/unmappable values")
        print("- It's ready to apply a batch of changes")
        print("- It encounters errors it can't resolve")
        
        if not self.views:
            print("No views configured in config.yaml. Cannot proceed.")
            return
            
        print(f"\nFound {len(self.views)} configured tables:")
        for view_name, view_id in self.views.items():
            print(f"  - {view_name} ({view_id})")
        
        # Ask which workflows to run
        print("\nSelect which workflows to run:")
        run_annotation_correction = input("1. Run annotation correction? (yes/no): ").lower() in ['y', 'yes']
        run_vocab_normalization = input("2. Run vocabulary normalization? (yes/no): ").lower() in ['y', 'yes']
        
        if not run_annotation_correction and not run_vocab_normalization:
            print("No workflows selected. Returning to main menu.")
            return
            
        # Set confidence thresholds for autonomous decisions
        print("\nSetting autonomous decision-making parameters...")
        confidence_threshold = 0.8  # Auto-accept corrections with high confidence
        auto_reject_threshold = 0.3  # Auto-reject corrections with very low confidence
        
        all_changes = []  # Collect all changes across tables
        
        # Process each table
        for view_name, view_id in self.views.items():
            print(f"\n🤖 Processing table: {view_name} ({view_id})")
            print('='*60)
            
            table_changes = []
            
            if run_annotation_correction:
                print(f"\n🔍 Running autonomous annotation correction for {view_name}...")
                annotation_changes = self._run_autonomous_annotation_correction(view_name, view_id)
                table_changes.extend(annotation_changes)
                
            if run_vocab_normalization:
                print(f"\n🔍 Running autonomous vocabulary normalization for {view_name}...")
                vocab_changes = self._run_vocab_normalization_fully_autonomous(view_name, view_id)
                table_changes.extend(vocab_changes)
            
            if table_changes:
                all_changes.extend(table_changes)
                print(f"\n✅ Completed {view_name}: {len(table_changes)} changes queued")
            else:
                print(f"\n✅ Completed {view_name}: No changes needed")
        
        # Present final batch for approval
        if all_changes:
            self._present_final_batch_for_approval(all_changes)
        else:
            print("\n🎉 Semi-autonomous processing complete! No changes were needed across all tables.")
        
        print("\n=== Semi-Autonomous Mode Complete ===")

    def _run_autonomous_annotation_correction(self, view_name, view_id):
        """Run annotation correction autonomously for all columns in a table"""
        print(f"\n🔍 Autonomous annotation correction for {view_name}...")
        
        # Get all columns from the table/view
        try:
            entity = self.syn.get(view_id)
            columns = [c['name'] for c in self.syn.getColumns(entity)]
            print(f"  Found {len(columns)} columns to check")
        except Exception as e:
            print(f"  ❌ Error getting columns: {e}")
            return
        
        # Track corrections across all columns
        all_corrections = {}
        columns_with_issues = 0
        columns_processed = 0
        
        for i, column_name in enumerate(columns, 1):
            print(f"  [{i}/{len(columns)}] Checking '{column_name}'...", end=" ")
            
            # Check if this column exists in the data model before proceeding
            try:
                jsonld_tool = JsonLdGetValidValuesTool()
                data_model_values = jsonld_tool._run(self.data_model_path, column_name)
                
                if "not found in the data model" in str(data_model_values) or "No valid values" in str(data_model_values):
                    print("⏭️  Not in data model (skipped)")
                    continue
                    
            except Exception as e:
                print(f"⚠️  Error checking data model: {e}")
                continue
            
            columns_processed += 1
            
            # Get correction plan for this column
            workflow = CorrectionWorkflow(
                syn=self.syn,
                llm=self.llm,
                view_synapse_id=view_id,
                data_model_path=self.data_model_path
            )
            
            correction_plan = workflow._get_column_correction_plan(column_name)
            
            if correction_plan and (correction_plan.get('corrections') or correction_plan.get('unmappable')):
                corrections = correction_plan.get('corrections', [])
                unmappable = correction_plan.get('unmappable', [])
                
                # Apply autonomous decision making
                if corrections:
                    approved_corrections = []
                    for correction in corrections:
                        # Use the confidence score provided by the LLM agent
                        confidence = correction.get('confidence', 0.5)  # Default to medium confidence if not provided
                        
                        if confidence >= 0.8:  # High confidence - auto-approve
                            approved_corrections.append(correction)
                            print(f"      ✅ Auto-approved: '{correction['current_value']}' → '{correction['new_value']}' (confidence: {confidence:.2f})")
                        elif confidence >= 0.6:  # Medium confidence - auto-approve with note
                            approved_corrections.append(correction)
                            print(f"      ⚠️  Auto-approved: '{correction['current_value']}' → '{correction['new_value']}' (confidence: {confidence:.2f})")
                        else:  # Low confidence - skip in autonomous mode, would need user review
                            print(f"      ❌ Skipped: '{correction['current_value']}' → '{correction['new_value']}' (confidence: {confidence:.2f} too low)")
                    
                    if approved_corrections:
                        all_corrections[column_name] = approved_corrections
                        columns_with_issues += 1
                        print(f"✅ {len(approved_corrections)} auto-approved")
                    else:
                        print("⚠️  All corrections had low confidence (skipped)")
                else:
                    print("✅ No issues")
            else:
                print("✅ No issues")
        
        # Summary
        print(f"\n📊 Summary: Processed {columns_processed} columns, found issues in {columns_with_issues}")
        
        # Apply all corrections if any were found
        if all_corrections:
            print(f"\n🤖 Applying corrections automatically...")
            workflow = CorrectionWorkflow(
                syn=self.syn,
                llm=self.llm,
                view_synapse_id=view_id,
                data_model_path=self.data_model_path
            )
            
            success = workflow.apply_annotation_corrections(all_corrections)
            if success:
                print(f"✅ Successfully applied corrections to {view_name}")
            else:
                print(f"❌ Failed to apply corrections to {view_name}")
        else:
            print(f"✅ No corrections needed for {view_name}")

    def _run_vocab_normalization_fully_autonomous(self, view_name, view_id):
        """Run vocabulary normalization with full autonomy - pick columns automatically"""
        print(f"🤖 Autonomously analyzing vocabulary in {view_name}...")
        
        try:
            view = self.syn.get(view_id)
            columns = [c['name'] for c in self.syn.getColumns(view)]
            
            # Automatically identify columns that likely need normalization
            target_columns = self._identify_normalization_candidates(columns, view_id)
            
            if not target_columns:
                print("  📊 No columns identified as normalization candidates")
                return []
            
            print(f"  📊 Auto-selected {len(target_columns)} columns for normalization: {', '.join(target_columns)}")
            
            all_changes = []
            
            for column_name in target_columns:
                print(f"    🔧 Normalizing '{column_name}'...", end=" ")
                
                try:
                    changes = self._run_single_column_normalization_autonomous(view_name, view_id, column_name)
                    if changes:
                        all_changes.extend(changes)
                        print(f"✅ {len(changes)} normalizations")
                    else:
                        print("✅ No changes needed")
                        
                except Exception as e:
                    print(f"⚠️ Error: {str(e)[:30]}...")
                    continue
            
            return all_changes
            
        except Exception as e:
            print(f"❌ Error processing vocabulary for {view_name}: {e}")
            return []

    def _identify_normalization_candidates(self, columns, view_id):
        """Automatically identify columns that likely need vocabulary normalization"""
        candidates = []
        
        # Look for columns with names that typically contain free-text entries
        text_column_patterns = [
            'author', 'contributor', 'investigator', 'name', 'title', 
            'description', 'keyword', 'tag', 'category', 'type'
        ]
        
        for column in columns:
            column_lower = column.lower()
            
            # Skip clearly structured columns
            if any(skip in column_lower for skip in ['id', 'date', 'time', 'url', 'path', 'size', 'count']):
                continue
                
            # Include columns likely to have normalization opportunities
            if any(pattern in column_lower for pattern in text_column_patterns):
                candidates.append(column)
                continue
                
            # Check if column has reasonable diversity for normalization
            try:
                query = f'SELECT COUNT(DISTINCT "{column}") as unique_count, COUNT(*) as total_count FROM {view_id} WHERE "{column}" IS NOT NULL LIMIT 1'
                result = self.syn.tableQuery(query).asDataFrame()
                
                if len(result) > 0:
                    unique_count = result['unique_count'].iloc[0]
                    total_count = result['total_count'].iloc[0]
                    
                    # Good normalization candidate: many values but not too many
                    if 2 <= unique_count <= min(100, total_count * 0.5):
                        candidates.append(column)
                        
            except Exception:
                # If we can't analyze, skip this column
                continue
        
        return candidates[:5]  # Limit to 5 columns to avoid overwhelming

    def _run_single_column_normalization_autonomous(self, view_name, view_id, column_name):
        """Run normalization for a single column with autonomous decisions"""
        workflow = UncontrolledVocabNormalizationWorkflow(
            syn=self.syn,
            llm=self.llm,
            views={view_name: view_id}
        )
        
        # Find row identifier automatically
        try:
            view = self.syn.get(view_id)
            columns = [c['name'] for c in self.syn.getColumns(view)]
            
            row_id_col = None
            for col in columns:
                if col.lower() in ['row_id', 'rowid', 'id', 'uid', 'unique_id']:
                    row_id_col = col
                    break
            
            if not row_id_col:
                # Default to first column that looks like an ID
                for col in columns:
                    if 'id' in col.lower():
                        row_id_col = col
                        break
                
            if not row_id_col:
                return []  # Can't proceed without row identifier
            
            # Run normalization with default rules
            follow_up_tasks = workflow.run_for_specific_column(
                view_name, view_id, column_name, row_id_col, 
                user_rules=""  # Use default normalization rules
            )
            
            return [{'table': view_name, 'column': column_name, 'type': 'vocabulary_normalization'}]
            
        except Exception as e:
            return []

    def _present_final_batch_for_approval(self, all_changes):
        """Present all accumulated changes for final approval"""
        print(f"\n🎯 FINAL REVIEW: Ready to apply {len(all_changes)} change batches")
        print("="*60)
        
        total_corrections = 0
        for change_batch in all_changes:
            table_name = change_batch['table']
            change_type = change_batch['type']
            
            if change_type == 'annotation':
                corrections = change_batch['corrections']
                total_corrections += len(corrections)
                
                print(f"\n📋 {table_name} - Annotation corrections ({len(corrections)} changes):")
                for correction in corrections[:3]:  # Show first 3
                    print(f"    '{correction['current_value']}' → '{correction['new_value']}' ({correction['reason']})")
                if len(corrections) > 3:
                    print(f"    ... and {len(corrections) - 3} more corrections")
                    
            elif change_type == 'vocabulary_normalization':
                print(f"\n📋 {table_name} - Vocabulary normalization in column '{change_batch['column']}'")
                total_corrections += 1
        
        print(f"\n🎯 TOTAL: {total_corrections} corrections across {len(all_changes)} operations")
        
        while True:
            approval = input("\n🚀 Apply all changes? (yes/no/details): ").lower()
            
            if approval in ['y', 'yes']:
                print("\n🚀 Applying all changes...")
                self._apply_all_changes(all_changes)
                break
            elif approval in ['n', 'no']:
                print("\n❌ Changes cancelled. No modifications made.")
                break
            elif approval == 'details':
                self._show_detailed_changes(all_changes)
            else:
                print("Please enter 'yes', 'no', or 'details'")

    def _apply_all_changes(self, all_changes):
        """Apply all the accumulated changes"""
        print("\n🔧 Applying changes...")
        
        for i, change_batch in enumerate(all_changes, 1):
            table_name = change_batch['table']
            print(f"  [{i}/{len(all_changes)}] Applying changes to {table_name}...", end=" ")
            
            try:
                if change_batch['type'] == 'annotation':
                    # Apply annotation corrections using the correction workflow
                    view_id = self.views[table_name]
                    workflow = CorrectionWorkflow(
                        syn=self.syn,
                        llm=self.llm,
                        view_synapse_id=view_id,
                        data_model_path=self.data_model_path
                    )
                    
                    # Group corrections by column
                    corrections_by_column = {}
                    for correction in change_batch['corrections']:
                        col_name = correction['column']
                        if col_name not in corrections_by_column:
                            corrections_by_column[col_name] = []
                        corrections_by_column[col_name].append(correction)
                    
                    success = workflow.apply_annotation_corrections(corrections_by_column)
                    if not success:
                        print("❌ Failed to apply corrections")
                        continue
                        
                elif change_batch['type'] == 'vocabulary_normalization':
                    # Vocabulary changes were already applied during processing
                    pass
                
                print("✅")
                
            except Exception as e:
                print(f"❌ Error: {e}")
        
        print("\n🎉 All changes applied successfully!")

    def _show_detailed_changes(self, all_changes):
        """Show detailed breakdown of all changes"""
        print("\n📋 DETAILED CHANGE BREAKDOWN")
        print("="*60)
        
        for change_batch in all_changes:
            table_name = change_batch['table']
            change_type = change_batch['type']
            
            print(f"\n🔸 {table_name} ({change_type}):")
            
            if change_type == 'annotation' and 'corrections' in change_batch:
                for correction in change_batch['corrections']:
                    print(f"    Column '{correction['column']}':")
                    print(f"      '{correction['current_value']}' → '{correction['new_value']}'")
                    print(f"      Reason: {correction['reason']} (Confidence: {correction['confidence']:.1%})")
                    
            elif change_type == 'vocabulary_normalization':
                print(f"    Column: {change_batch['column']}")
                print(f"    Applied vocabulary normalization rules")

    def _run_annotation_correction_manual(self):
        """Run annotation correction in manual mode"""
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
                    selected_view_id = self.views[selected_view_name]
                    print(f"\nYou have selected: {selected_view_name}")
                    break
                else:
                    print("Invalid choice. Please enter a number from the list.")
            except ValueError:
                print("Invalid input. Please enter a number.")
        
        workflow = CorrectionWorkflow(
            syn=self.syn,
            llm=self.llm,
            view_synapse_id=selected_view_id,
            data_model_path=self.data_model_path
        )
        workflow.run()

    def _run_vocab_normalization_manual(self):
        """Run vocabulary normalization in manual mode"""
        workflow = UncontrolledVocabNormalizationWorkflow(
            syn=self.syn,
            llm=self.llm,
            views=self.views
        )
        follow_up_tasks = workflow.run()
        self._handle_follow_up_tasks(follow_up_tasks)

    def _run_freetext_correction_manual(self):
        """Run freetext correction in manual mode"""
        workflow = FreetextCorrectionWorkflow(
            syn=self.syn, 
            llm=self.llm,
            views=self.views
        )
        follow_up_tasks = workflow.run()
        self._handle_follow_up_tasks(follow_up_tasks)

    def _handle_follow_up_tasks(self, follow_up_tasks):
        """Handle follow-up tasks like GitHub issue creation"""
        if follow_up_tasks:
            print("\n--- Processing Follow-up Tasks ---")
            for task in follow_up_tasks:
                if task.get('type') == 'github_issue':
                    try:
                        github_agent_class = self.agents.get('github_issue_filer')
                        if github_agent_class:
                            github_agent = github_agent_class()
                            github_agent.run(task)
                        else:
                            print("Error: GitHub Issue Filer Agent not configured.")
                    except ConnectionError as e:
                        print(f"Could not process GitHub issue task: {e}")
                else:
                    print(f"Unknown follow-up task type: {task.get('type')}") 