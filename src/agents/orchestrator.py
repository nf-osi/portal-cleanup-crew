from crewai import Agent, Crew, Process, Task
import yaml
from .uncontrolled_vocab_normalizer import get_uncontrolled_vocab_normalizer_agent
from .freetext_corrector import get_freetext_corrector_agent
from .github_issue_filer import GitHubIssueFilerAgent
from .ontology_expert import OntologyExpert
from .link_external_data import get_link_external_data_agent
from .sync_external_metadata import get_sync_external_metadata_agent
from .dataset_annotation_agent import get_dataset_annotation_agent
from ..utils.llm_utils import get_llm
import os
import synapseclient
import json
from ..workflows.correction import CorrectionWorkflow
from ..workflows.freetext_correction import FreetextCorrectionWorkflow
from ..workflows.uncontrolled_vocab_normalization import UncontrolledVocabNormalizationWorkflow

import getpass
from ..tools.jsonld_tools import JsonLdGetValidValuesTool
import subprocess
import re
from ..utils.synapse_utils import (update_table_column_with_corrections, 
                                 get_synapse_table_as_df, 
                                 build_synapse_table)

class OrchestratorAgent:
    def __init__(self, config):
        self.config = config
        self.llm_config = config.get('llm', {})
        self.ac_config = config.get('annotation_corrector', {})
        self.fc_config = config.get('freetext_correction', {})
        self.oe_config = config.get('ontology_expert', {})
        
        self.data_model_path = self.ac_config.get('data_model_url')
        self.views = self.config.get('views', {})
        self.llm = get_llm(config=self.llm_config)
        self.syn = self._login_to_synapse()
        
        self.agents = {
            "uncontrolled_vocab_normalizer": get_uncontrolled_vocab_normalizer_agent(llm=get_llm('uncontrolled_vocab_normalizer', self.llm_config)),
            "freetext_corrector": get_freetext_corrector_agent(llm=get_llm('freetext_corrector', self.llm_config)),
            "github_issue_filer": GitHubIssueFilerAgent(),
            "ontology_expert": OntologyExpert(llm=get_llm('ontology_expert', self.llm_config)),
            "link_external_data": get_link_external_data_agent(syn=self.syn),
            "sync_external_metadata": get_sync_external_metadata_agent(syn=self.syn),
            "dataset_annotation_agent": get_dataset_annotation_agent(syn=self.syn)
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

    def run(self, mode=None):
        if not self.syn:
            print("Synapse login failed. Exiting.")
            return

        print("\nWelcome to the Synapse Data Curation Assistant.")
        self._run_manual_mode()


    def _run_manual_mode(self):
        """Run the original manual mode where user selects each task"""
        while True:
            print("\nPlease select a task to perform:")
            print("1. Correct Synapse Annotations based on data model")
            print("2. Standardize Uncontrolled Terms (e.g., investigator names)")
            print("3. Correct Free-Text Fields")
            print("4. Link External Dataset to Synapse (PRIDE, GEO, SRA, ENA, etc.)")
            print("5. Sync External Metadata to Synapse Annotations")
            print("6. Annotate Existing Synapse Dataset")
            print("7. Exit")

            choice = input("Enter the number of your choice: ")
            if choice == '1':
                self._run_annotation_correction_manual()
            elif choice == '2':
                self._run_uncontrolled_vocab_normalization_manual()
            elif choice == '3':
                self._run_freetext_correction_manual()
            elif choice == '4':
                self._run_link_external_data_manual()
            elif choice == '5':
                self._run_sync_external_metadata_manual()
            elif choice == '6':
                self._run_dataset_annotation_manual()
            elif choice == '7':
                break
            else:
                print("Invalid choice. Please enter a number from 1 to 7.")

    def _run_annotation_correction_manual(self):
        """
        Manually runs the annotation correction workflow.
        The user selects a table, and the workflow runs on it.
        """
        # Let user select a table
        if not self.views:
            print("No views configured in config.yaml. Cannot proceed.")
            return
            
        print("\nPlease select a table to work on:")
        view_list = list(self.views.items())
        for i, (view_name, view_id) in enumerate(view_list, 1):
            print(f"{i}. {view_name} ({view_id})")
        
        try:
            choice = int(input("Enter the number of your choice: "))
            if 1 <= choice <= len(view_list):
                selected_view_name, selected_view_id = view_list[choice - 1]
                print(f"\nYou have selected: {selected_view_name}")

                workflow = CorrectionWorkflow(
                    syn=self.syn, 
                    llm=self.llm, 
                    view_synapse_id=selected_view_id, 
                    data_model_path=self.data_model_path,
                    orchestrator=self  # Pass the orchestrator
                )
                follow_up_tasks = workflow.run()
                if follow_up_tasks:
                    self._handle_follow_up_tasks(follow_up_tasks)
            else:
                print("Invalid choice.")
        except ValueError:
            print("Invalid input. Please enter a number.")


    def _run_uncontrolled_vocab_normalization_manual(self):
        """
        Manually runs the uncontrolled vocabulary normalization workflow.
        The user selects a table, and the workflow runs on it.
        """
        workflow = UncontrolledVocabNormalizationWorkflow(
            syn=self.syn, 
            llm=self.llm,
            views=self.views,
            orchestrator=self  # Pass the orchestrator for ontology expert access
        )
        follow_up_tasks = workflow.run()
        if follow_up_tasks:
            self._handle_follow_up_tasks(follow_up_tasks)

    def _run_freetext_correction_manual(self):
        """
        Manually runs the freetext correction workflow.
        The user selects a table, and the workflow runs on it.
        """
        workflow = FreetextCorrectionWorkflow(
            syn=self.syn,
            llm=self.llm,
            views=self.views,
            freetext_settings=self.fc_config
        )
        follow_up_tasks = workflow.run()
        if follow_up_tasks:
            self._handle_follow_up_tasks(follow_up_tasks)
    


    def _run_link_external_data_manual(self):
        """
        Manually runs the External Data to Synapse linking workflow.
        """
        print("\n--- External Data to Synapse Linking Workflow ---")
        print("This workflow will fetch data from external repositories and create file links in Synapse.")
        print("Supported repositories: PRIDE (PXD...), GEO (GSE...), ENA (ERP..., ERR... - PREFERRED), SRA (SRP...), and more")
        print("NOTE: ENA provides direct raw FASTQ access (PREFERRED). SRA provides .sra format files only.")
        
        dataset_id = input("Enter the dataset ID (e.g., PXD001234 for PRIDE, GSE123456 for GEO, ERR123456 for ENA, SRP399711 for SRA): ").strip()
        target_synapse_id = input("Enter the target Synapse Project/Folder ID (e.g., syn12345678): ").strip()

        if not dataset_id or not target_synapse_id:
            print("Dataset ID and Synapse ID are required. Exiting workflow.")
            return

        # Validate Synapse ID format
        if not target_synapse_id.startswith('syn'):
            print("Warning: Synapse IDs typically start with 'syn'. Proceeding anyway...")

        # Detect repository type
        dataset_upper = dataset_id.upper()
        if dataset_upper.startswith('PXD'):
            repo_type = "PRIDE"
        elif dataset_upper.startswith('GSE'):
            repo_type = "GEO"
        elif dataset_upper.startswith('SRP'):
            repo_type = "SRA"
        elif dataset_upper.startswith(('ERP', 'ERR', 'ERS', 'ERX', 'PRJ')):
            repo_type = "ENA"
        else:
            repo_type = "UNKNOWN"
            print(f"Warning: Repository type not recognized for '{dataset_id}'. The agent will attempt to handle it generically.")

        # For SRA datasets, check if ENA FASTQ files are available and offer choice
        # For ENA datasets, use ENA tools directly
        file_type_preference = None
        if repo_type == "SRA":
            print(f"\nChecking file availability for SRA dataset {dataset_id}...")
            try:
                # Quick check for ENA FASTQ availability
                from src.tools.sra_tools import SraDatasetFilesTool
                sra_tool = SraDatasetFilesTool()
                files_result = sra_tool._run(srp_id=dataset_id, include_ena_fastq=True)
                
                sra_count = files_result.get('file_categories', {}).get('SRA', 0)
                fastq_count = files_result.get('file_categories', {}).get('FASTQ', 0)
                
                if fastq_count > 0:
                    print(f"\nFound {sra_count} SRA files and {fastq_count} ENA FASTQ files for {dataset_id}")
                    print("\nFile format options:")
                    print("1. Link SRA files (.sra format - requires SRA Toolkit for FASTQ conversion)")
                    print("2. Link ENA FASTQ files (direct FASTQ access - recommended)")
                    print("3. Link both SRA and ENA FASTQ files")
                    
                    while True:
                        choice = input("Choose file format preference (1, 2, or 3): ").strip()
                        if choice == "1":
                            file_type_preference = "SRA_ONLY"
                            print("Selected: SRA files only")
                            break
                        elif choice == "2":
                            file_type_preference = "FASTQ_ONLY"
                            print("Selected: ENA FASTQ files only (recommended)")
                            break
                        elif choice == "3":
                            file_type_preference = "BOTH"
                            print("Selected: Both SRA and ENA FASTQ files")
                            break
                        else:
                            print("Invalid choice. Please enter 1, 2, or 3.")
                else:
                    print(f"\nFound {sra_count} SRA files for {dataset_id}. No ENA FASTQ files available.")
                    file_type_preference = "SRA_ONLY"
                    
            except Exception as e:
                print(f"Warning: Could not check file availability: {e}")
                print("Proceeding with default SRA file linking...")
                file_type_preference = "SRA_ONLY"
        
        elif repo_type == "ENA":
            print(f"\nENA accession {dataset_id} detected. ENA provides direct FASTQ file access.")
            file_type_preference = "ENA_FASTQ_ONLY"

        print(f"\nInitiating external data linking for {repo_type} dataset {dataset_id} to Synapse container {target_synapse_id}...")

        try:
            from crewai import Task, Crew, Process

            # Create a task for the external data linking agent
            task_description = f"""
                Link external dataset {dataset_id} to Synapse container {target_synapse_id} using external file links only (no annotations)."""
            
            # Add file type preference for SRA datasets
            if file_type_preference:
                task_description += f"""
                
                FILE TYPE PREFERENCE: {file_type_preference}
                - If "SRA_ONLY": Only link .sra format files from NCBI SRA
                - If "FASTQ_ONLY": Only link FASTQ files from ENA (skip .sra files) 
                - If "BOTH": Link both SRA and ENA FASTQ files in separate subfolders
                - If "ENA_FASTQ_ONLY": Use ENA FASTQ Files Fetcher directly for efficient FASTQ-only access"""

            task = Task(
                description=task_description + f"""
                
                REPOSITORY IDENTIFICATION:
                - Analyze the dataset ID '{dataset_id}' to determine repository type
                - Use appropriate tools based on repository type (PREFER ENA OVER SRA when both are available):
                  * PXD prefix = PRIDE repository (use PRIDE Dataset Metadata Fetcher and PRIDE Dataset Files Fetcher)
                  * GSE prefix = GEO repository (use GEO Metadata Fetcher and GEO Dataset Files Fetcher)
                  * ERP/ERR/ERS/ERX/PRJ prefix = ENA repository (use ENA Dataset Metadata Fetcher and ENA FASTQ Files Fetcher) - PREFERRED for raw data
                  * SRP prefix = SRA repository (use SRA Dataset Metadata Fetcher and SRA Dataset Files Fetcher) - USE ONLY if ENA not available
                  * EBI Metagenomics URLs = ENA repository (use ENA tools for direct FASTQ access)
                  * Unknown = Use Code Interpreter Tool to investigate and create custom solution (supports external HTTP requests, package installation, and arbitrary Python code execution). CRITICAL: When using Code Interpreter Tool, the libraries_used parameter MUST be a list like ['requests', 'beautifulsoup4'] NOT a string like 'requests,beautifulsoup4'. If the dataset contains sequencing data, prioritize finding ENA accessions over SRA accessions.
                
                Follow these EFFICIENT steps using batch operations:
                1. Identify repository type from dataset ID pattern
                2. Fetch metadata using appropriate repository-specific tool
                3. Get the list of files using the most efficient approach for FILE TYPE PREFERENCE (PREFER RAW DATA):
                   - FIRST PRIORITY: Use ENA FASTQ Files Fetcher if ENA accessions are available (provides direct raw FASTQ access)
                   - SRA_ONLY: Use SRA Dataset Files Fetcher with include_ena_fastq=false (fetches only SRA files) - AVOID unless necessary
                   - FASTQ_ONLY: Use SRA Dataset Files Fetcher with include_ena_fastq=true, then IMMEDIATELY filter the results to include only files where file_category='FASTQ' (ignore all SRA files)
                   - BOTH: Use SRA Dataset Files Fetcher with include_ena_fastq=true (use all files as-is)
                   - ENA_FASTQ_ONLY: Use ENA FASTQ Files Fetcher directly (MOST EFFICIENT and PREFERRED for raw FASTQ access)
                4. Create appropriate folder structure based on filtered file list
                5. Use create_folders to create organized folder structure in ONE operation:
                   - Main folder named after the dataset (using title and accession)
                   - Subfolders based on repository conventions and file type preference
                6. Use create_external_file_links to create ALL external file links in ONE batch operation:
                   - NEVER download files - always use external file links
                   - Use appropriate URLs (FTP/HTTP) to create external links
                   - Organize files into logical subfolders by type and category
                   - Intelligently determine MIME types based on file extensions
                   - Include file sizes from metadata when available
                   - Include MD5 checksums from metadata when available for integrity validation
                7. Provide a summary of files linked and folder structure created
                
                CRITICAL EFFICIENCY RULES:
                - ALWAYS PREFER ENA over SRA when both are available (ENA provides better raw data access)
                - ALWAYS PREFER raw data formats (FASTQ) over processed formats (SRA) when possible
                - Use create_folders for ALL folders in ONE call
                - Use create_external_file_links for ALL filtered files in ONE batch call
                - Keep descriptions under 1000 characters when creating file links
                - This should complete in 4-6 tool calls total (including filtering step)
                - DO NOT apply annotations - that is handled by a separate workflow
                - RESPECT the FILE TYPE PREFERENCE - filter files immediately after fetching, don't process unwanted files
                - For FASTQ_ONLY preference: fetch with include_ena_fastq=true but immediately discard all files where file_category='SRA'
                - For Code Interpreter Tool: Always use libraries_used as a LIST format: ["requests", "beautifulsoup4"] never as string "requests,beautifulsoup4"
                - If unknown dataset contains sequencing data, look for ENA accessions (ERR, ERP, ERS, ERX) before SRA accessions (SRR, SRP)
                """,
                agent=self.agents["link_external_data"],
                expected_output="A detailed summary of the external data linking operation including repository type identified, folder structure created, external file links created, and any issues encountered. Note: Annotations are not applied in this step."
            )

            crew = Crew(
                agents=[self.agents["link_external_data"]],
                tasks=[task],
                process=Process.sequential,
                verbose=True,
                memory=False
            )

            print("\nExecuting external data linking workflow...")
            result = crew.kickoff()
            
            print("\n" + "="*60)
            print("EXTERNAL DATA LINKING WORKFLOW COMPLETED")
            print("="*60)
            print(f"Result: {result}")
            
        except Exception as e:
            print(f"\nError during external data linking workflow: {e}")
            print("Please check your inputs and try again.")

    def _run_sync_external_metadata_manual(self):
        """
        Manually runs the External Metadata to Synapse Annotations sync workflow.
        """
        print("\n--- External Metadata to Synapse Annotations Sync Workflow ---")
        print("This workflow will apply external metadata (such as from PRIDE, GEO, etc.) as")
        print("schema-compliant annotations to existing Synapse entities.")
        
        metadata_source = input("Enter the metadata source (e.g., 'PRIDE', 'GEO'): ").strip().upper()
        
        if metadata_source == 'PRIDE':
            pride_id = input("Enter the PRIDE dataset ID (e.g., PXD001234): ").strip()
            target_synapse_folder = input("Enter the Synapse folder ID containing the files to annotate: ").strip()
            
            if not pride_id or not target_synapse_folder:
                print("PRIDE ID and Synapse folder ID are required. Exiting workflow.")
                return
            
            print(f"\nApplying PRIDE metadata from {pride_id} to files in {target_synapse_folder}...")
            
            try:
                from crewai import Task, Crew, Process
                
                task = Task(
                    description=f"""
                    Apply PRIDE dataset metadata as schema-compliant annotations to Synapse entities.
                    
                    Steps:
                    1. Fetch metadata for PRIDE dataset {pride_id}
                    2. Use PRIDE Annotation Mapper to generate schema-compliant annotations:
                       - Map PRIDE metadata to valid schema attributes
                       - Use data model URL: {self.data_model_path}
                       - Ensure all annotation values are valid according to the schema
                    3. Find all file entities in Synapse folder {target_synapse_folder} (including subfolders)
                    4. Use apply_annotations to apply the mapped annotations to ALL files in ONE batch operation
                    5. Provide a summary of annotations applied
                    
                    CRITICAL REQUIREMENTS:
                    - ONLY use schema-valid annotation values
                    - Do NOT hardcode annotation values - always validate against schema
                    - Apply annotations in batch for efficiency
                    - Provide detailed summary of what annotations were applied
                    """,
                    agent=self.agents["sync_external_metadata"],
                    expected_output="A detailed summary of the external metadata sync operation including the annotations generated, files annotated, and any schema validation issues encountered."
                )
                
                crew = Crew(
                    agents=[self.agents["sync_external_metadata"]],
                    tasks=[task],
                    process=Process.sequential,
                    verbose=True,
                    memory=False
                )
                
                print("\nExecuting external metadata sync workflow...")
                result = crew.kickoff()
                
                print("\n" + "="*60)
                print("EXTERNAL METADATA SYNC WORKFLOW COMPLETED")
                print("="*60)
                print(f"Result: {result}")
                
            except Exception as e:
                print(f"\nError during external metadata sync workflow: {e}")
                print("Please check your inputs and try again.")
        
        else:
            print(f"Metadata source '{metadata_source}' is not yet supported.")
            print("Currently supported sources: PRIDE")

    def _run_dataset_annotation_manual(self):
        """
        Manually runs the dataset annotation workflow.
        """
        print("\n--- Dataset Annotation Workflow ---")
        print("This workflow analyzes an existing Synapse dataset and applies intelligent schema-based annotations.")
        print("It will identify data files, extract metadata, detect the appropriate template, and apply annotations.")
        
        synapse_id = input("Enter the Synapse folder/dataset ID to analyze (e.g., syn12345678): ").strip()

        if not synapse_id:
            print("Synapse ID is required. Exiting workflow.")
            return

        # Validate Synapse ID format
        if not synapse_id.startswith('syn'):
            print("Warning: Synapse IDs typically start with 'syn'. Proceeding anyway...")

        print(f"\nInitiating dataset analysis and annotation for {synapse_id}...")
        print("This will:")
        print("1. Analyze folder structure and classify files")
        print("2. Extract external identifiers and metadata")
        print("3. Determine appropriate metadata template")
        print("4. Generate and apply schema-compliant annotations")
        print("5. Save annotation summary as CSV")

        try:
            from crewai import Task, Crew, Process

            task = Task(
                description=f"""
                Analyze and annotate Synapse dataset {synapse_id} with intelligent schema-based annotations.
                
                Follow these steps in order:
                
                1. ANALYZE FOLDER STRUCTURE:
                   - Use Synapse Folder Analysis Tool to scan {synapse_id} recursively
                   - Identify and classify all files (data vs metadata vs other)
                   - Extract external identifiers from names, descriptions, and annotations
                   - Get summary of file types, sizes, and existing annotations
                
                2. ANALYZE METADATA FILES:
                   - If metadata files are found, use Metadata File Analysis Tool to extract structured information
                   - Parse up to 5 metadata files to gather additional context
                   - Extract key-value pairs, tabular data, or structured information
                
                3. DETERMINE APPROPRIATE TEMPLATE:
                   - Use Template Detection Tool to get all available templates from the schema
                   - Use data model URL: {self.data_model_path}
                   - Analyze file types, external identifiers, and metadata content
                   - Based on the evidence, select the most appropriate metadata template
                   - Consider file extensions, content types, external repository IDs, and metadata content
                
                4. GENERATE ANNOTATIONS:
                   - Use Annotation Generation Tool with the chosen template
                   - Use data model URL: {self.data_model_path}
                   - Get controlled vocabulary options for each template attribute
                   - Map available metadata to appropriate schema attributes
                   - Generate consistent annotations for all DATA files (not metadata files)
                   - Ensure required attributes are filled and controlled vocabularies are used
                
                5. APPLY ANNOTATIONS:
                   - Use apply_annotations to apply generated annotations to all data files in batch
                   - Focus only on DATA files, not metadata or auxiliary files
                   - Use the file classification from step 1 to determine which files to annotate
                
                6. SAVE DOCUMENTATION:
                   - Use Annotation CSV Save Tool to create a local CSV record
                   - Save to './dataset_annotations_{{timestamp}}.csv'
                   - Include all generated annotations for documentation and review
                
                CRITICAL REQUIREMENTS:
                - Only annotate DATA files, not metadata or auxiliary files
                - Use schema-compliant controlled vocabulary values when available
                - Fill all required template attributes
                - Apply consistent annotations across similar files
                - Base decisions on actual file analysis, not assumptions
                - Let the available templates and metadata guide the annotation process
                - ALWAYS use data model URL: {self.data_model_path} for ALL JSON-LD operations
                """,
                agent=self.agents["dataset_annotation_agent"],
                expected_output="A comprehensive summary of the dataset analysis and annotation process, including the number of files analyzed, template selected, annotations applied, and any issues encountered. Include the path to the saved CSV file with annotation details."
            )
            
            crew = Crew(
                agents=[self.agents["dataset_annotation_agent"]],
                tasks=[task],
                process=Process.sequential,
                verbose=True,
                memory=False
            )
            
            result = crew.kickoff()
            print(f"\nDataset annotation workflow completed successfully!")
            print(f"Result: {result}")
            
        except Exception as e:
            print(f"Error running dataset annotation workflow: {e}")
            import traceback
            traceback.print_exc()

    def _handle_follow_up_tasks(self, follow_up_tasks):
        """
        Handles any follow-up tasks generated by a workflow,
        such as filing GitHub issues.
        """
        if not follow_up_tasks:
            return

        print("\nFollow-up tasks have been generated:")
        for i, task_details in enumerate(follow_up_tasks, 1):
            print(f"  {i}. Type: {task_details.get('type')}")
            # print(f"     Details: {task_details.get('details')}")

        if input("Do you want to execute these tasks now? (yes/no): ").lower() in ['y', 'yes']:
            for task_details in follow_up_tasks:
                if task_details.get('type') == 'file_github_issue' and self.agents.get("github_issue_filer"):
                    print("\nFiling GitHub issue...")
                    self.agents["github_issue_filer"].file_issue(
                        title=task_details['details'].get('title'),
                        body=task_details['details'].get('body'),
                        repo=task_details['details'].get('repo')
                    )
                else:
                    print(f"Skipping task of type '{task_details.get('type')}' - no handler configured.")

    def _consult_ontology_expert(self, column_name: str, value: str) -> dict:
        """
        Consults the ontology expert for a suggestion for a given value.
        """
        print(f"  🎓 Consulting ontology expert for '{value}' in column '{column_name}'...")
        
        try:
            # Get a few other values from the column to provide context
            context_query = f'SELECT "{column_name}" FROM {self.ac_config["main_fileview"]} WHERE "{column_name}" IS NOT NULL LIMIT 5'
            context_df = self.syn.tableQuery(context_query).asDataFrame()
            context_values = context_df[column_name].unique().tolist()
            
            # Remove the value we are trying to map, if it's there
            if value in context_values:
                context_values.remove(value)
            
            # Make sure we don't have too many, and they are strings
            context_values = [str(v) for v in context_values[:4]]

        except Exception as e:
            # print(f"      Warning: Could not get context values for expert. {e}")
            context_values = []

        task = Task(
            description=f"For the column '{column_name}', find the best standardized ontology term for the value '{value}'. For context, here are some other existing values in this column: {context_values}\nYour final answer MUST be a single JSON object with 'term', 'uri', and 'confidence_score' keys.",
            agent=self.agents["ontology_expert"],
            expected_output="A single JSON object with 'term', 'uri', and 'confidence_score' keys."
        )

        crew = Crew(
            agents=[self.agents["ontology_expert"]],
            tasks=[task],
            process=Process.sequential,
            verbose=True,
            memory=False
        )
        
        result = crew.kickoff()
        
        try:
            # The result from crew.kickoff() is a CrewOutput object, we need the raw string from it
            json_string = result.raw if hasattr(result, 'raw') else str(result)
            
            # The output might be wrapped in a JSON markdown block
            if '```json' in json_string:
                json_string = json_string.split('```json')[1].split('```')[0].strip()

            expert_suggestion = json.loads(json_string)
            if expert_suggestion and 'term' in expert_suggestion and 'uri' in expert_suggestion:
                print(f"  🎓 Expert suggests: '{expert_suggestion['term']}' ({expert_suggestion['uri']})")
                
                # Check if the suggested term is a new term for this column
                is_new = self._is_new_term(column_name, expert_suggestion['uri'])

                confidence_val = expert_suggestion.get('confidence_score', 0.7)
                if isinstance(confidence_val, str):
                    if confidence_val.lower() == 'high':
                        confidence = 0.9
                    elif confidence_val.lower() == 'medium':
                        confidence = 0.6
                    elif confidence_val.lower() == 'low':
                        confidence = 0.3
                    else: # Try to convert to float if it's a string number
                        try:
                            confidence = float(confidence_val)
                        except (ValueError, TypeError):
                            confidence = 0.7
                else:
                    confidence = float(confidence_val)

                return {
                    "current_value": value,
                    "new_value": expert_suggestion['term'],
                    "confidence": confidence,
                    "is_new_term": is_new,
                    "uri": expert_suggestion['uri']
                }
        except (json.JSONDecodeError, TypeError) as e:
            print(f"  ❌ Error decoding expert suggestion: {e}")
            raw_output = result.raw if hasattr(result, 'raw') else str(result)
            print(f"  Raw output from expert: {raw_output}")
            return None

    def _is_new_term(self, column_name, term_uri):
        """
        Checks if a given term URI is new for a specific column based on the data model.
        """
        try:
            jsonld_tool = JsonLdGetValidValuesTool()
            valid_values = jsonld_tool._run(source=self.data_model_path, attribute_name=column_name)
            
            # The tool returns a list of dictionaries if there are URIs
            if isinstance(valid_values, list) and valid_values:
                # Check if the first item is a dict to guess the structure
                if isinstance(valid_values[0], dict):
                    existing_uris = [v.get('uri') for v in valid_values if v.get('uri')]
                    return term_uri not in existing_uris
        except Exception:
            # If we can't get the valid values, assume it's not new to be safe
            return False
        return True # It's new if we couldn't find it or the list was empty

    def _apply_all_changes(self, all_changes):
        """
        Applies a batch of changes collected across multiple tables/workflows.
        """
        # We need to flatten the `all_changes` structure for the CorrectionWorkflow
        # The structure is a list of dicts, e.g.,
        # [{'table': 'view_name', 'type': 'annotation', 'corrections': {'col': [...]}}, ...]

        for change_batch in all_changes:
            table_name = change_batch.get('table')
            change_type = change_batch.get('type')
            
            if change_type == 'annotation':
                corrections = change_batch.get('corrections')
                view_id = self.views.get(table_name)
                if view_id and corrections:
                    print(f"\nApplying annotation changes to {table_name} ({view_id})...")
                    workflow = CorrectionWorkflow(
                        syn=self.syn, 
                        llm=self.llm, 
                        view_synapse_id=view_id, 
                        data_model_path=self.data_model_path,
                        orchestrator=self
                    )
                    workflow._execute_updates(corrections)
            
            elif change_type == 'vocab':
                # This part is a bit trickier because the vocab workflow handles execution internally.
                # For now, we'll assume the semi-autonomous vocab workflow would need a similar
                # refactor to queue changes and execute them here.
                # This is a placeholder for that future implementation.
                print(f"Skipping application of vocab changes for {table_name} - not yet implemented in batch mode.")

    def _show_detailed_changes(self, flat_changes):
        """
        Shows a detailed view of all changes about to be applied.
        """
        print("\n--- Detailed Changes ---")
        for change in flat_changes:
            print(f"Table: {change['table_name']}")
            print(f"  Column: {change['column']}")
            print(f"    - Change '{change['current_value']}' to '{change['new_value']}'")
        print("------------------------\n")