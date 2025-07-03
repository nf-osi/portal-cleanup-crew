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
        print("\\nChecking for Synapse configuration file...")
        if os.path.exists(os.path.expanduser("~/.synapseConfig")):
            print("Synapse configuration file found. Attempting to log in...")
            syn = synapseclient.login()
            print("\\nSynapse login successful.")
            return syn
        else:
            print("No Synapse configuration file found. Please log in manually.")
            syn_user = input("Enter your Synapse username: ")
            syn_pass = getpass.getpass("Enter your Synapse password or personal access token: ")
            try:
                syn = synapseclient.login(email=syn_user, password=syn_pass)
                print("\\nSynapse login successful.")
                return syn
            except Exception as e:
                print(f"\\nError logging in to Synapse: {e}")
                return None

    def run(self):
        if not self.syn:
            print("Synapse login failed. Exiting.")
            return

        print("\\nWelcome to the Synapse Data Curation Assistant.")
        while True:
            print("\\nPlease select a task to perform:")
            print("1. Correct Synapse Annotations based on data model")
            print("2. Standardize Uncontrolled Terms (e.g., investigator names)")
            print("3. Correct Free-Text Fields")
            print("4. Exit")
            
            choice = input("Enter the number of your choice: ")

            if choice == '1':
                # Since this workflow operates on a single view at a time,
                # we need to ask the user to select one, similar to the freetext workflow.
                view_choices = list(self.views.keys())
                if not view_choices:
                    print("No views configured in config.yaml. Exiting.")
                    continue

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
            elif choice == '2':
                workflow = UncontrolledVocabNormalizationWorkflow(
                    syn=self.syn,
                    llm=self.llm,
                    views=self.views
                )
                workflow.run()
            elif choice == '3':
                workflow = FreetextCorrectionWorkflow(
                    syn=self.syn, 
                    llm=self.llm,
                    views=self.views
                )
                follow_up_tasks = workflow.run()

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
            elif choice == '4':
                print("Exiting the assistant. Goodbye!")
                break
            else:
                print("Invalid choice. Please enter a number from 1 to 4.") 