from crewai import Agent, Crew, Process, Task
import yaml
from .term_standardizer import get_term_standardizer_agent
from .typo_corrector import get_typo_corrector_agent
from src.utils.llm_utils import get_llm
import os
import synapseclient
import json
from src.workflows.correction import CorrectionWorkflow
import getpass

class OrchestratorAgent:
    def __init__(self, view_synapse_id=None, data_model_path=None):
        self.view_synapse_id = view_synapse_id
        self.data_model_path = data_model_path
        self.llm = get_llm()
        self.syn = self._login_to_synapse()
        self.standardizer_agent = get_term_standardizer_agent(llm=self.llm)
        self.typo_agent = get_typo_corrector_agent(llm=self.llm)

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
            print("3. Correct Typos in Free-Text Fields")
            print("4. Exit")
            
            choice = input("Enter the number of your choice: ")

            if choice == '1':
                workflow = CorrectionWorkflow(
                    syn=self.syn, 
                    llm=self.llm,
                    view_synapse_id=self.view_synapse_id,
                    data_model_path=self.data_model_path
                )
                workflow.run()
            elif choice == '2':
                # self._handle_term_standardization()
                print("Term standardization not yet implemented.")
            elif choice == '3':
                # self._handle_typo_correction()
                print("Typo correction not yet implemented.")
            elif choice == '4':
                print("Exiting the assistant. Goodbye!")
                break
            else:
                print("Invalid choice. Please enter a number from 1 to 4.") 