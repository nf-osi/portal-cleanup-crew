import synapseclient
import os

def get_synapse_client():
    """
    Initializes the Synapse client and logs in.

    Checks for the .synapseConfig file and uses it for login if available.
    """
    config_path = os.path.expanduser("~/.synapseConfig")
    
    print("Checking for Synapse configuration file...")
    if not os.path.exists(config_path):
        print("Synapse configuration file not found at ~/.synapseConfig.")
        print("You may be prompted for credentials.")
    else:
        print("Synapse configuration file found. Attempting to log in...")

    syn = synapseclient.Synapse()
    try:
        syn.login(silent=True)
        print("Synapse login successful.")
        return syn
    except Exception as e:
        print(f"Synapse login failed: {e}")
        return None 