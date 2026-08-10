import sys
from pathlib import Path
import yaml
import argparse

# Add the project root to the Python path
sys.path.append(str(Path(__file__).parent.parent))

from src.agents.orchestrator import OrchestratorAgent

def main():
    """
    Main function to run the Synapse curation and management agentic system.
    """
    print("Starting Synapse Curation and Management Agentic System...")
    
    try:
        # Load configuration from config.yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        
        # Correctly parse the nested configuration
        ac_config = config.get('annotation_corrector', {})
        fc_config = config.get('freetext_correction', {})
        oe_config = config.get('ontology_expert', {})
        
        orchestrator = OrchestratorAgent(config=config)
        orchestrator.run()
    except (ValueError, FileNotFoundError) as e:
        print(f"\nError: {e}")
        print("Please ensure your 'config.yaml' is configured correctly and you have set your API keys.")

if __name__ == "__main__":
    main() 