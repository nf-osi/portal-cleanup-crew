import sys
from pathlib import Path
import yaml

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
        
        orchestrator = OrchestratorAgent(ac_config=ac_config)
        orchestrator.run()
    except (ValueError, FileNotFoundError) as e:
        print(f"\nError: {e}")
        print("Please ensure your 'config.yaml' is configured correctly and you have set your API keys.")

if __name__ == "__main__":
    main() 