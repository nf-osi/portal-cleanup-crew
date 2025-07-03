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
        view_synapse_id = ac_config.get('main_fileview')
        data_model_path = ac_config.get('data_model_url')

        if not view_synapse_id or not data_model_path:
            raise ValueError("Configuration error: 'main_fileview' and 'data_model_url' must be set under 'annotation_corrector' in config.yaml")

        orchestrator = OrchestratorAgent(view_synapse_id=view_synapse_id, data_model_path=data_model_path)
        orchestrator.run()
    except (ValueError, FileNotFoundError) as e:
        print(f"\nError: {e}")
        print("Please ensure your 'config.yaml' is configured correctly and you have set your API keys.")

if __name__ == "__main__":
    main() 