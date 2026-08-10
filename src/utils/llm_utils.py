import yaml
from crewai import LLM
import os
import time
import random

def get_llm(agent_name: str = None, config: dict = None):
    """
    Initializes and returns the LLM based on the provided configuration.
    If an agent_name is given, it will look for a specific model for that agent
    (e.g., 'ontology_expert_model') and fall back to the base 'model' if not found.
    """
    if config is None:
        try:
            with open('config.yaml', 'r') as f:
                config = yaml.safe_load(f).get('llm', {})
        except (FileNotFoundError, yaml.YAMLError):
            print("Warning: Could not read or parse config.yaml. Relying on environment variables.")
            config = {}
    
    try:
        with open('creds.yaml', 'r') as f:
            creds_config = yaml.safe_load(f).get('llm', {})
    except (FileNotFoundError, yaml.YAMLError):
        print("Warning: Could not read or parse creds.yaml. Relying on environment variables.")
        creds_config = {}

    # Merge credentials, giving priority to creds.yaml
    credentials = {**config.get('credentials', {}), **creds_config.get('credentials', {})}

    for key, value in credentials.items():
        os.environ[key] = value

    # CrewAI's openrouter tool expects OPENAI_API_KEY. If OPENROUTER_API_KEY is provided,
    # set OPENAI_API_KEY to its value for compatibility.
    if 'OPENROUTER_API_KEY' in os.environ:
        os.environ['OPENAI_API_KEY'] = os.environ['OPENROUTER_API_KEY']

    # Determine the model name with agent-specific fallback
    model_name = None
    if agent_name:
        agent_config = config.get('agents', {}).get(agent_name, {})
        model_name = agent_config.get('model')

    if not model_name:
        model_name = config.get('model')

    if not model_name:
        raise ValueError("LLM model not specified in config.yaml or as an agent-specific model.")

    print(f"🤖  Using model: {model_name} for agent: {agent_name or 'default'}")
    return LLM(model=model_name)


def crew_kickoff_with_retry(crew, max_retries=3, base_delay=2, max_delay=10, context="operation"):
    """
    Executes crew.kickoff() with retry logic for LLM failures.
    
    Args:
        crew: The CrewAI crew instance to execute
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Base delay in seconds between retries (default: 2)
        max_delay: Maximum delay in seconds (default: 10)
        context: Description of the operation for logging (default: "operation")
    
    Returns:
        The result from crew.kickoff()
    
    Raises:
        The last exception encountered if all retries fail
    """
    for attempt in range(max_retries):
        try:
            result = crew.kickoff()
            # Check if the result is valid (not None or empty)
            if result is None:
                raise ValueError("Invalid response from LLM call - None result")
            
            # Check if result has expected attributes
            if hasattr(result, 'raw'):
                if not result.raw or result.raw.strip() == "":
                    raise ValueError("Invalid response from LLM call - empty raw content")
            elif isinstance(result, str):
                if not result.strip():
                    raise ValueError("Invalid response from LLM call - empty string result")
            
            # If we got here, the result appears valid
            return result
            
        except Exception as e:
            is_retry_worthy = (
                "Invalid response from LLM call" in str(e) or
                "None or empty" in str(e) or
                "RemoteProtocolError" in str(e) or
                "peer closed connection" in str(e) or
                "ConnectionError" in str(e) or
                "Timeout" in str(e) or
                "rate limit" in str(e).lower() or
                "502" in str(e) or
                "503" in str(e) or
                "504" in str(e)
            )
            
            if not is_retry_worthy or attempt == max_retries - 1:
                # Either not worth retrying or this was the last attempt
                if attempt == max_retries - 1:
                    print(f"❌ Failed to complete {context} after {max_retries} attempts. Last error: {e}")
                else:
                    print(f"❌ Non-retryable error in {context}: {e}")
                raise e
            
            # Calculate delay with exponential backoff and jitter
            delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
            
            print(f"⚠️  {context} failed (attempt {attempt + 1}/{max_retries}): {e}")
            print(f"🔄 Retrying in {delay:.1f} seconds...")
            time.sleep(delay)
    
    # This should never be reached, but just in case
    raise RuntimeError(f"Unexpected failure in retry logic for {context}") 