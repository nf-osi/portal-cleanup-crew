import synapseclient
from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from typing import Type, Optional
import io
import sys
import pandas as pd
from contextlib import redirect_stdout


class CodeSnippetInput(BaseModel):
    """Input for the Synapse Python Code Executor Tool."""
    code_snippet: str = Field(description="The Python code snippet to be executed.")

class SynapsePythonCodeExecutorTool(BaseTool):
    name: str = "Synapse Python Code Executor Tool"
    description: str = (
        "Executes a snippet of Python code with an authenticated 'synapseclient' instance named 'syn'. "
        "Use this to perform any operation available in the synapseclient library, such as querying data, "
        "getting schemas, updating entities, etc. The final expression's value will be returned. "
        "Print statements will be captured and returned as part of the output."
    )
    args_schema: Type[BaseModel] = CodeSnippetInput

    syn: Optional[synapseclient.Synapse] = None

    def __init__(self, syn: synapseclient.Synapse = None, **kwargs):
        super().__init__(**kwargs)
        self.syn = syn if syn else self._login()

    def _login(self):
        print("Checking for Synapse configuration file...")
        try:
            return synapseclient.login()
        except Exception as e:
            print(f"Synapse login failed: {e}")
            return None

    def _run(self, code_snippet: str) -> str:
        """
        Executes the given Python code snippet.
        """
        if not self.syn:
            return "Synapse client not initialized. Login failed."

        local_scope = {"syn": self.syn}
        output_buffer = io.StringIO()

        try:
            with redirect_stdout(output_buffer):
                exec(code_snippet, {"__builtins__": __builtins__}, local_scope)
            
            output = output_buffer.getvalue()
            if 'result' in local_scope:
                result_val = local_scope['result']
                # If the result is a pandas DataFrame, convert it to a structured format
                if isinstance(result_val, pd.DataFrame):
                    result_str = result_val.to_json(orient='records')
                else:
                    result_str = str(result_val)
                return f"Execution successful.\\nOutput:\\n{output}\\nResult:\\n{result_str}"
            return f"Execution successful.\\nOutput:\\n{output}"

        except Exception as e:
            return f"Execution failed: {e}\\nOutput:\\n{output_buffer.getvalue()}" 