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
        "Use this for read-only operations like querying data or getting schemas. "
        "Do not use this for updates; use the dedicated 'update_view' or 'update_table' tools instead. "
        "The final expression's value will be returned. "
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


class UpdateViewInput(BaseModel):
    """Input for updating a Synapse View."""
    view_id: str = Field(description="The Synapse ID of the view to update.")
    updates_df: pd.DataFrame = Field(description="A pandas DataFrame containing the rows and columns to update.")

    class Config:
        arbitrary_types_allowed = True

class UpdateViewTool(BaseTool):
    name: str = "update_view"
    description: str = "Updates a Synapse View with the provided data."
    args_schema: Type[BaseModel] = UpdateViewInput
    syn: Optional[synapseclient.Synapse] = None

    def __init__(self, syn: synapseclient.Synapse, **kwargs):
        super().__init__(**kwargs)
        self.syn = syn

    def _run(self, view_id: str, updates_df: pd.DataFrame) -> str:
        """
        Executes the update operation for a Synapse View.
        """
        if not self.syn:
            return "Synapse client not initialized. Login failed."
        if updates_df.empty:
            return "No updates to perform."

        try:
            table_to_update = self.syn.get(view_id)
            self.syn.store(synapseclient.Table(table_to_update.id, updates_df))
            return f"Successfully updated view {view_id} with {len(updates_df)} rows."
        except Exception as e:
            return f"Failed to update view {view_id}. Error: {e}"


class UpdateTableInput(BaseModel):
    """Input for updating a Synapse Table."""
    table_id: str = Field(description="The Synapse ID of the table to update.")
    updates_df: pd.DataFrame = Field(description="A pandas DataFrame containing the rows and columns to update.")

    class Config:
        arbitrary_types_allowed = True

class UpdateTableTool(BaseTool):
    name: str = "update_table"
    description: str = "Updates a Synapse Table with the provided data."
    args_schema: Type[BaseModel] = UpdateTableInput
    syn: Optional[synapseclient.Synapse] = None

    def __init__(self, syn: synapseclient.Synapse, **kwargs):
        super().__init__(**kwargs)
        self.syn = syn

    def _run(self, table_id: str, updates_df: pd.DataFrame) -> str:
        """
        Executes the update operation for a Synapse Table.
        """
        if not self.syn:
            return "Synapse client not initialized. Login failed."
        if updates_df.empty:
            return "No updates to perform."
            
        try:
            table_to_update = self.syn.get(table_id)
            self.syn.store(synapseclient.Table(table_to_update.id, updates_df))
            return f"Successfully updated table {table_id} with {len(updates_df)} rows."
        except Exception as e:
            return f"Failed to update table {table_id}. Error: {e}" 