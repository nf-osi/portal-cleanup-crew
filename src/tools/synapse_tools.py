import synapseclient
from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from typing import Type, Optional, List
import io
import sys
import pandas as pd
from contextlib import redirect_stdout
import os


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


class FileUploadInput(BaseModel):
    """Input for uploading a file to Synapse."""
    file_path: str = Field(description="Local path to the file to upload")
    parent_id: str = Field(description="Synapse ID of the parent container (project or folder)")
    description: Optional[str] = Field(default=None, description="Optional description for the file")
    annotations: Optional[dict] = Field(default=None, description="Optional annotations for the file")

    class Config:
        arbitrary_types_allowed = True


class SynapseFileUploadTool(BaseTool):
    name: str = "upload_file"
    description: str = (
        "Uploads a file to a Synapse project or folder. "
        "Returns the Synapse ID of the uploaded file."
    )
    args_schema: Type[BaseModel] = FileUploadInput
    syn: Optional[synapseclient.Synapse] = None

    def __init__(self, syn: synapseclient.Synapse, **kwargs):
        super().__init__(**kwargs)
        self.syn = syn

    def _run(self, file_path: str, parent_id: str, description: Optional[str] = None, annotations: Optional[dict] = None) -> str:
        """
        Uploads a file to Synapse.
        
        Args:
            file_path: Local path to the file
            parent_id: Synapse ID of parent container
            description: Optional file description
            annotations: Optional file annotations
            
        Returns:
            Status message with file ID or error details
        """
        if not self.syn:
            return "Synapse client not initialized. Login failed."
        
        if not os.path.exists(file_path):
            return f"File not found: {file_path}"
        
        try:
            # Create the file entity
            file_entity = synapseclient.File(
                path=file_path,
                parent=parent_id,
                description=description
            )
            
            # Add annotations if provided
            if annotations:
                file_entity.annotations = annotations
            
            # Upload the file
            uploaded_file = self.syn.store(file_entity)
            
            return f"Successfully uploaded file to Synapse. File ID: {uploaded_file.id}, Name: {uploaded_file.name}"
            
        except Exception as e:
            return f"Failed to upload file {file_path} to Synapse. Error: {e}"


class FolderCreationInput(BaseModel):
    """Input for creating a folder in Synapse."""
    folder_name: str = Field(description="Name of the folder to create")
    parent_id: str = Field(description="Synapse ID of the parent container (project or folder)")
    description: Optional[str] = Field(default=None, description="Optional description for the folder")


class SynapseFolderCreationTool(BaseTool):
    name: str = "create_folder"
    description: str = (
        "Creates a folder in a Synapse project or existing folder. "
        "Returns the Synapse ID of the created folder."
    )
    args_schema: Type[BaseModel] = FolderCreationInput
    syn: Optional[synapseclient.Synapse] = None

    def __init__(self, syn: synapseclient.Synapse, **kwargs):
        super().__init__(**kwargs)
        self.syn = syn

    def _run(self, folder_name: str, parent_id: str, description: Optional[str] = None) -> str:
        """
        Creates a folder in Synapse.
        
        Args:
            folder_name: Name of the folder
            parent_id: Synapse ID of parent container
            description: Optional folder description
            
        Returns:
            Status message with folder ID or error details
        """
        if not self.syn:
            return "Synapse client not initialized. Login failed."
        
        try:
            # Create the folder entity
            folder = synapseclient.Folder(
                name=folder_name,
                parent=parent_id,
                description=description
            )
            
            # Store the folder
            created_folder = self.syn.store(folder)
            
            return f"Successfully created folder in Synapse. Folder ID: {created_folder.id}, Name: {created_folder.name}"
            
        except Exception as e:
            return f"Failed to create folder '{folder_name}' in Synapse. Error: {e}"


class ExternalFileLinkInput(BaseModel):
    """Input for creating an external file link in Synapse."""
    external_url: str = Field(description="URL of the external file (e.g., FTP, HTTP)")
    file_name: str = Field(description="Name for the file in Synapse")
    parent_id: str = Field(description="Synapse ID of the parent container (project or folder)")
    description: Optional[str] = Field(default=None, description="Optional description for the file")
    annotations: Optional[dict] = Field(default=None, description="Optional annotations for the file")
    file_size: Optional[int] = Field(default=None, description="Optional file size in bytes")
    mimetype: Optional[str] = Field(default=None, description="Optional MIME type for the file (e.g., 'application/xml' for mzML, 'text/tab-separated-values' for TSV). If not provided, defaults to 'application/octet-stream'")

    class Config:
        arbitrary_types_allowed = True


class SynapseExternalFileLinkTool(BaseTool):
    name: str = "create_external_file_link"
    description: str = (
        "Creates a Synapse File entity that links to an external URL (like FTP or HTTP) "
        "without downloading the file. This creates a 'symlink' to the external data. "
        "The LLM should intelligently determine appropriate MIME types based on file extensions "
        "(e.g., 'application/xml' for .mzML, 'text/tab-separated-values' for .tsv, etc.) "
        "and provide file sizes from metadata when available."
    )
    args_schema: Type[BaseModel] = ExternalFileLinkInput
    syn: Optional[synapseclient.Synapse] = None

    def __init__(self, syn: synapseclient.Synapse, **kwargs):
        super().__init__(**kwargs)
        self.syn = syn

    def _run(self, external_url: str, file_name: str, parent_id: str, 
             description: Optional[str] = None, annotations: Optional[dict] = None, file_size: Optional[int] = None, mimetype: Optional[str] = None) -> str:
        """
        Creates an external file link in Synapse using external file handles.
        
        Args:
            external_url: URL of the external file
            file_name: Name for the file in Synapse
            parent_id: Synapse ID of parent container
            description: Optional file description
            annotations: Optional file annotations
            file_size: Optional file size in bytes
            mimetype: Optional MIME type for the file
            
        Returns:
            Status message with file ID or error details
        """
        if not self.syn:
            return "Synapse client not initialized. Login failed."
        
        try:
            # Create an external file handle using the built-in synapse client method
            file_handle_args = {
                'externalURL': external_url,
                'mimetype': mimetype if mimetype is not None else 'application/octet-stream'
            }
            
            # Add file size if provided
            if file_size is not None:
                file_handle_args['fileSize'] = file_size
            
            file_handle = self.syn._createExternalFileHandle(**file_handle_args)
            
            # Create the file entity using synapseclient.File with the external file handle
            file_entity = synapseclient.File(
                name=file_name,
                parent=parent_id,
                dataFileHandleId=file_handle['id']
            )
            # Set the download filename to preserve original extension
            file_entity.properties['downloadAs'] = file_name
            
            if description:
                file_entity.description = description
            
            # Add annotations if provided
            if annotations:
                file_entity.annotations = annotations
            
            # Store the file entity
            created_entity = self.syn.store(file_entity)
            
            return f"Successfully created external file link in Synapse. File ID: {created_entity.id}, Name: {created_entity.name}, External URL: {external_url}"
            
        except Exception as e:
            error_msg = f"Failed to create external file link '{file_name}' in Synapse. Error: {e}"
            # Add more detailed error information if available
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                error_msg += f" Response: {e.response.text}"
            if hasattr(e, 'response') and hasattr(e.response, 'status_code'):
                error_msg += f" Status: {e.response.status_code}"
            return error_msg + " Please reference API documentation at https://rest-docs.synapse.org/rest/"


class BatchFolderCreationInput(BaseModel):
    """Input for creating multiple folders in Synapse."""
    folders: List[dict] = Field(description="List of folder specifications, each containing 'folder_name', 'parent_id', and optional 'description'")

    class Config:
        arbitrary_types_allowed = True


class SynapseBatchFolderCreationTool(BaseTool):
    name: str = "create_folders"
    description: str = (
        "Creates multiple folders in Synapse in a single operation. "
        "Returns the Synapse IDs of all created folders."
    )
    args_schema: Type[BaseModel] = BatchFolderCreationInput
    syn: Optional[synapseclient.Synapse] = None

    def __init__(self, syn: synapseclient.Synapse, **kwargs):
        super().__init__(**kwargs)
        self.syn = syn

    def _run(self, folders: List[dict]) -> str:
        """
        Creates multiple folders in Synapse.
        
        Args:
            folders: List of folder specifications
            
        Returns:
            Status message with all folder IDs or error details
        """
        if not self.syn:
            return "Synapse client not initialized. Login failed."
        
        if not folders:
            return "No folders to create."
        
        results = []
        try:
            for folder_spec in folders:
                folder_name = folder_spec['folder_name']
                parent_id = folder_spec['parent_id']
                description = folder_spec.get('description', None)
                
                # Create the folder entity
                folder = synapseclient.Folder(
                    name=folder_name,
                    parent=parent_id,
                    description=description
                )
                
                # Store the folder
                created_folder = self.syn.store(folder)
                results.append(f"Created folder '{created_folder.name}' (ID: {created_folder.id})")
            
            return f"Successfully created {len(results)} folders:\n" + "\n".join(results)
            
        except Exception as e:
            return f"Failed to create folders. Error: {e}"


class BatchExternalFileLinkInput(BaseModel):
    """Input for creating multiple external file links in Synapse."""
    files: List[dict] = Field(description="List of file specifications, each containing 'external_url', 'file_name', 'parent_id', and optional 'description', 'file_size', 'mimetype'")

    class Config:
        arbitrary_types_allowed = True


class SynapseBatchExternalFileLinkTool(BaseTool):
    name: str = "create_external_file_links"
    description: str = (
        "Creates multiple Synapse File entities that link to external URLs (like FTP or HTTP) "
        "without downloading the files in a single batch operation. "
        "The LLM should intelligently determine appropriate MIME types and file sizes for each file."
    )
    args_schema: Type[BaseModel] = BatchExternalFileLinkInput
    syn: Optional[synapseclient.Synapse] = None

    def __init__(self, syn: synapseclient.Synapse, **kwargs):
        super().__init__(**kwargs)
        self.syn = syn

    def _run(self, files: List[dict]) -> str:
        """
        Creates multiple external file links in Synapse.
        
        Args:
            files: List of file specifications
            
        Returns:
            Status message with all file IDs or error details
        """
        if not self.syn:
            return "Synapse client not initialized. Login failed."
        
        if not files:
            return "No files to create."
        
        results = []
        failed = []
        
        for file_spec in files:
            try:
                external_url = file_spec['external_url']
                file_name = file_spec['file_name'] 
                parent_id = file_spec['parent_id']
                description = file_spec.get('description', None)
                file_size = file_spec.get('file_size', None)
                mimetype = file_spec.get('mimetype', 'application/octet-stream')
                
                # Create an external file handle using the built-in synapse client method
                file_handle_args = {
                    'externalURL': external_url,
                    'mimetype': mimetype
                }
                
                # Add file size if provided
                if file_size is not None:
                    file_handle_args['fileSize'] = file_size
                
                file_handle = self.syn._createExternalFileHandle(**file_handle_args)
                
                # Create the file entity using synapseclient.File with the external file handle
                file_entity = synapseclient.File(
                    name=file_name,
                    parent=parent_id,
                    dataFileHandleId=file_handle['id']
                )
                # Set the download filename to preserve original extension
                file_entity.properties['downloadAs'] = file_name
                
                if description:
                    file_entity.description = description
                
                # Store the file entity
                created_entity = self.syn.store(file_entity)
                
                results.append(f"Created external file link '{created_entity.name}' (ID: {created_entity.id})")
                
            except Exception as e:
                failed.append(f"Failed to create '{file_spec.get('file_name', 'unknown')}': {e}")
        
        summary = f"Successfully created {len(results)} external file links"
        if failed:
            summary += f", {len(failed)} failed"
        
        summary += ":\n" + "\n".join(results)
        if failed:
            summary += "\n\nFailures:\n" + "\n".join(failed)
        
        return summary


class BatchAnnotationInput(BaseModel):
    """Input for applying annotations to multiple Synapse entities."""
    annotations: List[dict] = Field(description="List of annotation specifications, each containing 'entity_id' and 'annotations' dict")

    class Config:
        arbitrary_types_allowed = True


class SynapseBatchAnnotationTool(BaseTool):
    name: str = "apply_annotations"
    description: str = (
        "Applies annotations to multiple Synapse entities in a single batch operation. "
        "Each annotation specification should include the entity ID and the annotations to apply."
    )
    args_schema: Type[BaseModel] = BatchAnnotationInput
    syn: Optional[synapseclient.Synapse] = None

    def __init__(self, syn: synapseclient.Synapse, **kwargs):
        super().__init__(**kwargs)
        self.syn = syn

    def _run(self, annotations: List[dict]) -> str:
        """
        Applies annotations to multiple Synapse entities.
        
        Args:
            annotations: List of annotation specifications
            
        Returns:
            Status message with results or error details
        """
        if not self.syn:
            return "Synapse client not initialized. Login failed."
        
        if not annotations:
            return "No annotations to apply."
        
        results = []
        failed = []
        
        for annotation_spec in annotations:
            try:
                entity_id = annotation_spec['entity_id']
                annotation_dict = annotation_spec['annotations']
                
                # Get the entity without downloading the file
                entity = self.syn.get(entity_id, downloadFile=False)
                
                # Apply the annotations
                entity.annotations = annotation_dict
                
                # Store the updated entity  
                updated_entity = self.syn.store(entity, forceVersion=False)
                
                results.append(f"Applied annotations to '{updated_entity.name}' (ID: {updated_entity.id})")
                
            except Exception as e:
                failed.append(f"Failed to annotate '{annotation_spec.get('entity_id', 'unknown')}': {e}")
        
        summary = f"Successfully applied annotations to {len(results)} entities"
        if failed:
            summary += f", {len(failed)} failed"
        
        summary += ":\n" + "\n".join(results)
        if failed:
            summary += "\n\nFailures:\n" + "\n".join(failed)
        
        return summary


def get_entity_children_recursively(syn: synapseclient.Synapse, synapse_id: str) -> pd.DataFrame:
    """
    Recursively fetches all children of a Synapse entity (project, folder, or dataset)
    and returns them as a pandas DataFrame.
    """
    try:
        entity = syn.get(synapse_id, downloadFile=False)
        
        items_to_process = []
        if isinstance(entity, (synapseclient.Project, synapseclient.Folder)):
            items_to_process = list(syn.getChildren(parent=entity, includeTypes=["file", "folder", "dataset"]))
        elif isinstance(entity, synapseclient.Dataset):
            dataset_items = syn.tableQuery(f"SELECT id FROM {entity.id}").asDataFrame()
            for item_id in dataset_items['id']:
                 items_to_process.append({'id': item_id, 'type': 'org.sagebionetworks.repo.model.FileEntity'})

        all_files_df = pd.DataFrame()
        for child in items_to_process:
            child_id = child['id']
            child_type = child.get('type', '')

            if 'FileEntity' in child_type:
                try:
                    file_entity = syn.get(child_id, downloadFile=False)
                    file_df = pd.DataFrame([{
                        'id': file_entity.id,
                        'name': file_entity.name,
                        'etag': file_entity.etag,
                        'version': file_entity.versionNumber
                    }])
                    all_files_df = pd.concat([all_files_df, file_df], ignore_index=True)
                except synapseclient.core.exceptions.SynapseHTTPError as e:
                    print(f"Warning: Could not get file entity {child_id}. {e}")

            elif 'FolderEntity' in child_type or 'DatasetEntity' in child_type:
                sub_files_df = get_entity_children_recursively(syn, child_id)
                if not sub_files_df.empty:
                    all_files_df = pd.concat([all_files_df, sub_files_df], ignore_index=True)

        return all_files_df

    except Exception as e:
        print(f"Error getting children for {synapse_id}: {e}")
        return pd.DataFrame() 