import synapseclient
from synapseclient import Table
import pandas as pd

def update_table_column_with_corrections(
    syn: synapseclient.Synapse, 
    table_id: str, 
    column_name: str, 
    corrections: list[dict]
):
    """
    Updates a single column in a Synapse table based on a list of corrections.

    This function will:
    1. Query for the specific rows that need updating.
    2. Apply the corrections to the queried data.
    3. Store the changes back to Synapse.

    Args:
        syn: An authenticated Synapse client instance.
        table_id: The Synapse ID of the table/view to update.
        column_name: The name of the column to update.
        corrections: A list of dictionaries, where each dict has
                     'current_value' and 'new_value' keys.
    """
    if not corrections:
        print(f"No corrections provided for column '{column_name}'.")
        return

    # Separate None values from string values for the WHERE clause
    current_values = [c['current_value'] for c in corrections]
    string_values = [f"'{v}'" for v in current_values if v is not None]
    
    where_clauses = []
    if string_values:
        where_clauses.append(f'"{column_name}" IN ({", ".join(string_values)})')
    if None in current_values:
        where_clauses.append(f'"{column_name}" IS NULL')
    
    if not where_clauses:
        print("No values to query for update.")
        return

    # Query for the rows to be updated to get their current state, ROW_ID, and ROW_VERSION
    query = f'SELECT "{column_name}" FROM {table_id} WHERE {" OR ".join(where_clauses)}'
    
    try:
        results = syn.tableQuery(query, includeRowIdAndRowVersion=True)
        updates_df = results.asDataFrame(convert_to_datetime=True)

        if updates_df.empty:
            print(f"Warning: Query for values to update in '{column_name}' returned no rows.")
            return

        # Create a mapping from old value to new value for efficient lookup
        correction_map = {c['current_value']: c['new_value'] for c in corrections}

        # Apply the corrections to the DataFrame using replace.
        # .replace is safer than .map as it leaves unmapped values (including NaN) untouched.
        updates_df[column_name] = updates_df[column_name].replace(correction_map)

        # Store the modified DataFrame back to Synapse
        table_to_store = Table(table_id, updates_df)
        syn.store(table_to_store)
        
        print(f"Successfully updated {len(updates_df)} rows for column '{column_name}' in table {table_id}.")

    except Exception as e:
        print(f"Error updating column '{column_name}' in Synapse table {table_id}: {e}")
        import traceback
        traceback.print_exc()

def get_synapse_table_as_df(syn: synapseclient.Synapse, synapse_id: str) -> pd.DataFrame:
    """
    Queries a Synapse table or view and returns it as a pandas DataFrame.
    """
    try:
        results = syn.tableQuery(f"SELECT * FROM {synapse_id}")
        df = results.asDataFrame()
        return df
    except Exception as e:
        print(f"Error querying table {synapse_id}: {e}")
        return pd.DataFrame()

def build_synapse_table(syn: synapseclient.Synapse, name: str, parent_id: str, columns: list) -> synapseclient.Table:
    """
    Builds a new Synapse Table with the given name, parent project/folder, and column schema.
    """
    try:
        schema = synapseclient.Schema(name=name, parent=parent_id, columns=columns)
        table = syn.store(schema)
        print(f"Successfully created table '{name}' ({table.id}).")
        return table
    except Exception as e:
        print(f"Error creating table '{name}': {e}")
        return None 