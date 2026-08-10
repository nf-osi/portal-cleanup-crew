import synapseclient

def prompt_for_view_and_column(syn: synapseclient.Synapse, views: dict):
    """
    Prompts the user to select a Synapse view/table and a column.

    Args:
        syn: An authenticated Synapse client.
        views: A dictionary of available views from the config.

    Returns:
        A tuple of (view_synapse_id, column_name, row_id_col) or (None, None, None) if the user cancels.
    """
    # Prompt user to select a table
    view_choices = list(views.keys())
    if not view_choices:
        print("No views configured in config.yaml. Exiting.")
        return None, None, None

    print("\\nPlease select a table to work on:")
    for i, view_name in enumerate(view_choices):
        print(f"{i+1}. {view_name} ({views[view_name]})")

    while True:
        try:
            choice = int(input("Enter the number of your choice: ")) - 1
            if 0 <= choice < len(view_choices):
                selected_view_name = view_choices[choice]
                view_synapse_id = views[selected_view_name]
                print(f"\\nYou have selected: {selected_view_name}")
                break
            else:
                print("Invalid choice. Please enter a number from the list.")
        except ValueError:
            print("Invalid input. Please enter a number.")

    # Get columns for the selected view
    try:
        view = syn.get(view_synapse_id)
        columns = [c['name'] for c in syn.getColumns(view)]
    except Exception as e:
        print(f"Error fetching columns for view '{view_synapse_id}': {e}")
        return None, None, None

    # Ask user for the column name to process
    print("\\nAvailable columns to process:")
    for i, col in enumerate(columns):
        print(f"{i+1}. {col}")
    
    while True:
        try:
            choice = int(input("Enter the number of the column you want to process: ")) - 1
            if 0 <= choice < len(columns):
                column_name = columns[choice]
                break
            else:
                print("Invalid choice. Please enter a number from the list.")
        except ValueError:
            print("Invalid input. Please enter a number.")

    # Ask user for the row identifier column
    print("\\nPlease select the column that uniquely identifies rows (e.g., ROW_ID):")
    for i, col in enumerate(columns):
        print(f"{i+1}. {col}")

    while True:
        try:
            choice = int(input("Enter the number of the identifier column: ")) - 1
            if 0 <= choice < len(columns):
                row_id_col = columns[choice]
                print(f"Using '{row_id_col}' as the row identifier.")
                break
            else:
                print("Invalid choice. Please enter a number from the list.")
        except ValueError:
            print("Invalid input. Please enter a number.")
            
    return view_synapse_id, column_name, row_id_col 