import unittest
import synapseclient
from synapseclient import Project, Schema, Column, Table, RowSet
import pandas as pd
import uuid
from src.utils.synapse_utils import update_table_column_with_corrections

class TestSynapseUtilsIntegration(unittest.TestCase):

    def setUp(self):
        """
        Sets up a real Synapse project and table for integration testing.
        """
        self.syn = synapseclient.login()
        self.project_name = f"Test Project - {uuid.uuid4()}"
        self.project = self.syn.store(Project(name=self.project_name))

        # Define table schema
        self.column_name = "resourceType"
        cols = [
            Column(name=self.column_name, columnType='STRING', maximumSize=50),
            Column(name='another_col', columnType='INTEGER'),
        ]
        schema = self.syn.store(Schema(name="Test Table", columns=cols, parent=self.project))

        # Initial data
        self.initial_data = [
            ['dataset', 10],
            ['analysis', 20],
            ['processed', 30],
            [None, 40]
        ]
        self.table = self.syn.store(Table(schema, self.initial_data))
        self.table_id = self.table.schema.id

    def tearDown(self):
        """
        Deletes the Synapse project created for testing.
        """
        if hasattr(self, 'project'):
            self.syn.delete(self.project)
        
    def test_update_table_integration(self):
        """
        Tests the update_table_column_with_corrections function against a real Synapse table.
        """
        # 1. Define the corrections. Per user feedback, None values should not be changed.
        corrections = [
            {'current_value': 'dataset', 'new_value': 'Data Set'},
            {'current_value': 'analysis', 'new_value': 'result'},
            {'current_value': 'processed', 'new_value': 'processed data'}
        ]

        # 2. Run the function to be tested
        update_table_column_with_corrections(self.syn, self.table_id, self.column_name, corrections)

        # 3. Verify the changes
        
        # Query the table to get the updated data
        results = self.syn.tableQuery(f"SELECT {self.column_name}, another_col FROM {self.table_id}")
        updated_df = results.asDataFrame(convert_to_datetime=True)

        # Create the expected DataFrame. The None value should remain.
        expected_data = {
            self.column_name: ['Data Set', 'result', 'processed data', None],
            'another_col': [10, 20, 30, 40]
        }
        expected_df = pd.DataFrame(expected_data)

        # Sort both dataframes to ensure comparison is not affected by row order
        updated_df_sorted = updated_df.sort_values(by=['another_col']).reset_index(drop=True)
        expected_df_sorted = expected_df.sort_values(by=['another_col']).reset_index(drop=True)
        
        # Use pandas testing utility to compare the dataframes
        pd.testing.assert_frame_equal(updated_df_sorted, expected_df_sorted)

if __name__ == '__main__':
    unittest.main() 