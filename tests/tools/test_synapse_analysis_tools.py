import unittest
import json
from unittest.mock import patch, mock_open
from src.tools.synapse_analysis_tools import CreateAnnotationTemplateCSVTool

class TestCreateAnnotationTemplateCSVTool(unittest.TestCase):

    @patch('src.tools.synapse_analysis_tools._load_jsonld')
    def test_get_template_attributes(self, mock_load_jsonld):
        # Mock the JSON-LD data
        mock_jsonld_data = {
            "@graph": [
                {
                    "@id": "bts:RNASeqTemplate",
                    "rdfs:label": "RNASeqTemplate",
                    "sms:requiresDependency": [
                        {"@id": "bts:Assay"},
                        {"@id": "bts:DataType"}
                    ]
                },
                {
                    "@id": "bts:Assay",
                    "rdfs:label": "Assay",
                    "sms:displayName": "assay",
                    "rdfs:comment": "The type of assay"
                },
                {
                    "@id": "bts:DataType",
                    "rdfs:label": "DataType",
                    "sms:displayName": "dataType",
                    "rdfs:comment": "The type of data"
                }
            ]
        }
        mock_load_jsonld.return_value = mock_jsonld_data

        tool = CreateAnnotationTemplateCSVTool()
        attributes = tool._get_template_attributes(mock_jsonld_data, "RNASeqTemplate")

        expected_attributes = [
            {"id": "bts:Assay", "label": "assay", "description": "The type of assay"},
            {"id": "bts:DataType", "label": "dataType", "description": "The type of data"}
        ]

        self.assertEqual(attributes, expected_attributes)

if __name__ == '__main__':
    unittest.main() 