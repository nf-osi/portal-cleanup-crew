import pytest
import json
from unittest.mock import Mock
from src.tools.zooma_tools import ZoomaTermMappingTool, _extract_short_uri

@pytest.fixture
def zooma_tool():
    """Fixture to create an instance of the ZoomaTermMappingTool."""
    return ZoomaTermMappingTool()

@pytest.fixture
def mock_requests_get(mocker):
    """Fixture to mock requests.get."""
    return mocker.patch('requests.get')

# Mock API response for "plexiform neurofibroma"
mock_neurofibroma_response = [
    {
        "semanticTags": ["http://purl.obolibrary.org/obo/NCIT_C3797"],
        "derivedFrom": {
            "annotatedProperty": {
                "propertyValue": "Plexiform Neurofibroma"
            }
        }
    }
]

def test_extract_short_uri():
    """Test the URI extraction helper function."""
    assert _extract_short_uri("http://purl.obolibrary.org/obo/NCIT_C3797") == "NCIT_C3797"
    assert _extract_short_uri("http://a/b/c/D") == "D"
    assert _extract_short_uri("E") == "E"

def test_run_success(zooma_tool, mock_requests_get):
    """Test successful run returns a JSON object with term and URI."""
    # Arrange
    mock_response = Mock()
    mock_response.json.return_value = mock_neurofibroma_response
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response

    # Act
    result_json = zooma_tool._run(query="plexiform neurofibroma")
    result = json.loads(result_json)

    # Assert
    assert result == {"term": "Plexiform Neurofibroma", "uri": "NCIT_C3797"}
    mock_requests_get.assert_called_once_with(
        "http://www.ebi.ac.uk/spot/zooma/v2/api/services/annotate",
        params={"propertyValue": "plexiform neurofibroma"}
    )

def test_run_no_results(zooma_tool, mock_requests_get):
    """Test that an empty API response returns a JSON error."""
    # Arrange
    mock_response = Mock()
    mock_response.json.return_value = []
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response

    # Act
    result_json = zooma_tool._run(query="some rare term")
    result = json.loads(result_json)

    # Assert
    assert "error" in result
    assert result["error"] == "No ontology term found for the given query."

def test_run_malformed_response(zooma_tool, mock_requests_get):
    """Test that a malformed API response returns a JSON error."""
    # Arrange
    malformed_response = [{"confidence": "HIGH"}]  # Missing 'derivedFrom'
    mock_response = Mock()
    mock_response.json.return_value = malformed_response
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response

    # Act
    result_json = zooma_tool._run(query="test")
    result = json.loads(result_json)

    # Assert
    assert "error" in result
    assert result["error"] == "Could not extract term or URI from the API response." 