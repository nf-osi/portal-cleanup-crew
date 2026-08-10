import requests
import json
from crewai.tools import BaseTool
from typing import Optional, Type
from pydantic import BaseModel, Field


class ZoomaInput(BaseModel):
    """Input model for the ZOOMA Term Mapper tool."""
    query: str = Field(description="The text to be annotated with an ontology term.")

def _extract_short_uri(uri: str) -> str:
    """Extracts the short form of a URI (e.g., 'NCIT_C3797')."""
    return uri.split('/')[-1]

class ZoomaTermMappingTool(BaseTool):
    name: str = "ZOOMA Term Mapper"
    description: str = "Maps a given text to the best-matching ontology term and URI using the ZOOMA API."
    args_schema: Type[BaseModel] = ZoomaInput
    llm: Optional[any] = None

    def __init__(self, llm: any = None, **kwargs):
        super().__init__(**kwargs)
        if llm:
            self.llm = llm

    def _run(self, query: str) -> str:
        """Use the ZOOMA API to find the best ontology term for a given text."""
        base_url = "http://www.ebi.ac.uk/spot/zooma/v2/api"
        endpoint = "/services/annotate"
        params = {"propertyValue": query}

        try:
            response = requests.get(f"{base_url}{endpoint}", params=params)
            response.raise_for_status()
            results = response.json()

            if not results:
                return json.dumps({"error": "No ontology term found for the given query."})

            best_match = results[0]
            try:
                term = best_match['derivedFrom']['annotatedProperty']['propertyValue']
                uri = best_match['semanticTags'][0]
                short_uri = _extract_short_uri(uri)
                
                return json.dumps({"term": term, "uri": short_uri})
            except (KeyError, TypeError, IndexError):
                return json.dumps({"error": "Could not extract term or URI from the API response."})

        except requests.exceptions.RequestException as e:
            return json.dumps({"error": f"An error occurred while calling the ZOOMA API: {e}"}) 