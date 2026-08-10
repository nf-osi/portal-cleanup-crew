from crewai.tools import BaseTool
from pydantic import BaseModel, Field
import sys

class AskHumanForFeedbackInput(BaseModel):
    """Input for asking a human for feedback."""
    question: str = Field(description="The question to ask the human.")

class AskHumanForFeedbackTool(BaseTool):
    name: str = "Ask Human For Feedback Tool"
    description: str = (
        "Asks a human for feedback on a specific question. "
        "Use this when you need a human to review your work or provide additional information."
    )
    args_schema: type[BaseModel] = AskHumanForFeedbackInput

    def _run(self, question: str) -> str:
        """
        Asks a human for feedback.
        """
        try:
            print("\n" + "="*50)
            print("HUMAN REVIEW REQUESTED".center(50))
            print("="*50)
            print(f"\n{question}")
            
            # Use standard input() for a normal terminal experience
            feedback = input("> ")
            
            return feedback.strip()
        
        except Exception as e:
            return f"An error occurred while asking for feedback: {str(e)}" 