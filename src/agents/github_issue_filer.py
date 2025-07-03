from crewai import Agent
import os
import subprocess
import getpass

class GitHubIssueFilerAgent:
    def __init__(self):
        self._check_gh_cli()

    def _check_gh_cli(self):
        """Checks if the GitHub CLI 'gh' is installed and authenticated."""
        try:
            # Check if gh is installed
            subprocess.run(["gh", "--version"], check=True, capture_output=True)
            # Check if user is logged in
            subprocess.run(["gh", "auth", "status"], check=True, capture_output=True)
            print("GitHub CLI ('gh') is installed and authenticated.")
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("\\n--- GitHub CLI Not Ready ---")
            print("The GitHub CLI tool ('gh') is required for this agent to function.")
            print("It seems that 'gh' is either not installed or you are not authenticated.")
            print("Please follow the instructions here to install and authenticate: https://cli.github.com/")
            raise ConnectionError("GitHub CLI not found or not authenticated.")

    def _format_issue_body(self, corrections, column_name):
        """Formats the list of corrections into a markdown string for the issue body."""
        body = (
            f"Automated-suggested corrections for the column **`{column_name}`**.\\n\\n"
            "A human should review these changes and apply them to the relevant files in this repository.\\n\\n"
            "---"
        )
        for correction in corrections:
            study_ids_str = ", ".join(correction['study_ids'])
            body += (
                f"\\n\\n### Correction for Study ID(s): `{study_ids_str}`\\n"
                f"- **Column**: `{correction['column_name']}`\\n"
                f"- **Action**: The following change is proposed:\\n"
                "\\n```diff\\n"
                f"- {correction['original_text']}\\n"
                f"+ {correction['corrected_text']}\\n"
                "```"
            )
        return body

    def run(self, task):
        """
        Runs the GitHub issue filing task.
        - Formats the issue content
        - Creates a new issue using the 'gh' CLI tool
        """
        repo_url = task['repo_url']
        corrections = task['corrections']
        
        # We can assume all corrections are for the same column in this batch
        column_name = corrections[0]['column_name'] if corrections else "N/A"

        print(f"\\n--- GitHub Issue Filing Workflow ---")
        print(f"Received task to file an issue for {len(corrections)} corrections in {repo_url}")

        issue_title = f"Freetext corrections for column '{column_name}'"
        issue_body = self._format_issue_body(corrections, column_name)

        try:
            command = [
                "gh", "issue", "create",
                "--title", issue_title,
                "--body", issue_body,
                "--repo", repo_url
            ]
            
            # Use subprocess.run to execute the command
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            
            issue_url = result.stdout.strip()
            print(f"\\n✅ Successfully created GitHub issue: {issue_url}")

        except subprocess.CalledProcessError as e:
            print(f"\\n❌ Failed to create GitHub issue.")
            print(f"Command failed with exit code {e.returncode}.")
            print(f"Stderr: {e.stderr}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return
        
        print("\\nGitHub issue filing process finished.") 