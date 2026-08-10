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
            print("\n--- GitHub CLI Not Ready ---")
            print("The GitHub CLI tool ('gh') is required for this agent to function.")
            print("It seems that 'gh' is either not installed or you are not authenticated.")
            print("Please follow the instructions here to install and authenticate: https://cli.github.com/")
            raise ConnectionError("GitHub CLI not found or not authenticated.")

    def _format_new_term_issue_body(self, new_term_suggestions, column_name):
        """Formats the list of new term suggestions into a markdown string for the issue body."""
        body = (
            f"The following new ontology terms have been proposed for the **`{column_name}`** attribute in the data model.\n\n"
            "This suggestion was approved during the data curation process. Please review and, if appropriate, add the new term(s) to the data model.\n\n"
            "---"
        )
        for suggestion in new_term_suggestions:
            body += (
                f"\n\n### Suggestion for Original Term: `{suggestion['original_term']}`\n"
                f"- **Suggested Term**: `{suggestion['standard_term']}`\n"
                f"- **Suggested URI**: `{suggestion['uri']}`"
            )
        return body

    def run(self, task):
        """
        Runs the GitHub issue filing task.
        - Formats the issue content for new term suggestions
        - Creates a new issue using the 'gh' CLI tool
        """
        repo_url = task.get('repo_url')
        column_name = task.get('column_name')
        new_term_suggestions = task.get('new_term_suggestions')

        if not all([repo_url, column_name, new_term_suggestions]):
            print("Error: Task is missing required fields ('repo_url', 'column_name', 'new_term_suggestions').")
            return

        print(f"\n--- GitHub Issue Filing Workflow for New Terms ---")
        print(f"Received task to file an issue for {len(new_term_suggestions)} new terms in '{column_name}'.")

        issue_title = f"New Term Suggestions for '{column_name}'"
        issue_body = self._format_new_term_issue_body(new_term_suggestions, column_name)

        try:
            command = [
                "gh", "issue", "create",
                "--title", issue_title,
                "--body", issue_body,
                "--repo", repo_url
            ]
            
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            
            issue_url = result.stdout.strip()
            print(f"\n✅ Successfully created GitHub issue: {issue_url}")

        except subprocess.CalledProcessError as e:
            print(f"\n❌ Failed to create GitHub issue.")
            print(f"Command failed with exit code {e.returncode}.")
            print(f"Stderr: {e.stderr}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            
        print("\nGitHub issue filing process finished.")

    def file_issue(self, title, body, repo):
        """
        Files a generic GitHub issue with the provided title, body, and repo.
        This is a simpler interface for general issue filing.
        """
        print(f"\n--- Filing GitHub Issue ---")
        print(f"Title: {title}")
        print(f"Repository: {repo}")

        try:
            command = [
                "gh", "issue", "create",
                "--title", title,
                "--body", body,
                "--repo", repo
            ]
            
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            
            issue_url = result.stdout.strip()
            print(f"\n✅ Successfully created GitHub issue: {issue_url}")
            return issue_url

        except subprocess.CalledProcessError as e:
            print(f"\n❌ Failed to create GitHub issue.")
            print(f"Command failed with exit code {e.returncode}.")
            print(f"Stderr: {e.stderr}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None