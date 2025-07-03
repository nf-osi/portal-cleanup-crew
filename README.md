# Synapse Data Curation Assistant

This project provides an interactive command-line tool to assist with data curation and management tasks in Synapse.org data portals. It uses an AI-powered agentic system (built with CrewAI) to identify and correct metadata annotation errors.

## Features

- **Interactive Correction**: An interactive workflow to correct Synapse data annotations based on a provided data model (e.g., a JSON-LD schema).
- **AI-Powered Suggestions**: Uses a Large Language Model (LLM) to intelligently find and suggest corrections for invalid or non-standard annotation values.
- **User-in-the-Loop**: Puts the human user in control. The user reviews, modifies, and approves every change before it is applied to Synapse.
- **Configurable**: Easily configured to work with different Synapse views, data models, and LLMs.

## Annotation Correction Workflow

The primary workflow is for correcting annotations in a Synapse File View. It follows these steps:

1.  **Column Iteration**: The tool iterates through each column of the specified Synapse File View.
2.  **Agent Investigation**: For each column, an AI agent:
    a.  Determines the list of valid values from the linked data model.
    b.  Finds all unique values in the Synapse column.
    c.  Compares the two lists and generates a correction plan for any discrepancies (e.g., typos, non-standard terms).
3.  **Interactive Review**: The tool presents the agent's plan to the user. For each proposed change, the user can:
    -   **Accept** the suggestion.
    -   **Provide a different** correction.
    -   **Reject** the suggestion.
    The user can also provide corrections for values the agent couldn't map, or choose to skip them.
4.  **Final Approval**: After the review is complete, the tool presents a summary of all the changes that will be made. The user must give a final 'yes' to proceed.
5.  **Execution**: Upon approval, the tool executes the plan, updating all relevant entities in Synapse in parallel.

## Setup

1.  **Clone the repository and install dependencies**:
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    pip install -r requirements.txt
    ```

2.  **Configure Credentials**:
    -   Make a copy of `example_creds.yaml` and name it `creds.yaml`.
    -   Edit `creds.yaml` and add your API key for the LLM provider. The default is configured for OpenRouter.
        ```yaml
        # creds.yaml
        llm:
          credentials:
            OPENROUTER_API_KEY: "YOUR_API_KEY_HERE"
        ```

3.  **Configure the Tool**:
    -   Open `config.yaml`.
    -   Under `annotation_corrector`, set the `main_fileview` to the Synapse ID of the view you want to curate.
    -   Ensure the `data_model_url` points to the correct JSON-LD data model for your project.

4.  **Log in to Synapse**:
    -   The tool can use an existing Synapse configuration file (`~/.synapseConfig`). You can create one by running `synapse login` in your terminal and following the prompts.
    -   Alternatively, the tool will prompt you to enter your Synapse username and password/personal access token when it starts.

## Usage

Run the main script from the root of the project:

```bash
python src/main.py
```

The application will start, and you can select the "Correct Synapse Annotations" task from the menu to begin the workflow. 