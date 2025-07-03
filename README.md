# Synapse Curation and Management Agentic System

This project uses CrewAI to build an agentic system framework for the curation and management of data in Synapse.org data portals like the AMP-ALS portal and the NF Data Portal.

## Overview

The system is built in a modular fashion to solve different data portal curation and data management tasks. It consists of a central orchestrator agent and task-specific agents.

### Agent Workflow

Each task-specific agent follows this general pattern:

1.  **Investigate**: The agent investigates Synapse data portal assets for possible errors that it can fix.
2.  **Propose**: The agent proposes a fix for the identified errors.
3.  **Summarize & Approve**: The agent summarizes the exact changes that will be made and presents them to a human user for approval or rejection.
    -   If the user rejects the proposal, they can provide feedback for the agent to refine its proposal.
4.  **Apply Fix**: If the user approves the proposal, the agent applies the fix using the `synapseclient` Python package. 