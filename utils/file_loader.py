import os

def list_notebook_files(directory):
    """List all Databricks Notebook files in a directory."""
    return [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith(".dbc") or file.endswith(".json")]

def load_notebook_content(file_path):
    """Read the contents of a Databricks notebook file."""
    with open(file_path, 'r') as file:
        return file.read()
