import os
import json

NOTEBOOK_DIR = "./notebooks"


def parse_notebooks():
    """Parse all notebooks in the specified directory and return their code cells."""
    if not os.path.exists(NOTEBOOK_DIR):
        print(f"Directory '{NOTEBOOK_DIR}' does not exist.")
        return []

    notebooks_data = []
    for root, _, files in os.walk(NOTEBOOK_DIR):
        for file_name in files:
            if file_name.endswith(".ipynb"):
                notebook_path = os.path.join(root, file_name)
                print(f"Reading notebook: {notebook_path}")
                code_cells = parse_single_notebook(notebook_path)
                notebooks_data.append((file_name, code_cells))
    return notebooks_data


def parse_single_notebook(notebook_path):
    """Parse a single notebook and extract code cells."""
    with open(notebook_path, "r") as file:
        notebook_content = json.load(file)

    code_cells = [
        cell["source"]
        for cell in notebook_content.get("cells", [])
        if cell["cell_type"] == "code"
    ]
    return code_cells
