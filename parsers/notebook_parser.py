"""
    Contains Functions that parses Databricks Notebooks format.
"""
import os
import re
import json
import subprocess

PYLINTRC_PATH = ".pylintrc"

def parse_notebooks(notebook_dir):
    """Parse all notebooks in the specified directory and return their code cells."""
    if not os.path.exists(notebook_dir):
        print(f"Directory '{notebook_dir}' does not exist.")
        return []

    notebooks_data = []
    for root, _, files in os.walk(notebook_dir):
        for file_name in files:
            if file_name.endswith(".ipynb"):
                notebook_path = os.path.join(root, file_name)
                print(f"Reading notebook: {notebook_path}")
                code_cells = parse_single_notebook(notebook_path)
                notebooks_data.append((file_name, code_cells))
    return notebooks_data


def parse_single_notebook(notebook_path):
    """Parse a single notebook and extract code cells."""
    with open(notebook_path, "r", encoding='utf-8') as file:
        notebook_content = json.load(file)

    # code_cells = [
    #     cell["source"]
    #     for cell in notebook_content.get("cells", [])
    #     if cell["cell_type"] == "code"
    # ]
    code = notebook_content.get("cells", [])
    return code

def extract_code_cells(nb_json_content):
    """Extracts code blocks, especially Python, from Notebook.
    
    Args:
        nb_json_content (json): Contains the notebook data in JSON format.
    
    Returns:
        list: List of Python code in the form of strings.
    """
    python_cells = []
    current_line = 1

    print("Extracting Code...")
    for cell_number, cell in enumerate(nb_json_content.get("cells", []), start=1):
        if cell.get("cell_type") == "code":
            code = "".join(cell.get("source", []))
            num_lines = max(1, len(code.splitlines()))
            if not code.strip().startswith("%"):
                python_cells.append({
                    "cell_number": cell_number,
                    "code": code,
                    "start_line": current_line,
                    "end_line": current_line + num_lines - 1
                })
                current_line += num_lines  # Update the starting line for next block

    return python_cells

def lint_python_code(dbr_account, dbr_env, nb_path, code_cells):
    """Applies Pylint to the given code.
    
    Args:
        nb_path (str): Path where the notebook is stored in Databricks.
        code_cells (list): List of code cells extracted from the notebook.
    
    Returns:
        list: List of key fields obtained after linting.
    """
    
    #result_data = []
    # nb_name_pattern = re.compile(r'^nb_[a-z]{3}_[a-z0-9_]+$')
    nb_name = nb_path.split("/")[-1]
    # if not nb_name_pattern.match(nb_name):
    #     result_data.append({
    #         "Notebook Name": nb_name,
    #         "Status": "Non-Compliant",
    #         "Notebook Path": nb_path,
    #         "Cell Number": '',
    #         "Line in Cell": '',
    #         "Message ID": "C1102",
    #         "Type": "convention",
    #         "Symbol": "incorrect-notebook-name",
    #         "Description": "Notebook name does not follow the required format: nb_(3 digit sub-domain code)_(functional description), all snake-case."
    #     })

    temp_file_name = "temp.py"
    file_path = os.path.join(os.getcwd(), temp_file_name)
    with open(file_path, "w", encoding='utf-8') as temp_file:
        for block in code_cells:
            temp_file.write(f"{block['code']}\n")

    lint_data = []
    
    print(f'Linting {nb_path}')
    try:
        result = subprocess.run(
            ["pylint", temp_file_name, "--output-format=json", "--disable=C0303,C0103", f"--rcfile={PYLINTRC_PATH}"],
            capture_output=True,
            text=True,
            check=False
        )
        #print('result.stdout.strip()-> ',result.stdout.strip(),type(result.stdout.strip()))
        if result.stdout.strip() == '[]':
            print('Compliant')
            lint_data = [
                {
                    "Workspace" : dbr_account,
                    "Environment" : dbr_env,
                    "Domain" :  nb_path.split("/")[3],
                    "Notebook Name": nb_name,
                    "Status": "Compliant",
                    "Notebook Path": nb_path,
                    "Cell Number": '',
                    "Line in Cell": '',
                    "Message ID": '',
                    "Type": '',
                    "Symbol": '',
                    "Description": ''
                }
            ]
        else:
            print('Non-Compliant')
            lint_issues = json.loads(result.stdout)
            for issue in lint_issues:
                line_number = issue.get("line")
                if line_number:
                    for block in code_cells:
                        #print('block-> ',block)
                        if block["start_line"] <= line_number <= block["end_line"]:
                            block_line_no = line_number - block["start_line"] + 1
                            lint_data.append({
                                "Workspace" : dbr_account,
                                "Environment" : dbr_env,
                                "Domain" :  nb_path.split("/")[3],
                                "Notebook Name": nb_name,
                                "Status": "Non-Compliant",
                                "Notebook Path": nb_path,
                                "Cell Number": block['cell_number'],
                                "Line in Cell": block_line_no,
                                "Message ID": issue.get('message-id'),
                                "Type": issue.get('type'),
                                "Symbol": issue.get('symbol'),
                                "Description": issue.get('message')
                            })
        #print('lint_data-> \n',lint_data)
    except Exception as e:
        print(f"Error occurred in linting {nb_path}: {e} {e.args}")
    finally:
        os.remove(file_path)
    
    #result_data.extend(lint_data)
    return lint_data
