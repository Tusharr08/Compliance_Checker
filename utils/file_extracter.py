"""
    Contains Utility File Functions that:
    1- Extracts Data from .dbc archive files
    2- Converts .Python file to .ipynb format
    3- Cleans up any directory
"""
import zipfile
import os
import json
from nbformat import v4 as nbf
import nbformat

def extract_dbc_file(dbc_dir ='notebooks/dbc', json_dir='notebooks/dbc'):
    """Extracts Data from .dbc archive file

    Args:
        dbc_dir (str, optional): Path where .dbc files are stored. Defaults to 'notebooks/dbc'.
        json_dir (str, optional): Path to store JSON data. Defaults to 'notebooks/dbc'.
    """
    dbc_dir = os.path.abspath(dbc_dir)
    json_dir = os.path.abspath(json_dir)

    if not os.path.exists(json_dir):
        os.makedirs(json_dir)

    dbc_files = [f for f in os.listdir(dbc_dir) if f.endswith(".dbc")]
    print("Extracting DBC files:\n")
    for dbc_file in dbc_files:
        dbc_file_path = os.path.join(dbc_dir, dbc_file)

        print(f"Extracting {dbc_file_path}...\n")
        with zipfile.ZipFile(dbc_file_path, "r") as zip_ref:
            extracted_path = f"./notebooks/extracted/{os.path.splitext(dbc_file)[0]}"
            zip_ref.extractall(extracted_path)

        print(f"{dbc_file} extracted and saved to {extracted_path}!")
        python_to_ipynb_conversion(input_dir=extracted_path)
        #python_to_json(source_dir=extracted_path, json_dir='./notebooks/json')
        print("Function alerady called.")
        #os.remove(temp_zip_path)
        # ipynb_to_json(extracted_path, json_dir)
        #cleanup_dir(extracted_path)

def python_to_ipynb_conversion(input_dir, output_dir='./notebooks/ipynb'):
    """
    Converts extracted .py files to .ipynb files.
    
    Args:
        input_dir (str): Directory containing extracted .dbc files.
        output_dir (str): Directory to save the converted files.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".python"):
                file_path = os.path.join(root, file)
                print(f"Converting {file_path} to .ipynb format...")
                try:
                    with open(file_path,"r", encoding='utf-8') as f:
                        content = json.loads(f.read())

                    notebook = nbf.new_notebook()
                    cells =[]

                    for command in content.get("commands", []):
                        if command.get("subtype")=='command':
                            cell_type = "markdown" if command.get("command",'').startswith("%md") else "code"
                            cell_content = command.get("command",'').replace('%md\n','') if cell_type=='markdown' else command.get("command",'')
                            cell = nbformat.v4.new_markdown_cell(cell_content) if cell_type=='markdown' else nbf.new_code_cell(cell_content)
                            cells.append(cell)

                    notebook.cells= cells

                    nb_file_name = file.replace(".python",".ipynb")
                    nb_file_path = os.path.join(output_dir, nb_file_name)

                    with open(nb_file_path, "w", encoding='utf-8') as f:
                        nbformat.write(notebook, f)
                    print(f"Converted: {file} to {nb_file_name} and stored at {nb_file_path}!")
                except Exception as e:
                    print(f"Error occure while converting to ipynb: {e}")


def cleanup_dir(dir_path):
    """Removes teh directory mentioned in dir_path

    Args:
        dir_path (str): Directory path that is to be removed.
    """
    if os.path.exists(dir_path):
        for root, _, files in os.walk(dir_path, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            os.rmdir(root)
        print(f"Cleaned up directory: {dir_path}")
