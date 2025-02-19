"""Main code for the Compliance Checker application.
"""
import os
from dotenv import load_dotenv
from parsers.yaml_parser import parse_yaml_file
from parsers.notebook_parser import parse_notebooks
from rules.yaml_rules import check_compliance_rules
from src.clone_repo import clone_repo
from src.report_generator import generate_yammlint_report, generate_dbr_nb_report, report_violations, generate_yaml_report_repowise
from utils.file_loader import  find_yaml_files, find_workflows_dir
from utils.file_extracter import extract_dbc_file

load_dotenv('.env')

repo_base_url=  os.getenv("GIT_ACCOUNT_URL")
dbr_base_url = os.getenv("DBR_WORKSPACE_URL")

#repo_list = 'GP_AF_BTP','GP_AF_SAL','GP_AF_SEG'
repo_list = ['GP_AF_SCF','GP_AF_FSR']
CLONED_DIR = 'cloned_repos'
WORKFLOW_DIR_NAME = 'workflows'
NB_DIR_NAME = 'notebooks'
ORG_NAME = os.getenv("ORG_NAME")

def process_repository(repo_name):
    """Clones the given repository, Finds workflow content, Finds the YAML files 
       present and check each file code for compliance.

    Args:
        repo_name (str): Name of the repository.
    """
    repo_url = f"{repo_base_url}/{repo_name}"
    print('repo_base_url:',repo_base_url)
    repo_clone_dir = os.path.join(CLONED_DIR, repo_name)

    if os.path.exists(repo_clone_dir):
        print(f"Repository {repo_name} already exists in {CLONED_DIR}.")
        print("Skipping Cloning...")
    else:
        print(f"Processing repository {repo_name}...")
        if not clone_repo(repo_url, repo_clone_dir):
            return
    workflows_dirs = find_workflows_dir(repo_clone_dir, WORKFLOW_DIR_NAME)
    if not workflows_dirs:
        return
    compliance_results=[]

    for workflows_dir in workflows_dirs:

        yaml_files = find_yaml_files(workflows_dir)
        if not yaml_files:
            return


        for yaml_file in yaml_files :
            print(f"Processing YAML file:{yaml_file}...")
            yaml_content = parse_yaml_file(yaml_file)
            yaml_file_name = os.path.basename(yaml_file)
            if isinstance(yaml_content, dict):
                compliance_results.append([repo_name,yaml_file_name, yaml_file,'Error',yaml_content])
            else:
                is_compliant, violations = check_compliance_rules(yaml_content)
                status = 'Compliant' if is_compliant else "Non-Compliant"
                compliance_results.append([repo_name,yaml_file_name, yaml_file,status,",".join(violations)])
    report_violations(repo_name, compliance_results)

def process_notebooks(nb_dir_name):
    """Extracting the code from the downloaded .dbc Notebook format 
       and parse each Notebook and extract Code Cells.

    Args:
        nb_dir_name (str): Folder name where the notebooks are stored.
    """
    print(f"Extracting Notebooks present in {nb_dir_name}/dbc")
    extract_dbc_file()

    nb_files_path = f"{nb_dir_name}/ipynb"
    print(f"Parsing Notebooks present in {nb_files_path}...")
    notebook_data = parse_notebooks(nb_files_path)
    #print(f"Notebook Data: \n {notebook_data}")
    if not notebook_data:
        print("No notebooks found for parsing.")
        return
    #ipynb_to_json(nb_files_path, f"{nb_dir_name}/json")

    for filename, code_cells in notebook_data:
        print(f"\nFile --> {filename}")
        print("Code Cells: \n")
        for code in code_cells:
            print(code,"\n")


def main():
    """
        Entry point for the application
    """


    generate_dbr_nb_report('/Workspace/Shared/GasPower/Abhishek Pal/BTP/Data Validation')
    generate_dbr_nb_report()
    # process_notebooks(NB_DIR_NAME)

    #generate_yammlint_report(ORG_NAME)
    #generate_yaml_report_repowise(ORG_NAME,'gp_btp_repo')

    # print('repo_base_url:',repo_base_url)
    # for repo in repo_list:

    #     process_repository(repo)

if __name__=="__main__":
    main()
