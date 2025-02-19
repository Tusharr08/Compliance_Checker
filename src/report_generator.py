"""
    Contains functions that:
    1- Generates report for cloned repos.
    2- Generate report for YAML file checking.
    3- Generates report for Databricks Notebooks file checking.
"""
import json
import pandas as pd
import streamlit as st
from parsers.yaml_parser import lint_yaml_content #, parse_yaml_content
from parsers.notebook_parser import lint_python_code, extract_code_cells
#from rules.yaml_rules  import check_compliance_rules
from src.dbr_client import find_notebooks, fetch_file_from_workspace
from src.git_client import list_repositories, list_af_internal_repositories, get_repository , find_yml_files_in_repo, fetch_file_content


def report_violations(repo_name, violations):
    """Generates csv report for yaml files present in cloned repos.

    Args:
        repo_name (str): Name of the repository.
        violations (list): List of violations after checking for compliance.
    """
    print(f"Creating Report for '{repo_name}'...")
    df = pd.DataFrame(violations, columns=["Repository","YAML File","YAML File Path","Status","Violations"])
    report_path = f"./reports/{repo_name}_report.csv"
    try:
        df.to_csv(report_path, index=False)
    except Exception as e:
        print(f"Unable to save file at {report_path} due to this error: {e}")
    print(f"Report for {repo_name} saved in '{report_path}'.")

def generate_yammlint_report(org):
    """Generates csv report after linting all YAML files.

    Args:
        org (str): Name of the organization

    Returns:
        list: list of violations
    """
    report=[]
    print(f"Entering {org}..")
    repos = list_repositories(org)
    internal_repos = list_af_internal_repositories(org)
    all_repos = repos + internal_repos
    print("Repository List fetched successfully.\n")
    for repo in all_repos:
        repo_name = repo['name']
        #custom_checks =''
        print(f"Scanning repository: {repo_name}...")
        yaml_files = find_yml_files_in_repo(org, repo_name)
        print(f" {len(yaml_files)} YAML files fetched successfully from {org}/{repo_name}.")
        for yaml_file in yaml_files:
            print(f"Linting file: {org}/{repo_name}/{yaml_file}...")
            content = fetch_file_content(org, repo_name, yaml_file)
            #print(type(content))
            # if repo['visibility']=='internal' and 'AF' in repo_name :
            #     yaml_data = parse_yaml_content(content)
            #     is_compliant, custom_checks = check_compliance_rules(yaml_data)
            #     custom_checks =''.join(custom_checks)
            if content:
                lint_results = lint_yaml_content(content)
                #print(f"lint_results: \n {lint_results}")
                if not lint_results:
                    report_entry = {
                            "Repository": repo_name,
                            "Type" : repo['visibility'],
                            "YAML File Name" : yaml_file.split("/")[-1],
                            "YAML File Path" : f"{org}/{repo_name}/{yaml_file}",
                            "YAML File Link" : f"https://github.build.ge.com/{org}/{repo_name}/{yaml_file}",
                            "Status" : "Compliant",
                            "Line Number" : '',
                            "Level" : '',
                            "Rule" : '',
                            "Description" : ''
                        }
                else:
                    for line in lint_results:
                        report_entry = {
                            "Repository": repo_name,
                            "Type" : repo['visibility'],
                            "YAML File Name" : yaml_file.split("/")[-1],
                            "YAML File Path" : f"{org}/{repo_name}/{yaml_file}",
                            "YAML File Link" : f"https://github.build.ge.com/{org}/{repo_name}/{yaml_file}",
                            "Line Number" : str(line["line"]),
                            "Level" : line['level'],
                            "Rule" : line['rule'],
                            "Description" : line['description']
                        }
                        #print(report_entry)
                report.append(report_entry)

    report_df = pd.DataFrame(report)
    # report_path = "./reports/YAMLLINT_report.csv"

    # try:
    #     df.to_csv(report_path, index=False)
    # except Exception as e:
    #     print(f"Unable to save file at {report_path} due to this error: {e}")
    # print(f"Report for {org} saved in '{report_path}'.")
    return report_df

def generate_dbr_nb_report(path):
    """Generates csv report for the Databricks Notebook Code.

    Args:
        path (str): Path in which notebooks have to searched and checked.

    Returns:
        list: list of violations
    """

    print(f"Now finding all Databricks Notebooks present in {path}..")
    notebook_paths = find_notebooks(path)
    print(f"Total {len(notebook_paths)} notebooks detected!")
    dbr_report = []
    #status = 'Non-Compliant'
    for nb_path in notebook_paths:
        try:
            file_content = fetch_file_from_workspace(nb_path)
            nb_json_content= json.loads(file_content)
            #print("Type of Content:",nb_json_content)
            code_cells = extract_code_cells(nb_json_content)
            #print(code_cells)
            results = lint_python_code(nb_path, code_cells)
            for result in results:
                dbr_report.append(result)

        except Exception as e:
            print(f"Error processing file {nb_path}: {e}")
            dbr_report.append({
                    "Notebook Name": nb_path.split("/")[-1],
                    "Status" : "Error",
                    "Notebook Path" : nb_path,
                    "Cell Number" : '',
                    "Line in Cell" : '',
                    "Message ID" : '',
                    "Type" : 'error',
                    "Symbol" : '',
                    "Description" : e
                })

    dbr_report_df = pd.DataFrame(dbr_report)
    print(dbr_report_df)
    report_path = f"./reports/{nb_path.split('/')[-1]}.csv"
    try:
        dbr_report_df.to_csv(report_path, index=False)
        print(f"Report for {path} saved in '{report_path}'.")
    except Exception as e:
        print(f"Unable to save file at {report_path} due to this error: {e}")

    return  dbr_report_df

def generate_yaml_report_repowise(org, repo_name):

    report=[]
    repo = get_repository(org, repo_name)
    print(f"Scanning repository: {repo_name}...")
    yaml_files = find_yml_files_in_repo(org, repo_name)
    print(f" {len(yaml_files)} YAML files fetched successfully from {org}/{repo_name}.")
    for yaml_file in yaml_files:
        print(f"Linting file: {org}/{repo_name}/{yaml_file}...")
        content = fetch_file_content(org, repo_name, yaml_file)

        if content:
            lint_results = lint_yaml_content(content)
            #print(f"lint_results: \n {lint_results}")
            if not lint_results:
                report_entry = {
                        "Repository": repo_name,
                        "Type" : repo['visibility'],
                        "YAML File Name" : yaml_file.split("/")[-1],
                        "YAML File Path" : f"{org}/{repo_name}/{yaml_file}",
                        "YAML File Link" : f"https://github.build.ge.com/{org}/{repo_name}/{yaml_file}",
                        "Status" : "Compliant",
                        "Line Number" : '',
                        "Level" : '',
                        "Rule" : '',
                        "Description" : ''
                    }
            else:
                for line in lint_results:
                    report_entry = {
                        "Repository": repo_name,
                        "Type" : repo['visibility'],
                        "YAML File Name" : yaml_file.split("/")[-1],
                        "YAML File Path" : f"{org}/{repo_name}/{yaml_file}",
                        "YAML File Link" : f"https://github.build.ge.com/{org}/{repo_name}/{yaml_file}",
                        "Status" : "Non-Compliant",
                        "Line Number" : str(line["line"]),
                        "Level" : line['level'],
                        "Rule" : line['rule'],
                        "Description" : line['description']
                    }
                    #print(report_entry)
            report.append(report_entry)

    report_df = pd.DataFrame(report)
    # report_path = f"./reports/{repo_name}_report.csv"

    # try:
    #     report_df.to_csv(report_path, index=False)
    # except Exception as e:
    #     print(f"Unable to save file at {report_path} due to this error: {e}")
    # print(f"Report for {org} saved in '{report_path}'.")
    return report_df