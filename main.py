"""Main code for the Compliance Checker application.
"""
import os
from dotenv import load_dotenv
from src.report_generator import generate_yammlint_report, generate_dbr_nb_report, report_violations, generate_yaml_report_repowise

load_dotenv('.env')

repo_base_url=  os.getenv("GIT_ACCOUNT_URL")
dbr_base_url = os.getenv("DBR_WORKSPACE_URL")

#repo_list = 'GP_AF_BTP','GP_AF_SAL','GP_AF_SEG'
repo_list = ['GP_AF_SCF','GP_AF_FSR']

CLONED_DIR = 'cloned_repos'
WORKFLOW_DIR_NAME = 'workflows'
NB_DIR_NAME = 'notebooks'

org_name = os.getenv("ORG_NAME")

dbr_account = os.getenv('GP_DBR_DEV_NAME')
dbr_token = os.getenv("GP_DBR_DEV_TOKEN")
dbr_host = os.getenv("GP_DBR_DEV_HOST_URL")

def main():
    """
        Entry point for the application
    """
    #generate_dbr_nb_report('/Workspace/Shared/GasPower/Abhishek Pal/BTP/Data Validation')
    #generate_dbr_nb_report(dbr_account, dbr_host, dbr_token, '/Workspace/Shared/GasPower/Akshita Grover/btp_view_lpa_mvp2/Gold/btp_view_lpa_mvp2_vw')
    generate_dbr_nb_report(dbr_account, dbr_host, dbr_token,'/Users')

    # process_notebooks(NB_DIR_NAME)

    #generate_yammlint_report(ORG_NAME)
    #generate_yaml_report_repowise(ORG_NAME,'gp_btp_repo')

    # print('repo_base_url:',repo_base_url)
    # for repo in repo_list:

    #     process_repository(repo)

if __name__=="__main__":
    main()
