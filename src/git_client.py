"""
    Contains Functions that:
    1- Lists repositories under the given org name.
    2- Lists only INTERNAL repos under the given org name.
    3- Find all YML files present under given repository.
    4- Fetch YAML content from the given repo path.
"""
import os
import base64
import requests
from dotenv import load_dotenv
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ENV_PATH= '.env'
load_dotenv(ENV_PATH)

github_api_url= os.getenv("GITHUB_API_URL")
org_name = os.getenv("ORG_NAME")
token = os.getenv('NEW_TOKEN')
my_account_name = os.getenv('MY_ACC_ID')

headers ={
    "Authorization": f'Bearer {token}'
}

def list_repositories(org):
    """Lists all the Public and Private repos under the given organization name.

    Args:
        org (str): Name of the organization

    Returns:
        list: list of all repository details.
    """
    print('Listing Repositories...')
    url = f"{github_api_url}/orgs/{org}/repos"
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=20)
        #response.raise_for_status()
    except Exception as e:
        print(f"Error listing repositories: {e}")
    return response.json()

def list_af_internal_repositories(org):
    """List of all INTERNAL repos present under the given org name.

    Args:
        org (str): Name of the organization.

    Returns:
        list: List of all INTERNAL repository details.
    """
    print('Listing Repositories...')
    params ={
        "type": "internal"
    }
    url = f"{github_api_url}/orgs/{org}/repos"
    try:
        response = requests.get(url, headers=headers, params=params, verify=False, timeout=20)
        #response.raise_for_status()
    except Exception as e:
        print(f"Error fetching repositories: {e}")
    return response.json()

def get_repository(org, repo_name):
    """Lists all the Public and Private repos under the given organization name.

    Args:
        org (str): Name of the organization

    Returns:
        list: list of repository details.
    """
    print('Getting Repository Details...')
    url = f"{github_api_url}/repos/{org}/{repo_name}"
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=20)
        #response.raise_for_status()
    except Exception as e:
        print(f"Error fetching repositories: {e}")
    return response.json()


def find_yml_files_in_repo(org, repo_name, path=""):
    """Find all the YAML or YML files present under the given repo path.

    Args:
        org (str): Name of the organization.
        repo_name (str): Name of the repo.
        path (str, optional): Given path. Defaults to "".

    Returns:
        list: List of all YML/YAML files.
    """
    files=[]
    repo_url=f"{github_api_url}/repos/{org}/{repo_name}/contents/{path}"
    try:
        response = requests.get(repo_url, headers=headers, verify=False, timeout=20)
        response.raise_for_status()

        if response.status_code ==200:
            contents = response.json()
            for content in contents:
                if content["type"]=='file' and content['name'].endswith(('.yaml', '.yml')):
                    files.append(content['path'])
                elif content['type']=='dir':
                    files.extend(find_yml_files_in_repo(org, repo_name, content['path']))
        else:
            print(f'Failed to fetch  {repo_name}/{path}: {response.status_code} {response.text}')
        return files
    except Exception as e:
        print(f"Error fetching content of {repo_name}: {e}")
        return files

def fetch_file_content(org, repo, file):
    """Fetches content of the file from the given path.

    Args:
        org (str): Name of the organization.
        repo (str): Name of the repo.
        file (str): Path for file to be fetched.

    Returns:
        str: Decoded content of the file.
    """
    file_url = f"{github_api_url}/repos/{org}/{repo}/contents/{file}"
    try:
        response = requests.get(url=file_url, headers=headers, verify=False, timeout=20)
        response.raise_for_status()
        #if response.status_code ==200:
        content = response.json()
        return base64.b64decode(content['content']).decode('utf-8')
    except Exception as e:
        print(f"Error fetching file {org}/{repo}/{file}: {e}")
        return None
