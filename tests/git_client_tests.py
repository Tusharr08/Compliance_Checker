import pytest
import os
from src.git_client import list_repositories, list_af_internal_repositories, get_repository, find_yml_files_in_repo, fetch_file_content

# Load environment variables
github_api_url = os.getenv("GITHUB_API_URL")
org_name = os.getenv("ORG_NAME")
token = os.getenv('NEW_TOKEN')
my_account_name = os.getenv('MY_ACC_ID')

# Sample data for testing
sample_org = "vernova-gp-dbr"
sample_repo_name = "gp_btp_repo"
sample_path = ""
sample_file_path = "path/to/sample.yml"


# Test cases for list_repositories function
def test_list_repositories():
    repos = list_repositories(sample_org)
    assert repos is not None
    assert isinstance(repos, list)

# Test cases for list_af_internal_repositories function
def test_list_af_internal_repositories():
    repos = list_af_internal_repositories(sample_org)
    assert repos is not None
    assert isinstance(repos, list)

# Test cases for get_repository function
def test_get_repository():
    repo_details = get_repository(sample_org, sample_repo_name)
    assert repo_details is not None
    assert isinstance(repo_details, dict)

# Test cases for find_yml_files_in_repo function
def test_find_yml_files_in_repo():
    yml_files = find_yml_files_in_repo(sample_org, sample_repo_name, sample_path)
    assert yml_files is not None
    assert isinstance(yml_files, list)

# Test cases for fetch_file_content function
def test_fetch_file_content():
    content = fetch_file_content(sample_org, sample_repo_name, sample_file_path)
    assert content is not None
    assert isinstance(content, str)

if __name__ == "__main__":
    pytest.main()
