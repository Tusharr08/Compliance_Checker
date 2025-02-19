import pytest
import os
from src.dbr_client import get_cluster_details, get_workspace_contents, list_clusters, list_catalogs, list_workspace_contents, find_notebooks, fetch_file_from_workspace

# Load environment variables
workspace_url = os.getenv("DBR_WORKSPACE_URL")
token = os.getenv("DATABRICKS_TOKEN")

# Sample data for testing
sample_cluster_name = "pwr-dev-cluster"
sample_cluster_id = "test-cluster-id"
sample_path = "/"
incorrect_notebook_path = "/Users/test-path"
correct_notebook_path = '/Users'
sample_notebook_path = "/Workspace/Users/tushar.gupta1@ge.com/Notebooks/Sample_Notebook"

# Test cases for get_cluster_details function
def test_get_cluster_details():
    cluster_id = get_cluster_details(sample_cluster_name)
    assert cluster_id is not None
    assert isinstance(cluster_id, str)

# Test cases for get_workspace_contents function
def test_get_workspace_contents():
    contents = get_workspace_contents(sample_path)
    assert contents is not None
    assert isinstance(contents, list)

# Test cases for fetching contents of workspace with wrong path
def test_get_workspace_contents_wrong_path():
    contents = get_workspace_contents(incorrect_notebook_path)
    assert contents is None

# Test cases for list_clusters function
def test_list_clusters():
    clusters = list_clusters()
    assert clusters is not None
    assert isinstance(clusters, list)

# Test cases for list_catalogs function
def test_list_catalogs():
    sample_cluster_id = get_cluster_details(sample_cluster_name)
    catalogs = list_catalogs(sample_cluster_id)
    assert catalogs is not None
    assert isinstance(catalogs, list)

# Test cases for list_workspace_contents function
def test_list_workspace_contents():
    contents = list_workspace_contents(sample_path)
    assert contents is not None
    assert isinstance(contents, list)

# Test cases for listing workspace contents using incorrect path
def test_list_workspace_contents_wrong_path():
    contents = list_workspace_contents(incorrect_notebook_path)
    assert contents is None

# Test cases for find_notebooks function
def test_find_notebooks():
    notebooks = find_notebooks(correct_notebook_path)
    assert notebooks is not None
    assert isinstance(notebooks, list)

# Test cases for finding notebooks in worng path
def test_find_notebooks_wrong_path():
    nbs = find_notebooks(path=incorrect_notebook_path, notebooks=[])
    print(nbs)
    assert nbs == []
    assert isinstance(nbs, list)

# Test cases for fetch_file_from_workspace function
def test_fetch_file_from_workspace():
    content = fetch_file_from_workspace(sample_notebook_path)
    assert content is not None
    assert isinstance(content, str)

if __name__ == "__main__":
    pytest.main()
