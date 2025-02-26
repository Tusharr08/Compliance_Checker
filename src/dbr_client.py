"""
    Contains Functions that:
    1- Gets list of Catalogs.
    2- gets list of Clusters.
    3- Prints list of Workspace Contents.
    4- Get list of Workspace Contents.
    5- Gets details of a cluster.
    6- Finds any Notebooks present in any folder.
    7- Fetch content of a file from Workspace.
    8- Fetch GUIDE rules for Notebooks.
"""
import os
import base64
import requests
from dotenv import load_dotenv


load_dotenv('.env')

hq_dbr_host = os.getenv('HQ_DBR_DEV_HOST_URL')
hq_dbr_token = os.getenv('HQ_DBR_DEV_TOKEN')



def get_cluster_details(cluster_name, workspace_url, token):
    """get the details of any cluster.

    Args:
        cluster_name (str): name of the cluster
        workspace_url : host url of the calling workspace

    Returns:
        str: cluster id of the given cluster
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    clusters_url = f"{workspace_url}/api/2.1/clusters/list"
    response = requests.get(clusters_url, headers=headers, timeout=20)
    if response.status_code == 200:
        clusters = response.json().get("clusters", [])
        for cluster in clusters:
            if cluster['cluster_name'] == cluster_name:
                #print(f"{cluster_name} -> {cluster['cluster_id']}")
                return cluster['cluster_id']
    else:
        print(f"Error in Getting Cluster Details: {response.status_code} - {response.text}")
    return None

def get_workspace_contents(workspace_url, token,  path="/"):
    """Get the list of files/folders in the given Workspace path

    Args:
        path (str, optional): Path for which the contents are to be listed. Defaults to "/".

    Returns:
        list: list of files/folders present
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    workspace_endpoint = f"{workspace_url}/api/2.0/workspace/list"
    params = {"path": path}
    #print('\n', workspace_url, '\n', workspace_endpoint, '\n', token ,'\n', params)
    response = requests.get(workspace_endpoint, headers=headers, params=params, timeout=20)
    if response.status_code == 200:
        objects = response.json().get("objects", [])
        return objects
    print(f"Error in fetching Workspace Contents: {response.status_code} - {response.text}")
    return None

def list_clusters( workspace_url, token):
    """
        Prints all the clusters present on the Databricks Platform
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    endpoint= f"{workspace_url}/api/2.1/clusters/list"
    #print(endpoint, token)
    print(f'Finding Clusters in {workspace_url} ...')
    response = requests.get(endpoint, headers=headers, timeout=20)
    if response.status_code == 200:
        clusters = response.json().get("clusters", [])
        #print("Clusters:")
        # for cluster in clusters:
        #     print(f"- {cluster['cluster_name']}")
    else:
        print(f"Error in listing clusters: {response.status_code} - {response.text}")
    return clusters

def list_catalogs(workspace_url, token, cluster_id):
    """Prints all the catalogs present under Unity Catalog.

    Args:
        cluster_id (str): id of the cluster that has Unity Catalog enabled.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    catalogs_url= f"{workspace_url}/api/2.1/unity-catalog/catalogs"
    params = {"cluster_id": cluster_id}
    #print(cluster_id, "\n",catalogs_url, "\n", params)
    response = requests.get(catalogs_url, headers=headers, params=params, timeout=20)
    if response.status_code == 200:
        catalogs = response.json().get("catalogs", [])
        #print("Catalogs:")
        #for catalog in catalogs:
            #print(f"- {catalog['name']}")
    else:
        print(f"Error in listing catalogs: {response.status_code} - {response.text}")
    return catalogs

def list_workspace_contents(workspace_url, token, path="/"):
    """Lists all the files/folders present in Workspace.

    Args:
        path (str, optional): Path to the folder in Workspace. Defaults to "/".

    Returns:
        List: List of objects in the path.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    workspace_endpoint = f"{workspace_url}/api/2.0/workspace/list"
    params = {"path": path}
    response = requests.get(workspace_endpoint, headers=headers, params=params, timeout=20)
    if response.status_code == 200:
        objects = response.json().get("objects", [])
        print(f"{path} Contents:")
        for obj in objects:
            print(f"- {obj['path']} ({obj['object_type']})")
        return objects
    
    print(f"Error in listing Workspace Contents: {response.status_code} - {response.text}")
    return None

def find_notebooks(workspace_url, token, path="/", notebooks=[]):
    """Find all Notebooks present in the given path.

    Args:
        path (str, optional): Path in which notebooks has to be searched. Defaults to "/".
        notebooks (list, optional): List of paths of all notebooks. Defaults to [].

    Returns:
        list: List of notebooks present in the given path.
    """
    #print('path-> ',path)
    objects = get_workspace_contents(workspace_url, token, path)
    #print('objects->\n',objects, 'notebooks:\n', notebooks)
    if objects is None:
        #print(f"Error in finding Notebook: Path ({path}) doesn't exist.")
        return notebooks

    for obj in objects:
        if obj['object_type'] == 'NOTEBOOK':
            notebooks.append(obj['path'])
            print(obj['path'])
        elif obj['object_type'] == 'DIRECTORY':
            find_notebooks(workspace_url, token, obj['path'], notebooks)
    return notebooks

def fetch_file_from_workspace(workspace_url, token, path):
    """Fetches the content of file present at path.

    Args:
        path (str): Path from where the file has to be fetched.

    Raises:
        Exception: In case fetching fails.

    Returns:
        str: Content of the file after decoding.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    url = f"{workspace_url}/api/2.0/workspace/export"
    params={
        "path":path,
        "format": "JUPYTER"
    }
    print(f"Fetching {path}...")
    response =  requests.get(url=url, headers=headers, params=params, timeout=20)
    response.raise_for_status()

    if response.status_code==200:
        encrypted_content = response.json().get('content')
        #print(encrypted_content)
        decoded_content = base64.b64decode(encrypted_content).decode('utf-8')
        #print('Type of file fetched: ',type(decoded_content))
        return decoded_content
    raise Exception(f"Error fetching workspace file :{response.json().get('content')}")
    
def fetch_hq_guide_rules(hq_workspace_url, hq_token ):
    """
    Fetches HQ guide rules from the Databricks workspace.

    Returns:
        dict: The JSON response containing the guide rules.
    """
    query = 'SELECT * FROM vhqd.ing_gov.gov_guide_rules_ing;'
    hq_host_url = f"{hq_workspace_url}/api/2.0/sql/statements"
    hq_headers = {
        "Authorization": f"Bearer {hq_token}",
        "Content-Type": "application/json"
    }
    gev_hq_gov_sql_dev = "82bb445d7683f43e"
    payload = {
        "statement": query,
        "warehouse_id": gev_hq_gov_sql_dev,
        "catalog": "vhqd",
        "format": "JSON_ARRAY",
        "disposition": "INLINE",
        "wait_timeout": "30s"
    }
    print(f"Fetching custom rules from {hq_host_url}...")
    try:
        query_resp = requests.post(
            url=hq_host_url,
            headers=hq_headers,
            json=payload
        )
        query_resp.raise_for_status()  # Raise an exception for HTTP errors
        return query_resp.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to fetch HQ guide rules: {e}")

