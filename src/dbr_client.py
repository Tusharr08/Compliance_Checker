"""
    Contains Functions that:
    1- Gets list of Catalogs.
    2- gets list of Clusters.
    3- Gets list of Workspace Contents.
    4- Gets details of a cluster.
    5- Finds any Notebooks present in any folder.
    6- Get content of a file from Workspace.
"""
import os 
import base64
import requests
from dotenv import load_dotenv


load_dotenv('.env')

workspace_url = os.getenv("DBR_WORKSPACE_URL")

token = os.getenv("DATABRICKS_TOKEN")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

def get_cluster_details(cluster_name):
    """get the details of any cluster.

    Args:
        cluster_name (str): name of the cluster

    Returns:
        str: cluster id of the given cluster
    """
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

def get_workspace_contents(path="/"):
    """Get the list of files/folders in the given Workspace path

    Args:
        path (str, optional): Path for which the contents are to be listed. Defaults to "/".

    Returns:
        list: list of files/folders present
    """
    workspace_endpoint = f"{workspace_url}/api/2.0/workspace/list"
    params = {"path": path}
    response = requests.get(workspace_endpoint, headers=headers, params=params, timeout=20)
    if response.status_code == 200:
        objects = response.json().get("objects", [])
        return objects
    print(f"Error in fetching Workspace Contents: {response.status_code} - {response.text}")
    return None

def list_clusters():
    """
        Prints all the clusters present on the Databricks Platform
    """
    endpoint= f"{workspace_url}/api/2.1/clusters/list"
    #print(endpoint, token)
    response = requests.get(endpoint, headers=headers, timeout=20)
    if response.status_code == 200:
        clusters = response.json().get("clusters", [])
        #print("Clusters:")
        # for cluster in clusters:
        #     print(f"- {cluster['cluster_name']}")
    else:
        print(f"Error in listing clusters: {response.status_code} - {response.text}")
    return clusters

def list_catalogs(cluster_id):
    """Prints all the catalogs present under Unity Catalog.

    Args:
        cluster_id (str): id of the cluster that has Unity Catalog enabled.
    """
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

def list_workspace_contents(path="/"):
    """Lists all the files/folders present in Workspace.

    Args:
        path (str, optional): Path to the folder in Workspace. Defaults to "/".

    Returns:
        List: List of objects in the path.
    """
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

def find_notebooks(path="/", notebooks=[]):
    """Find all Notebooks present in the given path.

    Args:
        path (str, optional): Path in which notebooks has to be searched. Defaults to "/".
        notebooks (list, optional): List of paths of all notebooks. Defaults to [].

    Returns:
        list: List of notebooks present in the given path.
    """
    #print('path-> ',path)
    objects = get_workspace_contents(path)
    #print('objects->\n',objects, 'notebooks:\n', notebooks)
    if objects is None:
        #print(f"Error in finding Notebook: Path ({path}) doesn't exist.")
        return notebooks

    for obj in objects:
        if obj['object_type'] == 'NOTEBOOK':
            notebooks.append(obj['path'])
            print(obj['path'])
        elif obj['object_type'] == 'DIRECTORY':
            find_notebooks(obj['path'], notebooks)
    return notebooks


def fetch_file_from_workspace(path):
    """Fetches the content of file present at path.

    Args:
        path (str): Path from where the file has to be fetched.

    Raises:
        Exception: In case fetching fails.

    Returns:
        str: Content of the file after decoding.
    """
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
    