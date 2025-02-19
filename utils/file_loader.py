"""
    File Utility Functions
"""
import os

def find_workflows_dir(base_dir, target_dir):
    """Find workflow directory in cloned repo.

    Args:
        base_dir (str): directory in which to search
        target_dir (str): directory to search

    Returns:
        list: list of files
    """
    wf_directories=[]
    print(f"Finding workflow directory in '{base_dir}'...")
    for root, dirs, _ in os.walk(base_dir):
        if target_dir in dirs:
            workflows_dir = os.path.join(root, target_dir)
            wf_directories.append(workflows_dir)
            print(f"Found '{target_dir}' directory at:{workflows_dir}")
    if not wf_directories:
        print(f"{target_dir} directory not found in the repository!")
    return wf_directories

def find_yaml_files(directory):
    """Finds all YAML/YML files in the given directory.

    Args:
        directory (str): Directory path.

    Returns:
        list: List of YAML/YML files.
    """
    print(f"Finding YAML files in {directory}...")
    yaml_files=[]
    for root,_ ,files in os.walk(directory):
        for file in files:
            if file.endswith(".yml") or file.endswith(".yaml"):
                yaml_files.append(os.path.join(root,file))

    print(f"Found {len(yaml_files)} YAML files in '{directory}'.")
    return yaml_files
