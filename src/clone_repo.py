from git import Repo

work_dir_name = 'workflows'


def clone_repo(repo_url,clone_dir ):
    try:
        #auth_url = repo_url.replace("https://", f"https://{pat}@")

        print(f"Cloning the repository from {repo_url}...")
        Repo.clone_from(repo_url, clone_dir)
        print(f'Repository cloned successfully to {clone_dir}!')
        return True
    except Exception as e:
        print(f'Error cloning repo: {e}')
        return False
    

# def find_workflows_dir(base_dir, target_dir):
#     wf_directories=[]
#     print(f"Finding workflow directory in '{base_dir}'...")
#     for root, dirs, files in os.walk(base_dir):
#         if(target_dir) in dirs:
#             workflows_dir = os.path.join(root, target_dir)
#             wf_directories.append(workflows_dir)
#             print(f"Found '{target_dir}' directory at:{workflows_dir}")
            
#     if not wf_directories:
#         print(f"{target_dir} directory not found in the repository!")
    
#     return wf_directories

# def find_yaml_files(directory):
#     print(f"Finding YAML files in {directory}...")
#     yaml_files=[]
#     for root, dirs,files in os.walk(directory):
#         for file in files:
#             if file.endswith(".yml") or file.endswith(".yaml"):
#                 yaml_files.append(os.path.join(root,file))

#     print(f"Found {len(yaml_files)} YAML files in '{directory}'.")
#     return yaml_files
