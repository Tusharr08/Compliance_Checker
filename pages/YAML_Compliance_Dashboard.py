import os
import time
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from src.git_client import list_repositories, list_af_internal_repositories
from src.report_generator import generate_yammlint_report, generate_yaml_report_repowise

load_dotenv('.env')

org_list = ['vernova-gp-dbr', 'vernova-hq-dbr']

st.subheader(':globe_with_meridians: GE Vernova')
st.title("YAML Compliance Dashboard")

org_name = st.selectbox('Select Git Enterprise Account:', org_list)
if org_name:
    st.text("Currently Checking:")
    st.subheader(org_name)

# List available repos
repos = list_repositories(org_name)
internal_repos = list_af_internal_repositories(org_name)
# print(repos, internal_repos)
all_repos = {**repos, **internal_repos}
# print(type(all_repos))
# print(all_repos)
all_repos_df = pd.DataFrame(all_repos)
all_repos_df_selected = all_repos_df[['name', 'visibility','language', 'description']]
st.subheader("Available Git Repositories")
st.dataframe(all_repos_df_selected)

repo_list = ['All Repositories']
for repo in all_repos_df_selected['name']:
    repo_list.append(repo)

selected_repo = st.selectbox("Choose a repository to check compliance:",repo_list)

# Input repo name for report generation

# Generate Report
if st.button("Generate YAML Compliance Report"):

    if selected_repo!='All Repositories':
        with st.status("Generating report..."):
            time.sleep(1)
            st.write(f"Scanning repository {selected_repo}...")
            time.sleep(10)
            st.write(f"Repository Contents fetched successfully from {selected_repo}")
            time.sleep(1)
            st.write("Linting YAML files...")
            yaml_report_df = generate_yaml_report_repowise(org_name, selected_repo)
            st.subheader("Compliance Report for YAML Files")
            st.dataframe(yaml_report_df)
    else:
    
        with st.status("Generating report..."):
            time.sleep(1)
            st.write(f"Entering {org_name}...")
            time.sleep(1)
            st.write(f"Repository List fetched successfully from {org_name}")
            time.sleep(1)
            st.write("Scanning repositories...")
            yaml_report_df = generate_yammlint_report(org_name)
            st.subheader("Compliance Report for YAML Files")
            st.dataframe(yaml_report_df)
    st.success("Report Generated!")
