import os
import time
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from src.git_client import list_repositories, list_af_internal_repositories
from src.report_generator import generate_yammlint_report, generate_yaml_report_repowise

# Load environment variables
load_dotenv('.env')

# Set page configuration
st.set_page_config(page_title="YAML Compliance Dashboard", page_icon=":globe_with_meridians:", layout="wide")

# Main content
st.image(os.path.join(os.getcwd(), 'GE-Vernova-Emblem.png'), width=300)
st.title("YAML Compliance Dashboard")
st.markdown("### Ensure your YAML files are compliant with the latest standards.")

# Display organization name
org_name = os.getenv("ORG_NAME")
st.info(f"Logged into: **{org_name}**")

try:
    # List available repositories
    repos = list_repositories(org_name)
    all_repos_df = pd.DataFrame(repos)
    all_repos_df_selected = all_repos_df[['name', 'visibility', 'language', 'description']]

    # Display repositories in an expandable section
    with st.expander("Available Git Repositories"):
        st.dataframe(all_repos_df_selected)

    # Repository selection
    repo_list = ['All Repositories'] + all_repos_df_selected['name'].tolist()
    selected_repo = st.selectbox("Choose a repository to check compliance:", repo_list)

    # Generate Report button
    if st.button("Generate YAML Compliance Report"):
        with st.spinner("Generating report..."):
            if selected_repo != 'All Repositories':
                st.write(f"Scanning repository **{selected_repo}**...")
                time.sleep(2)
                st.write("Fetching repository contents...")
                time.sleep(2)
                st.write("Linting YAML files...")
                yaml_report_df = generate_yaml_report_repowise(org_name, selected_repo)
            else:
                st.write(f"Entering **{org_name}**...")
                time.sleep(2)
                st.write("Fetching repository list...")
                time.sleep(2)
                st.write("Scanning repositories...")
                yaml_report_df = generate_yammlint_report(org_name)
            
            st.success("Report Generated!")
            st.toast("YAML Compliance Report generated successfully!")
            st.subheader("Compliance Report for YAML Files")
            st.dataframe(yaml_report_df)

except Exception as e:
    st.error(f"An error occurred: {e}")

# Footer
st.markdown("---")
st.markdown("© 2025 GE Vernova. All rights reserved.")
