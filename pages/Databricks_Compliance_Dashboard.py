import os
import time
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from src.dbr_client import get_workspace_contents
from src.report_generator import generate_dbr_nb_report

# Load environment variables
load_dotenv('.env')

# Set page configuration
st.set_page_config(page_title="Databricks Compliance Dashboard", page_icon=":globe_with_meridians:", layout="wide")

# Main content
st.image(os.path.join(os.getcwd(), 'GE-Vernova-Emblem.png'), width=300)
st.title("Databricks Compliance Dashboard")
st.markdown("### Ensure your Databricks notebooks are compliant with the latest standards.")

workspaces = {
    os.getenv('GP_DBR_DEV_NAME'):{
        "host": os.getenv('GP_DBR_DEV_HOST_URL'),
        "token": os.getenv('GP_DBR_DEV_TOKEN')
    },
    os.getenv('HQ_DBR_DEV_NAME'):{
        "host": os.getenv('HQ_DBR_DEV_HOST_URL'),
        "token": os.getenv('HQ_DBR_DEV_TOKEN')
    },
    os.getenv('NUCLEAR_DBR_DEV_NAME'):{
        "host": os.getenv('NUCLEAR_DBR_DEV_HOST_URL'),
        "token": os.getenv('NUCLEAR_DBR_DEV_TOKEN')
    },
    os.getenv('GP_DBR_PROD_NAME'):{
        "host": os.getenv('GP_DBR_PROD_HOST_URL'),
        "token": os.getenv('GP_DBR_PROD_TOKEN')
    },
    os.getenv('HQ_DBR_PROD_NAME'):{
        "host": os.getenv('HQ_DBR_PROD_HOST_URL'),
        "token": os.getenv('HQ_DBR_PROD_TOKEN')
    },
    os.getenv('NUCLEAR_DBR_PROD_NAME'):{
        "host": os.getenv('NUCLEAR_DBR_PROD_HOST_URL'),
        "token": os.getenv('NUCLEAR_DBR_PROD_TOKEN')
    },
}

# Databricks account selection
dbr_account = st.selectbox('Select Databricks account:', list(workspaces.keys()))
if dbr_account:
    workspace_config = workspaces[dbr_account]
    dbr_host = workspace_config["host"]
    dbr_token = workspace_config["token"]
    
    st.info(f"Currently Checking: **{dbr_account}**")
    st.markdown(f"➤ {dbr_host}")

# Display Databricks Workspace Contents
st.markdown(f"#### 🔍Workspace Contents for account {dbr_account}:")

# Initialize session state to store workspace items
if 'workspace_items' not in st.session_state:
    st.session_state.workspace_items = get_workspace_contents(dbr_host, dbr_token)

# Display initial workspace items
df = pd.DataFrame(st.session_state.workspace_items)

#st.write(f"--➤ Current Directory: `{st.session_state.current_folder}`")
df_selected = df[['object_type', 'path']]
st.data_editor(
    df_selected,
    column_config={
        "Items:": st.column_config.ListColumn(
            "Workspace Items",
            width="medium",
        ),
    },
    hide_index=True,
)

# Text input for folder
folder = st.text_input('Enter folder to list contents or type "exit" to stop:')

# Update folder contents based on user input
if folder:
    if folder.lower() == 'exit':
        st.write("Exiting...")
    else:
        st.session_state.current_folder = '/' + folder
        folder_df = pd.DataFrame(get_workspace_contents(dbr_host, dbr_token, st.session_state.current_folder))
        if 'language' in folder_df.columns:
            folder_df_selected = folder_df[['object_type', 'path', 'language']]
        else:
            folder_df_selected = folder_df[['object_type', 'path']]
        st.dataframe(folder_df_selected)

# Input for Databricks folder path
dbr_folder_path = st.text_input("Enter the path to the Databricks folder for compliance report generation:")

# Generate Report
if st.button("Generate Notebook Compliance Report"):
    if dbr_folder_path:
        with st.spinner("Generating report..."):
            time.sleep(10)
            st.write('Finding Notebooks...')
            time.sleep(10)
            st.write('Fetching Notebooks...')
            time.sleep(10)
            st.write('Linting Notebooks...')
            report = generate_dbr_nb_report(dbr_account, dbr_host, dbr_token, dbr_folder_path)
            report_df = pd.DataFrame(report)
            st.write("### Compliance Report")
            st.dataframe(report_df)
        st.success("Report generated successfully!")
        st.toast("Notebook Compliance Report generated successfully!")

        # Display Statistics
        st.write("### Compliance Statistics")
        if not report_df.empty:
            st.write(f"**Total Violations:** {len(report_df)}")
            st.write(f"**Distinct Notebooks:** {report_df['Notebook Name'].nunique()}")
            st.write(f"**Violation Types:** {report_df['Type'].nunique()} types")

            # Plotting Type of Violations
            violation_counts = report_df['Type'].value_counts()
            st.bar_chart(violation_counts)
        else:
            st.warning("Please enter a valid path.")

# Footer
st.markdown("---")
st.markdown("© 2025 GE Vernova. All rights reserved.")
