import os
import time
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from src.dbr_client import  get_workspace_contents
from src.report_generator import generate_dbr_nb_report

load_dotenv('.env')

dbr_acc_list = ['gas-power-dev-dbr', 'hq-dev-dbr']

st.subheader(':globe_with_meridians: GE Vernova')
st.title("Databricks Compliance Dashboard")

dbr_account = st.selectbox('Select Databricks account:', dbr_acc_list)
if dbr_account:
    st.text("Currently Checking:")
    st.subheader(dbr_account)

# Display Databricks Workspace Contents
#workspace_contents = list_workspace_contents()
st.text("Workspace Contents:")


# Initialize session state to store workspace items
if 'workspace_items' not in st.session_state:
    st.session_state.workspace_items = get_workspace_contents()

# Display initial workspace items
df = pd.DataFrame(st.session_state.workspace_items)
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
        folder_df = pd.DataFrame(get_workspace_contents(st.session_state.current_folder))
        print(folder_df)
        if 'language' in folder_df.columns:
            folder_df_selected = folder_df[['object_type', 'path', 'language']]
        else:
            folder_df_selected = folder_df[['object_type', 'path']]
        st.dataframe(folder_df_selected)
        #st.experimental_rerun()
            

dbr_folder_path = st.text_input("Enter the path to the Databricks folder for compliance report generation:")

# Generate Report
if st.button("Generate Notebook Compliance Report"):
    if dbr_folder_path:
            try:
                with st.status("Generating report..."):
                    time.sleep(5)
                    st.write('Finding Notebooks...')
                    time.sleep(5)
                    st.write('Fetching Notebooks...')
                    time.sleep(2)
                    st.write('Linting Notebooks...')
                    report = generate_dbr_nb_report(dbr_folder_path)
                    report_df = pd.DataFrame(report)
                    st.write("### Compliance Report")
                    st.dataframe(report_df)
                st.success("Report generated successfully!")
            except Exception as e:
                st.error(f"Failed to generate report: {e}")
        
        

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
