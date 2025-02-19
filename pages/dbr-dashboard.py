import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from src.dbr_client import  get_workspace_contents
from src.report_generator import generate_dbr_nb_report

load_dotenv('.env')

dbr_acc_list = ['gas-power-dev-dbr', 'hq-dev-dbr']

st.subheader('🌎 GE Vernova')
st.title("🚀 Databricks Compliance Dashboard")

dbr_account = st.selectbox('Select Databricks account:', dbr_acc_list)
if dbr_account:
    st.text("Currently Checking:")
    st.subheader(dbr_account)

def navigate_to_directory(path):
    st.session_state.navigation_history.append(st.session_state.current_path)
    st.session_state.current_path = path

def display_folder_contents(account, path):
    contents  = get_workspace_contents(path)
    # for item in contents:
    #     if item["object_type"] == 'DIRECTORY':
    #         if st.button(f"🗂️ {item['path']} {item['object_type']}"):
                
    #     else:
    #             st.write(f"📄 {item['path']}")

    if contents:

        df = pd.DataFrame(contents)
        df['path_button'] = df.apply(
            lambda row: row['path'] if row['object_type'] == "DIRECTORY" else None, axis=1
        )

        edited_df = st.data_editor(
            df[['object_type', 'path', 'path_button']],
            column_config={
                "path_button": st.column_config.TextColumn(
                    "path",
                    help="Click to enter the folder"
                ),
                "object_type": "Type"
            }
        )

        for i, row in df.iterrows():
            if row['object_type'] == 'DIRECTORY':
                if st.session_state.
        if edited_df['path_button'] is not None:
            if isinstance(edited_df['path_button'], str):
                navigate_to_directory(edited_df['path_button'])

if "current_path" not in st.session_state:
    st.session_state.current_path = "/"
if "navigation_history" not in st.session_state:
    st.session_state.navigation_history =[]

with st.container():
    # Display Databricks Workspace Contents
    st.text(f"🔍 Workspace Contents for account {dbr_account}:")

    if st.session_state.navigation_history:
        if st.button("👈🏼 Back"):
            st.session_state.current_path = st.session_state.navigation_history.pop()

    #Display Current Path
    st.write(f"╰┈➤ Current Directory: `{st.session_state.current_path}` ")

    display_folder_contents(dbr_account, st.session_state.current_path)
