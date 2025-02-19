import streamlit as st
import pandas as pd
from src.dbr_client import list_clusters, list_catalogs, get_cluster_details


st.set_page_config(page_title="Compliance Dashboard", layout="wide")
st.subheader(':globe_with_meridians: GE Vernova')

st.write("# Welcome to the GUIDE Compliance Dashboard")
st.write("Navigate using the sidebar to check compliance reports.")

if st.button("Click Available clusters"):
    with st.spinner("Fetching list of clusters"):
        clusters = pd.DataFrame(list_clusters())
        st.dataframe(clusters)
    st.success("Clusters Fetched!")

    cluster_list = clusters[['cluster_name']]
    cluster_name = st.selectbox("Select cluster to Fetch Cluster ID:", cluster_list)
    if cluster_name:
        cluster_id = get_cluster_details(cluster_name)
        st.markdown(f"### Selected Cluster: **{cluster_name}**")
        st.markdown(f"#### Cluster ID: `{cluster_id}`")

with st.expander("Click to check available catalogs in selected cluster:"):
    if 'cluster_id' in locals():
        catalogs = list_catalogs(cluster_id)
        st.dataframe(catalogs)
    else:
        st.warning("Please select a cluster first.")


    


