import os
import streamlit as st

# Set up the page configuration
st.set_page_config(page_title="Compliance Dashboard", layout="wide")

# Display the header and subheader
st.image(os.path.join(os.getcwd(), 'GE-Vernova-Emblem.png'), width=300)
st.write("# Welcome to the GUIDE Compliance Dashboard")
st.write("Navigate using the sidebar to check compliance reports.")

# Main content
st.write("## :rocket: Compliance Dashboard Overview")
st.write("""
Welcome to the GUIDE Compliance Dashboard. This platform provides a comprehensive overview of compliance standards and best practices for working with notebooks and code in the GE Vernova environment. Use the sidebar to navigate through the various sections and ensure adherence to the guidelines.
""")

st.write("### Notebook Standards")
st.write("""
- **Naming Conventions**: Follow the specified naming conventions for functional and common notebooks.
- **Notebook Code Templates**: Ensure your notebooks include a title, description, configuration setups, imports, data loads, business/use case UDFs, and follow the provided templates.
- **Configuration Parameters**: Use the correct format for global and custom parameters, and ensure notebooks and logic are properly configured.
""")

st.write("### Python/PySpark Coding Practices")
st.write("""
- **Names to Avoid**: Avoid using ambiguous or reserved names.
- **Imports**: Organize imports logically.
- **Consistent Indentation**: Maintain consistent indentation throughout your code.
- **Variable and Function/Methods**: Use descriptive names for variables and functions/methods.
- **Comments and Documentation**: Include comments and documentation to explain your code.
- **Modularity and Reusability**: Write modular and reusable code.
- **Error Handling**: Implement robust error handling.
- **Avoid Hardcoding**: Avoid hardcoding values; use configuration files or parameters instead.
- **Summaries**: Provide summaries for complex logic.
""")

st.write("### General Recommendations")
st.write("""
- **Implicit Column Selection**: Prefer implicit column selection to direct access.
- **Refactor Complex Logic**: Refactor complex logical operations for clarity.
- **Schema Contract**: Use select statements to specify a schema contract.
- **Aliases**: Use aliases for readability.
- **Empty Columns**: Handle empty columns appropriately.
""")

st.write("### Performance Optimization")
st.write("""
- **User-Defined Function (UDF)**: Optimize UDFs for performance.
- **Joins**: Use efficient join strategies.
- **Window Functions**: Optimize window functions.
- **Dealing with Nulls**: Handle null values efficiently.
- **Partitioning**: Use partitioning strategies to optimize performance.
- **Delta Lake**: Use Delta Lake features like OPTIMIZE with ZORDER and liquid clustering.
- **Photon**: Utilize Photon for performance improvements.
- **Caching**: Use caching to speed up repeated queries.
""")

st.write("### Code Audit Checklist and Evaluation Criteria")
st.write("Ensure your code meets the specified audit checklist and evaluation criteria.")

st.write("### STD View Creation Standards")
st.write("Follow the standards for creating STD views.")

st.write("### GitHub and CI/CD Standards")
st.write("Adhere to the GitHub and CI/CD standards for version control and continuous integration/continuous deployment.")
