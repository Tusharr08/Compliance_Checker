import pytest
import os
import subprocess
import json
from unittest.mock import patch, mock_open, MagicMock

# Assuming the function lint_python_code is imported from the module
from parsers.notebook_parser import extract_code_cells, lint_python_code

# Mock data
valid_nb_path = "/Workspace/Shared/GasPower/Abhishek Pal/BTP/nb_fac_power_sql"
invalid_nb_path = "/Workspace/Shared/GasPower/Abhishek Pal/BTP/Data Validation"

compliant_notebook = {
  "cells": [
    {
      "cell_type": "code",
      "execution_count": 0,
      "metadata": {},
      "outputs": [],
      "source": [
        '"""Pyspark Code"""\n',
        "# Import necessary modules\n",
        "import math\n",
        "\n",
        "# Define a function to calculate the area of a circle\n",
        "def calculate_area(radius):\n",
        "    \"\"\"Calculate the area of a circle given its radius.\n",
        "    \n",
        "    Args:\n",
        "        radius (float): The radius of the circle.\n",
        "    \n",
        "    Returns:\n",
        "        float: The area of the circle.\n",
        "    \"\"\"\n",
        "    if radius < 0:\n",
        "        raise ValueError('Radius cannot be negative')\n",
        "    return math.pi * radius * radius\n",
        "\n",
        "# Calculate the area for a given radius\n",
        "rad = 5\n",
        "area = calculate_area(rad)\n",
        "print(f'The area of the circle with radius {rad} is {area:.2f}')"
      ]
    }
  ],
  "metadata": {},
  "nbformat": 4,
  "nbformat_minor": 2
}



non_compliant_notebook = {'cells': 
                          [{'cell_type': 'code', 'execution_count': 0, 'metadata': {'application/vnd.databricks.v1+cell': {'cellMetadata': {}, 'inputWidgets': {}, 'nuid': '86809c21-4be7-413e-bc6f-76cf56aaf9ea', 'showTitle': False, 'tableResultSettingsMap': {}, 'title': ''}}, 'outputs': [], 'source': ['%sql']}, 
                           {'cell_type': 'code', 'execution_count': 0, 'metadata': {'application/vnd.databricks.v1+cell': {'cellMetadata': {}, 'inputWidgets': {}, 'nuid': 'fdadebe8-9ef7-4fdd-8668-40d26d3aeb7d', 'showTitle': False, 'tableResultSettingsMap': {}, 'title': ''}}, 'outputs': [], 'source': []}, 
                           {'cell_type': 'code', 'execution_count': 0, 'metadata': {'application/vnd.databricks.v1+cell': {'cellMetadata': {'byteLimit': 2048000, 'rowLimit': 10000}, 'inputWidgets': {}, 'nuid': '648d680d-4d38-4e2b-bc26-516d2914c054', 'showTitle': False, 'tableResultSettingsMap': {}, 'title': ''}}, 'outputs': [{'output_type': 'stream', 'name': 'stdout', 'text': ['dev\n']}], 'source': ['import sys\n', ' \n', 'sys.path.append("/Workspace/Repos/Supply_Chain/gp_btp_repo")\n', ' \n', 'try:\n', '    # First attempt to import from the common path\n', '    from common.common_utils import *\n', 'except ImportError:\n', '    # If the first import fails, append the alternative path and try again\n', '    sys.path.append("/Workspace/Users/d44c1af5-e99e-4010-8302-464e74d13ba1/.bundle/BTP/files")\n', '    try:\n', '        from common.common_utils import *\n', '    except ImportError:\n', '        # If the second import fails, raise an ImportError\n', '        raise ImportError("Could not import \'common.common_utils\' from any of the provided paths.")']},
                           {'cell_type': 'code', 'execution_count': 0, 'metadata': {'application/vnd.databricks.v1+cell': {'cellMetadata': {'byteLimit': 2048000, 'rowLimit': 10000}, 'inputWidgets': {}, 'nuid': 'daf12e8c-7ef2-41ca-b57e-6946adb0e092', 'showTitle': False, 'tableResultSettingsMap': {}, 'title': ''}}, 'outputs': [{'output_type': 'execute_result', 'data': {'text/plain': ['DataFrame[]']}, 'execution_count': 2, 'metadata': {}}], 'source': ['createQuery = f"""CREATE OR REPLACE VIEW {vgp_catalog}.{btp_std_views}.btp_view_lpa_mvp2 as select * from  \n', '     {vgp_catalog}.{sot_btp}.btp_view_lpa_mvp2;"""\n', 'spark.sql(createQuery)']}, {'cell_type': 'code', 'execution_count': 0, 'metadata': {'application/vnd.databricks.v1+cell': {'cellMetadata': {'byteLimit': 2048000, 'rowLimit': 10000}, 'inputWidgets': {}, 'nuid': '17228e20-6943-4dbf-abb7-e2a0f1f747c9', 'showTitle': False, 'tableResultSettingsMap': {}, 'title': ''}}, 'outputs': [], 'source': ['alterQuery = f"""ALTER VIEW {vgp_catalog}.{btp_std_views}.btp_view_lpa_mvp2\n', ' SET TAGS (\'domain\' = \'Supply Chain\', \'Sub_domain_Code\' = \'btp\', \'sot_flag\' = \'Temporary SOT\', \'table_type\' = \'Secondary SOT\');"""\n', ' \n', '#Executing the Alter query\n', 'try:\n', '    spark.sql(alterQuery)\n', 'except Exception as e:\n', '    print(f"An error occurred: {e}")']}], 
                           'metadata': {'application/vnd.databricks.v1+notebook': {'computePreferences': None, 'dashboards': [], 'environmentMetadata': {'base_environment': '', 'environment_version': '1'}, 'language': 'python', 'notebookMetadata': {'pythonIndentUnit': 4}, 'notebookName': 'btp_view_lpa_mvp2_vw', 'widgets': {}},
                                         'language_info': {'name': 'python'}
                                        }, 
                            'nbformat': 4, 
                            'nbformat_minor': 0}

error_notebook = {
  "cells": [
    {
      "cell_type": "code",
      "source": [
        "import math\n",
        "\n",
        "def calculate_square_root(value):\n",
        "    if value < 0:\n",
        "        raise ValueError('Cannot calculate the square root of a negative number')\n",
        "    return math.sqrt(value)\n",
        "\n",
        "# This will trigger an exception\n",
        "result = calculate_square_root(-9)\n",
        "print(result)\n"
      ]
    }
  ],
  "metadata": {},
  "nbformat": 4,
  "nbformat_minor": 2
}



# Test cases for extract_code_cells
def test_extract_code_cells():

    code_cells = extract_code_cells(compliant_notebook)
    assert len(code_cells) == 1
    assert code_cells[0]['start_line'] == 1
    assert code_cells[0]['end_line'] == 22
    assert code_cells[0]['cell_number'] ==1

def test_valid_path_compliant_code():

    #with patch("subprocess.run", return_value = MagicMock(stdout=json.dumps([]))):
    code_cells = extract_code_cells(compliant_notebook) 
    result = lint_python_code(valid_nb_path, code_cells)
    print('ValidPathCOmpliantCode: \n',result)
    assert len(result) == 1
    assert result[0]["Status"] == "Compliant"


def test_invalid_path_compliant_code():
    code_cells = extract_code_cells(compliant_notebook)
    result = lint_python_code(invalid_nb_path, code_cells)
    print('Invalid path Compliant Code result->\n',result)
    assert len(result) >0
    assert result[0]['Status'] =="Non-Compliant"
    assert result[0]["Message ID"] == 'C1102'
    assert result[0]['Type'] == 'convention'
    assert result[1]['Status'] == 'Compliant'

def test_valid_path_non_compliant_code():
    
    code_cells = extract_code_cells(non_compliant_notebook)
    lint_results = lint_python_code(valid_nb_path, code_cells)

    assert isinstance(lint_results, list)
    assert len(lint_results) > 0
    assert all('Notebook Name' in result for result in lint_results)
    assert all('Status' in result for result in lint_results)
    assert all('Notebook Path' in result for result in lint_results)
    

    

def test_invalid_path_non_compliant_code():
    #with patch("subprocess.run", return_value = MagicMock(stdout=json.dumps([]))):
    code_cells = extract_code_cells(non_compliant_notebook) 
    lint_results = lint_python_code(invalid_nb_path, code_cells)
    print('InvalidpathNonComplaintCode->\n', lint_results)

    assert lint_results[0]['Message ID']== 'C1102'
    assert lint_results[0]['Status'] == 'Non-Compliant'
    assert lint_results[1]['Status'] == 'Non-Compliant'
    
    assert isinstance(lint_results, list)
    assert len(lint_results) > 0
    assert all('Notebook Name' in result for result in lint_results)
    assert all('Status' in result for result in lint_results)
    assert all('Notebook Path' in result for result in lint_results)

def test_empty_code():
    with patch("subprocess.run", return_value = MagicMock(stdout=json.dumps([]))): 
        result = lint_python_code(valid_nb_path,[])
    print('Empty Code->\n', result)
    assert len(result)==1
    assert result[0]['Status'] == 'Compliant'


def test_subprocess_failure(capfd):
    code_cells = extract_code_cells(error_notebook)
    with patch("subprocess.run", side_effect=Exception('Mocked Exception')):
        lint_python_code(valid_nb_path, code_cells)

        # Capture the output
        captured = capfd.readouterr()
        assert "Error occurred in linting" in captured.out
        assert "Mocked Exception" in captured.out


