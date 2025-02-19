import pytest
import pandas as pd
from src.report_generator import report_violations, generate_yammlint_report, generate_dbr_nb_report, generate_yaml_report_repowise

# Test cases for report_violations function
def test_report_violations(tmpdir):
    repo_name = "test_repo"
    violations = [
        ["test_repo", "test.yaml", "/path/to/test.yaml", "Non-Compliant", "Description of the violation"]
    ]
    report_violations(repo_name, violations)
    report_file = tmpdir.join(f"{repo_name}_report.csv")
    report_violations(repo_name, violations)
    assert report_file.check(), "Report file was not created."
    dataframe = pd.read_csv(report_file)
    assert not dataframe.empty, "Report file is empty."

# Integration test for generate_yammlint_report function
def test_generate_yammlint_report():
    org = "test_org"
    report_df = generate_yammlint_report(org)
    assert isinstance(report_df, pd.DataFrame), "Output is not a DataFrame."
    assert not report_df.empty, "DataFrame should not be empty if there are yaml files."

# Integration test for generate_dbr_nb_report function
def test_generate_dbr_nb_report():
    path = "/"
    report_df = generate_dbr_nb_report(path)
    assert isinstance(report_df, pd.DataFrame), "Output is not a DataFrame."
    assert not report_df.empty, "DataFrame should not be empty if there are notebooks."

# Integration test for generate_yaml_report_repowise function
def test_generate_yaml_report_repowise():
    org = "test_org"
    repo_name = "test_repo"
    report_df = generate_yaml_report_repowise(org, repo_name)
    assert isinstance(report_df, pd.DataFrame), "Output is not a DataFrame."
    assert not report_df.empty, "DataFrame should not be empty if there are yaml files in the repo."

if __name__ == "__main__":
    pytest.main()
