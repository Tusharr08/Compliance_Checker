import pytest
from unittest.mock import patch, mock_open
import yaml
from parsers.yaml_parser import parse_yaml_file, parse_yaml_content, lint_yaml_content

# Sample YAML content for testing
valid_yaml_content = """---
name: John Doe
age: 30
address:
  street: 123 Main St
  city: Anytown
  state: CA
"""

invalid_yaml_content = """
dag_id: btp.btp_vtt_daily  
tags: ['UAI3060967','Databricks','gas-power','btp','btp_dbr']
fsso: '502402774'
schedule_interval: '0 2 * * 0,1,2,3,4,5'
tz: 'Asia/Calcutta'

tasks:
- method_name: databricks_connection
  task_name: jb_btp_vtt_daily_terms_ref
  job_module: common.databricks_connection_module
  task_id: 1
  job_name: jb_btp_vtt_daily_terms_ref

- method_name: databricks_connection
  task_name: jb_btp_sourcesole_ref
  job_module: common.databricks_connection_module
  task_id: 2
  job_name: jb_btp_sourcesole_ref 
  depends_on: !!python/tuple [1,]
"""

# Test cases for parse_yaml_file function
def test_parse_yaml_file_success():
    with patch("builtins.open", mock_open(read_data=valid_yaml_content)):
        result = parse_yaml_file("dummy_path.yaml")
        assert result == {
            "name": "John Doe",
            "age": 30,
            "address": {
                "street": "123 Main St",
                "city": "Anytown",
                "state": "CA"
            }
        }

def test_parse_yaml_file_not_found():
    with patch("builtins.open", side_effect=FileNotFoundError):
        result = parse_yaml_file("dummy_path.yaml")
        assert isinstance(result, FileNotFoundError)

def test_parse_yaml_file_invalid_yaml():
    with patch("builtins.open", mock_open(read_data=invalid_yaml_content)):
        result = parse_yaml_file("dummy_path.yaml")
        assert isinstance(result, yaml.YAMLError)

# Test cases for parse_yaml_content function
def test_parse_yaml_content_success():
    result = parse_yaml_content(valid_yaml_content)
    assert result == {
        "name": "John Doe",
        "age": 30,
        "address": {
            "street": "123 Main St",
            "city": "Anytown",
            "state": "CA"
        }
    }

def test_parse_yaml_content_invalid_yaml():
    result = parse_yaml_content(invalid_yaml_content)
    assert "error" in result
    assert "Invalid YAML" in result["error"]

# Test cases for lint_yaml_content function
def test_lint_yaml_content_no_errors():
    result = lint_yaml_content(valid_yaml_content)
    
    assert result == []

def test_lint_yaml_content_with_errors():
    result = lint_yaml_content(invalid_yaml_content)
    assert len(result) > 0
    assert result[0]['level'] == 'error'

if __name__ == "__main__":
    pytest.main()
