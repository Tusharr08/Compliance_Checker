"""
    Contains Functions that:
    1- Parses YAML File present locally
    2- Parses Direct YAML content
    3- Applies Linting on YAML content
"""
import yaml
import yamllint
import yamllint.config
from yamllint.linter import run as yamllint_run

def parse_yaml_file(yaml_file_path):
    """Parses YAML file present on the given local file path.

    Args:
        yaml_file_path (str): File path where the file is stored.

    Returns:
        dict: key value pairs mentioned in yaml content
    """
    try:
        with open(yaml_file_path, 'r', encoding='utf-8') as yaml_content:
            parsed_yaml = yaml.safe_load(yaml_content)
            print(f"Successfully processed {yaml_file_path}")
            return parsed_yaml
    except FileNotFoundError as ef:
        print(f"Error file {yaml_file_path} not found!")
        return ef
    except yaml.YAMLError as e:
        print(f"Error parsing YAML FILE= {yaml_file_path}: {e}")
        return e
    # except yaml.YAMLError as e:
    #     rules_violated.append(f"YAML Parse Error:{e}")

    # return rules_violated

def parse_yaml_content(content):
    """Parses yaml content given.

    Args:
        content (str): yaml content fetched from git

    Returns:
        dict: key value pairs mentioned in yaml content
    """
    try:
        return yaml.safe_load(content)
    except yaml.YAMLError as e:
        return {"error": f"Invalid YAML : {str(e)}"}    
    
def lint_yaml_content(content):
    """Lints the yaml content given.

    Args:
        content (str): yaml content fetched from git

    Returns:
        list: list of key values obtained after linting the code.
    """
    lint_results =[]
    custom_config = """
    extends: default
    rules:
      trailing-spaces: disable
    """
    config = yamllint.config.YamlLintConfig(content=custom_config)

    errors = list(yamllint_run(content, config))
    #print("Errors:\n", errors)
    for error in errors:
        lint_results.append({
            "line": error.line,
            "column": error.column,
            "level": error.level,
            "rule" : error.rule,
            "description" : str(error)
        })
    #print('Lint_REsults: \n',lint_results)
    return lint_results
