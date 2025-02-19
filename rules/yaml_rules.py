"""
    Contains custom rules to check YAML files present in AF: Airflow repos
"""
import yaml 

def check_compliance_rules(parsed_yaml):
    """Contains specific compliance rules for airflow yaml files.

    Args:
        parsed_yaml (dict): Key value pairs extracted after parsing yaml file.

    Returns:
        list: list of violations occured after check.
        str: if file is compliant or not
    """
    rules_violated=[]
    is_compliant=False
    try:
        if "dag_id" not in parsed_yaml:
            rules_violated.append("Missing 'dag_id' key")
        if "tags" not in parsed_yaml:
            rules_violated.append("Missing 'tags' key")
        if "schedule_interval" not in parsed_yaml:
            rules_violated.append("No interval scheduled for this job")
        if parsed_yaml.get("tasks",[])==[]:
            rules_violated.append("No tasks allotted")

    except yaml.YAMLError as e:
        print(f"Error checking YAML FILE= {parsed_yaml}: {e}")
    
    if not rules_violated:
        is_compliant=True

    return is_compliant, rules_violated
