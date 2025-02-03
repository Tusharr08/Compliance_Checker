import yaml

def load_rules():
    """Load compliance rules from YAML."""
    with open("config/compliance_rules.yaml", "r") as file:
        return yaml.safe_load(file)
