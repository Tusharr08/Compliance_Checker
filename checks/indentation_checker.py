from utils.rule_loader import load_rules

def check_indentation(code_snippet):
    rules = load_rules()
    required_indent = rules["indentation"]["spaces_per_indent"]

    violations = []
    for line_num, line in enumerate(code_snippet.split("\n"), start=1):
        if line.startswith(" ") and (len(line) - len(line.lstrip())) % required_indent != 0:
            violations.append(f"Line {line_num}: Incorrect indentation")
    return violations
