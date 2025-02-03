from utils.rule_loader import load_rules

def check_error_handling(code_snippet):
    rules = load_rules()
    require_try_except = rules["error_handling"]["required"]

    if require_try_except and "try:" not in code_snippet:
        return ["Error handling is missing: no try-except block found"]
    return []
