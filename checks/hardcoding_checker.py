import re

def check_hardcoding(code_cells, config):
    """
    Check for hardcoding issues in the provided code cells.

    Compliance rules:
    - Detect file paths, database URLs, and sensitive keys.
    - Check for secrets if 'detect_secrets' is enabled in config.
    """
    hardcoding_issues = []
    path_pattern = r'(?:[A-Za-z]:\\|/)[^\s]*'  # Windows or Unix-style paths
    db_pattern = r"(?:jdbc|mysql|postgres|http|https)://[^\s]+"
    sensitive_key_pattern = r'["\'].*(api_key|apikey|token|password).*["\']'

    detect_secrets = config.get("hardcoding", {}).get("detect_secrets", False)
    warning_message = config.get("hardcoding", {}).get(
        "message", "Do not hardcode sensitive information. Use environment variables or a configuration file."
    )

    for idx, cell in enumerate(code_cells):
        for line_number, line in enumerate(cell, start=1):
            # Check for hardcoded file paths
            if re.search(path_pattern, line):
                hardcoding_issues.append(
                    f"Cell {idx + 1}, Line {line_number}: Hardcoded file path detected."
                )

            # Check for hardcoded database connection strings
            if re.search(db_pattern, line):
                hardcoding_issues.append(
                    f"Cell {idx + 1}, Line {line_number}: Hardcoded database URL detected."
                )

            # Check for hardcoded sensitive keys or passwords
            if re.search(sensitive_key_pattern, line, re.IGNORECASE):
                hardcoding_issues.append(
                    f"Cell {idx + 1}, Line {line_number}: Hardcoded sensitive value detected."
                )

            # Compliance message for secret detection
            if detect_secrets and (re.search(db_pattern, line) or re.search(sensitive_key_pattern, line)):
                hardcoding_issues.append(
                    f"Cell {idx + 1}, Line {line_number}: {warning_message}"
                )

    return hardcoding_issues
