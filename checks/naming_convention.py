import re

def check_naming_conventions(code_cells):
    """
    Check for naming convention violations in functions and variables.

    Compliance Rules:
    - Function names should follow snake_case.
    - Variable names should also follow snake_case.
    """
    violations = []
    function_pattern = re.compile(r"def\s+([a-z_][a-z0-9_]*)(?=\()")
    variable_pattern = re.compile(r"\b([A-Z][A-Za-z0-9]*)\s*=")  # Detect CamelCase variables

    for idx, cell in enumerate(code_cells):
        for line_number, line in enumerate(cell, start=1):
            # Check for function naming convention
            for match in function_pattern.finditer(line):
                function_name = match.group(1)
                if not re.match(r"^[a-z_]+[a-z0-9_]*$", function_name):
                    violations.append(
                        f"Cell {idx + 1}, Line {line_number}: Function '{function_name}' does not follow snake_case naming convention."
                    )

            # Check for variable naming convention
            for match in variable_pattern.finditer(line):
                variable_name = match.group(1)
                if re.match(r"^[A-Z]", variable_name):
                    violations.append(
                        f"Cell {idx + 1}, Line {line_number}: Variable '{variable_name}' uses CamelCase. Use snake_case instead."
                    )

    return violations
