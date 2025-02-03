from parser.notebook_parser import parse_notebooks
from checks.error_handling import check_error_handling
from checks.import_order import ImportOrderChecker
from checks.indentation_checker import check_indentation
from checks.naming_convention import check_naming_conventions
from checks.hardcoding_checker import check_hardcoding

def main():
    """Main function to run compliance checks on all parsed notebooks."""
    notebooks_data = parse_notebooks()

    if not notebooks_data:
        print("No notebooks found for parsing.")
        return

    for file_name, code_cells in notebooks_data:
        print(f"\nProcessing notebook: {file_name}")

        # Use functions from the checks directory
        violations = []
        violations.extend(check_naming_conventions(code_cells))
        violations.extend(check_indentation(code_cells))
        violations.extend(check_hardcoding(code_cells))

        if violations:
            print(f"Violations in {file_name}:")
            for violation in violations:
                print(f"  - {violation}")
        else:
            print(f"No violations found in {file_name}.")

if __name__=="__main__":
    main()