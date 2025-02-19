def compare_notebooks(template, existing):
    compliance_report = []
    for i, template_cell in enumerate(template):
        if i < len(existing):
            existing_cell = existing[i]
            if template_cell['cell_type'] != existing_cell['cell_type']:
                compliance_report.append(f"Cell {i+1}: Cell type mismatch. Expected {template_cell['cell_type']}, found {existing_cell['cell_type']}.")
            if 'source' in template_cell and 'source' in existing_cell:
                if template_cell['source'][0].strip() != existing_cell['source'][0].strip():
                    compliance_report.append(f"Cell {i+1}: Source content mismatch. Expected '{template_cell['source'][0].strip()}', found '{existing_cell['source'][0].strip()}'.")
        else:
            compliance_report.append(f"Cell {i+1}: Missing cell in existing notebook.")
    return compliance_report