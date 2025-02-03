import ast
from utils.rule_loader import load_rules

class ImportOrderChecker(ast.NodeVisitor):
    def __init__(self):
        self.imports = []
        self.rules = load_rules()

    def visit_Import(self, node):
        self.imports.append(('standard', node.lineno))
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        self.imports.append(('third_party', node.lineno) if node.module else ('project', node.lineno))
        self.generic_visit(node)

    def check_order(self):
        correct_order = self.rules["import_order"]["order"]
        # Custom logic to check order based on rules
        return "Import order violation" if self.imports != correct_order else "Import order is correct"
