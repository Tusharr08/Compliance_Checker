from pylint.checkers import BaseChecker, BaseRawFileChecker
from pylint.lint import PyLinter
import astroid
import pandas as pd

# Rules with detailed message
rules_data = {
    'RuleID': ['C1007', 'C1012', 'C1013'],
    'Description': [
        """Hardcode value found. Use configuration files or environment variables instead.",
            "hardcoded-value",
            "Avoid hardcoding values like file paths or credentials directly in the code.""",
        """Avoid using empty partitionBy(), harms performance",
            "avoid-empty-partitionby",
            "Avoid using empty partitionBy() as it forces Spark to combine all data into a single partition.""",
        """Specify an explicit frame for window functions",
            "specify-explicit-frame",
            "Always specify an explicit frame when using window functions to avoid unpredictable behavior.""",
    ],
    'Query': [
        """
def visit_assign(self, node):
    # Check if the assigned value is a hardcoded string
    print(f"Checking asignment node: {node.as_string()}")
    if isinstance(node.value, astroid.Const) and isinstance(node.value.value, str):
        self.add_message('hardcoded-value', node=node, args=(node.value.value,))
        """,
        """ 
def visit_call(self, node):
    print(f"Checking call node: {node.as_string()}")
    if isinstance(node.func, astroid.Attribute):
        # Rule 12: Empty PartionBy()
        func_name = node.func.attrname
        if func_name == 'partitionBy':
            if not node.args:
                self.add_message('avoid-empty-partitionby', node=node)
        """,
        """
def visit_call(self, node):
    print(f"Checking call node: {node.as_string()}")
    if isinstance(node.func, astroid.Attribute):
        # Rule 13: Window Functions
        func_name = node.func.attrname
        if func_name == 'over':
            if not any(keyword.arg in ['rowsBetween', 'rangeBetween'] for keyword in node.keywords):
                self.add_message('specify-explicit-frame', node=node)
        """
    ]
}

rules_df = pd.DataFrame(rules_data)
print(rules_df)

class CustomNBCodingStandards(BaseChecker):
    """Contains custom Pyspark checks to apply on Notebooks.

    Args:
        BaseChecker (Class): Pylint Class for applying checks.
    """

    name = 'notebook-coding-standards'
    priority = -1
    
    # Create message format by splitting Description
    msgs = {
        row['RuleID']: (
            parts[0].strip().strip('"'),
            parts[1].strip(),
            parts[2].strip()
        ) 
        for _, row in rules_df.iterrows() 
        for parts in [row['Description'].split(',')]
    }
    print('Msgs-> \n', msgs)
    options = {}

    def __init__(self, linter):
        super().__init__(linter)
        for _, rule in rules_df.iterrows():
            print('Implementing...')
            exec(rule['Query'], {'self': self, 'astroid': astroid})
    # def visit_assign(self, node):
    #     """Check if the assigned value is a hardcoded string."""
    #     print('Assigned..')
    #     for _, rule in rules_df.iterrows():
    #         if rule['Query'].strip().startswith('def visit_assign'):
    #             #print(rule['Query'])
    #             exec(rule['Query'], globals(), locals(), )
    #             print('executed assign..')
    #             self.visit_assign_rule(node)
    
    # def visit_call(self, node):
    #     """Checks function calls in PySpark code."""
    #     #print('Called..')
    #     for _, rule in rules_df.iterrows():
    #         if rule['Query'].strip().startswith('def visit_call'):
    #             #print(rule['Query'])
    #             exec(rule['Query'], globals(), locals())
    #             self.visit_call_rule(node)

    # def visit_assign_rule(self, node):
    #     pass  # Placeholder for dynamically executed rule logic

    # def visit_call_rule(self, node):
    #     pass  # Placeholder for dynamically executed rule logic

# Registration function
def register(linter: PyLinter):
    """Register the checker with Pylint."""
    linter.register_checker(CustomNBCodingStandards(linter))
