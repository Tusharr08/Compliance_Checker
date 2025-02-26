from pylint.checkers import BaseChecker
from pylint.interfaces import IAstroidChecker
import astroid
import pandas as pd

# Rules with detailed message
rules_data = {
    'RuleID': [
        #'C1001', 'C1002', 'C1003', 'C1004', 'C1005', 'C1006', 'C1007', 'C1008', 'C1009', 'C1010', 'C1011', 
        'C1012', 'C1013'],
    'Description': [
        # """Prefer implicit column selection using F.col() or direct column names.",
        # "pyspark-column-selection",
        # "Use F.col('colA') instead of df.colA""",
        # """Refactor complex logical operations for better readability.",
        # "pyspark-logical-refactor",
        # "Avoid overly complex logical operations inside filter() or when().""",
        # """Use 'aliases' instead of withColumnRenamed().", 
        # "pyspark-rename-aliases", 
        # "Use alias()  instead of withColumnRenamed().""",
        # """Use cast() within select()  instead of withColumn().", 
        # "pyspark-cast-select", 
        # "Use  cast() within select() like df.select(F.col('').cast('')) instead of  withColumn().""",
        # """Use F.lit(None) instead of empty strings for empty columns.", 
        # "pyspark-lit-none", 
        # "Always use F.lit(None) to maintain semantic correctness.""",
        # """Hardcode value found. Use configuration files or environment variables instead.",
        # "hardcoded-value",
        # "Avoid hardcoding values like file paths or credentials directly in the code.""",
        # """Use explicit how parameter in joins",
        # "join-explicit-how",
        # "Ensure that the how parameter is explicitly specified in joins.""",
        # """Avoid right joins",
        # "avoid-right-joins",
        # "Avoid using right joins; use left joins instead.""",
        # """Avoid using .dropDuplicates() or .distinct() as a crutch",
        # "avoid-dropduplicates-distinct",
        # "Avoid using .dropDuplicates() or .distinct() to mask underlying issues with duplicate rows.""",
        # """Enable ignorenulls flag for analytic functions",
        # "enable-ignorenulls",
        # "Enable the ignorenulls flag for analytic functions to handle null values properly.""",
        """Avoid using empty partitionBy(), harms performance",
        "avoid-empty-partitionby",
        "Avoid using empty partitionBy() as it forces Spark to combine all data into a single partition.""",
        """Specify an explicit frame for window functions",
        "specify-explicit-frame",
        "Always specify an explicit frame when using window functions to avoid unpredictable behavior.""",
    ],
    'Query' : [
        """ 
def visit_call(self, node):
    if isinstance(node.func, astroid.Attribute):
        #Rule 12: Empty PartionBy()
        func_name = node.func.attrname
        if func_name == 'partitionBy':
            if not node.args:
                self.add_message('avoid-empty-partitionby', node=node)
        """,
        """
def visit_call(self, node):
    if isinstance(node.func, astroid.Attribute):
        #Rule 13: Window Functions
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

    options = {}

    def visit_call(self, node):
        """Checks function calls in PySpark code."""
        # Iterate over rules and apply each rule's logic
        for _, rule in rules_df.iterrows():
            exec(rule['Python Logic'])

    def count_logical_operators(self, expr):
        if isinstance(expr, astroid.BoolOp):
            return sum(self.count_logical_operators(v) for v in expr.values)
        if isinstance(expr, astroid.BinOp):
            return 1 + self.count_logical_operators(expr.left) + self.count_logical_operators(expr.right)
        if isinstance(expr, astroid.Compare):
            return 1
        return 0

# Registration function
    def register(self, linter):
        """Register the checker with Pylint."""
        linter.register_checker(CustomNBCodingStandards(linter))
