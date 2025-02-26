"""
Contains custom checks with corresponding codes, applied on Notebooks.
"""
from pylint.checkers import BaseChecker, BaseRawFileChecker
from pylint.lint import PyLinter
import astroid

class CustomNBCodingStandards(BaseChecker):
    """Contains custom PySpark checks to apply on Notebooks.

    Args:
        BaseChecker (Class): Pylint Class for applying checks.
    """

    name = 'notebook-coding-standards'
    priority = -1
    msgs = {
        "C1001": (
            "Prefer implicit column selection using F.col() or direct column names.",
            "pyspark-column-selection",
            "Use F.col('colA') instead of df.colA"
        ),
        "C1002": (
            "Refactor complex logical operations for better readability.",
            "pyspark-logical-refactor",
            "Avoid overly complex logical operations inside filter() or when()."
        ),
        "C1003": (
            "Use select at the start or end of a transformation for schema consistency.",
            "pyspark-schema-contract",
            "Always use a select statement to prepare the dataframe for the next step."
        ),
        "C1004": (
            "Use 'aliases' instead of withColumnRenamed().",
            "pyspark-rename-aliases",
            "Use alias() instead of withColumnRenamed()."
        ),
        "C1005": (
            "Use cast() within select() instead of withColumn().",
            "pyspark-cast-select",
            "Use cast() within select() like df.select(F.col('').cast('')) instead of withColumn()."
        ),
        "C1006": (
            "Use F.lit(None) instead of empty strings for empty columns.",
            "pyspark-lit-none",
            "Always use F.lit(None) to maintain semantic correctness."
        ),
        "C1007": (
            "Hardcode value found. Use configuration files or environment variables instead.",
            "hardcoded-value",
            "Avoid hardcoding values like file paths or credentials directly in the code."
        ),
        "C1008": (
            "Use explicit how parameter in joins.",
            "join-explicit-how",
            "Ensure that the how parameter is explicitly specified in joins."
        ),
        "C1009": (
            "Avoid right joins.",
            "avoid-right-joins",
            "Avoid using right joins; use left joins instead."
        ),
        "C1010": (
            "Avoid using .dropDuplicates() or .distinct() as a crutch.",
            "avoid-dropduplicates-distinct",
            "Avoid using .dropDuplicates() or .distinct() to mask underlying issues with duplicate rows."
        ),
        "C1011": (
            "Enable ignorenulls flag for analytic functions.",
            "enable-ignorenulls",
            "Enable the ignorenulls flag for analytic functions to handle null values properly."
        ),
        "C1012": (
            "Avoid using empty partitionBy(), harms performance.",
            "avoid-empty-partitionby",
            "Avoid using empty partitionBy() as it forces Spark to combine all data into a single partition."
        ),
        "C1013": (
            "Specify an explicit frame for window functions.",
            "specify-explicit-frame",
            "Always specify an explicit frame when using window functions to avoid unpredictable behavior."
        ),
    }

    options = {}

    def visit_assign(self, node):
        # Rule 7: Check if the assigned value is a hardcoded string
        if isinstance(node.value, astroid.Const) and isinstance(node.value.value, str):
            self.add_message('C1007', node=node)

    # def visit_module(self, node):
    #     """Check if module name is in small-case or not"""
    #     module_name = node.name.split('.')[-1]
    #     if '_' not in module_name or not module_name.islower():
    #         self.add_message('module-naming-convention', node=node)

    # def visit_import(self, node):
    #     """Check the import case"""
    #     self._check_imported_module_names(node)

    # def visit_importfrom(self, node):
    #     """Checks the from x import y"""
    #     self._check_imported_module_names(node)

    def visit_call(self, node):
        """Checks function calls in PySpark code."""
        #print('\nNode:',node,'\n')

        if isinstance(node.func, astroid.Attribute):
            func_name = node.func.attrname
            # Rule 1: Prefer F.col() over direct column access
            if func_name == "select":
                for arg in node.args:
                    #Check if argument is a function call
                    #print('arg-> ',arg)
                    if isinstance(arg, astroid.Call) and isinstance(arg.func , astroid.Attribute):
                        if any(isinstance(sub_arg, astroid.Attribute) for sub_arg in arg.args):
                            self.add_message('C1001', node=node)
                    elif isinstance(arg, astroid.Attribute):
                        self.add_message('C1001', node=node)

            # Rule 2: Detect complex logical expressions
            if func_name in ["filter", "when"]:
                if len(node.args) > 0:
                    expr = node.args[0]
                    print('expr-> ',expr)
                    count = self.count_logical_operators(expr)
                    print('count-> ',count)
                    if count > 3:
                        self.add_message("C1002", node=node)

            # Rule 3: Check if a select() statement is used
            # if func_name == "select" and len(node.args) > 0:
            #     #print('Node: ',node)
            #     self.add_message("C1003", node=node)

            # Rule 4: Check for withColumnRenamed misuse
            if func_name == "withColumnRenamed":
                #print('Node: ',node)
                self.add_message("C1004", node=node)

            # Rule 5: Check for withColumn misuse
            if func_name == "withColumn":
                if len(node.args)>1:
                    col_expr = node.args[1]
                    if isinstance(col_expr, astroid.Call) and isinstance(col_expr.func, astroid.Attribute):
                        if col_expr.func.attrname == 'cast':
                            self.add_message("C1005", node=node)


            # Rule 6: Detect incorrect empty column values
            if func_name == "lit" and len(node.args) > 0:
                #, '\n','Node.args.values: ', node.args[0].values)
                if isinstance(node.args[0], astroid.Const) and node.args[0].value == "":
                    self.add_message("C1006", node=node)

            #Rule 8 & 9: JOin should have explicit how
            if func_name =='join':
                self.check_join_compliance(node)
            
            #Rule 10: Avoid using dropDuplicates
            if func_name == 'dropDuplicates':
                self.add_message('avoid-dropduplicates-distinct', node=node)
            
            #Rules 11: Prefer using ignoreNulls
            if func_name in ['first', 'last']:
                if not any(keyword.arg == 'ignorenulls' for keyword in node.keywords):
                    self.add_message('enable-ignorenulls', node=node)

            #Rule 12: Empty PartionBy()
            if func_name == 'partitionBy':
                if not node.args:
                    self.add_message('avoid-empty-partitionby', node=node)

            #Rule 13: Window Functions
            if func_name == 'over':
                if not any(keyword.arg in ['rowsBetween', 'rangeBetween'] for keyword in node.keywords):
                    self.add_message('specify-explicit-frame', node=node)


    def count_logical_operators(self, expr):
        """Count the number of operators for the expression."""
        #print('expr_rec:', expr)
        if isinstance(expr, astroid.BoolOp):
            return sum(self.count_logical_operators(v) for v in expr.values)
        if isinstance(expr, astroid.BinOp):
            return 1 + self.count_logical_operators(expr.left) + self.count_logical_operators(expr.right)
        if isinstance(expr, astroid.Compare):
            return 1
        return 0
    
    def _check_imported_module_names(self, node):
        """Checks the import and from x import y"""
        for name, _ in node.names:
            package_name = name.split('.')[0]
            if '_' in package_name or not package_name.islower():
                self.add_message('package-naming-convention', node=node)

    def check_join_compliance(self, node):
        """Checks join related rules

        Args:
            node (ast object): Current node object in AST
        """
        
        #Rule 8: Check for explicit "how" parameter
        if not any(keyword.arg == 'how' for keyword in node.keywords):
            self.add_message('join-explicit-how', node=node)

        #Rule 9: Check for right joins
        for keyword in node.keywords:
            #print('Keyword-> ',keyword, '\n', keyword.value.value, type(keyword.value.value))
            if keyword.arg == 'how' and keyword.value.value == 'right':
                self.add_message('avoid-right-joins', node=node)

class CustomLineChecker(BaseRawFileChecker):
    """Check the raw code lines.

    Args:
        BaseRawFileChecker (class): inherits the class
    """
    name = 'custom-line-checker'
    msgs={
        'C1101':(
            "Comment line exceeds 72 characters.",
            "comment-too-long",
            "Line comment length is fixed to 72 characters only."
        ),
    }
    def process_module(self, node):
        """Checks the length of comments.

        Args:
            node (ast object): Current node in Abstract Syntax Tree
        """
        with node.stream() as stream:
            for lineno , line  in enumerate(stream, start=1):
                line = line.decode('utf-8').strip('\n')
                if line.strip().startswith('#') and len(line) > 72:
                    self.add_message('comment-too-long', line=lineno)

def register(linter : PyLinter):
    """Registers the above custom check classes to Pylint configuration"""
    linter.register_checker(CustomNBCodingStandards(linter))
    linter.register_checker(CustomLineChecker(linter))
