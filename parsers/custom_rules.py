import os
import json
import astroid
import pandas as pd
from pylint.checkers import BaseChecker, BaseRawFileChecker
from pylint.lint import PyLinter
from dotenv import load_dotenv
from src.dbr_client import fetch_hq_guide_rules

load_dotenv('.env')

hq_dbr_host = os.getenv('HQ_DBR_DEV_HOST_URL')
hq_dbr_token = os.getenv('HQ_DBR_DEV_TOKEN')

# Rules with detailed message
rules = fetch_hq_guide_rules(hq_dbr_host, hq_dbr_token)

result = rules.get('result', {})
print("Results: ", result, '\n')
cols = rules.get('manifest', {})
print('Cols: ',cols, '\n')
col_list = []
for col in cols['schema']['columns']:
    col_list.append(col['name'])
print('col_list ', col_list, '\n')

data = result.get('data_array', [])
rules_df = pd.DataFrame(data, columns=col_list)
rules_df = rules_df.tail(2)
print("[INFO] Loaded Rules:\n",rules_df)

class DynamicChecker(BaseChecker):
    """Custom Pylint checker that dynamically implements functions from DataFrame."""

    name = 'dynamic-checker'
    priority = -1

    def __init__(self, linter=None):
        super().__init__(linter)
        self.dynamic_functions = {}
        self.msgs = {}
        self._load_dynamic_functions_and_msgs()

    def _load_dynamic_functions_and_msgs(self):
        """Load dynamic functions and messages from DataFrame."""
        for _, row in rules_df.iloc[::-1].iterrows():
            func_code = row['query']
            exec(func_code, globals())
            func_name = self._extract_function_name(func_code)
            print(func_name)
            self.dynamic_functions[func_name] = globals()[func_name]
            print(self.dynamic_functions)
            for parts in row['description'].split(','):
                self.msgs[f"C10{str(row['rule_id'])}"] = (
                    parts[0].strip(),
                    parts[1].strip(),
                    parts[2].strip()
                )

    def _extract_function_name(self, func_code):
        """Extract the function name from the function code."""
        lines = func_code.strip().split('\n')
        print('Lines: ', lines)
        first_line = lines[1].strip()
        print('First Line:\n',first_line,'\n')
        if first_line.startswith('def '):
            func_name = first_line.split('(')[0].split()[-1]
            print(func_name)
            return func_name
        raise ValueError("Invalid function definition")

    def visit_call(self, node):
        """Visit a function call node."""
        for func in self.dynamic_functions.values():
            func(self, node)

    def visit_assign(self, node):
        """Visit an assignment node."""
        for func in self.dynamic_functions.values():
            func(self, node)

def register(linter: PyLinter):
    """Register the custom checker."""
    linter.register_checker(DynamicChecker(linter))

# class CustomNBCodingStandards(BaseChecker):
#     """Contains custom Pyspark checks to apply on Notebooks.

#     Args:
#         BaseChecker (Class): Pylint Class for applying checks.
#     """

#     name = 'notebook-coding-standards'
#     priority = -1
    
#     # Create message format by splitting Description
#     msgs = {
#         row['rule_id']: (
#             parts[0].strip().strip('"'),
#             parts[1].strip(),
#             parts[2].strip()
#         ) 
#         for _, row in rules_df.iterrows() 
#         for parts in [row['Description'].split(',')]
#     }
#     print('Msgs-> \n', msgs)

#     options = {}

#     def __init__(self, linter):
#         super().__init__(linter)

#         self.dynamic_methods={}

#         for _, rule in rules_df.iterrows():
#             query = rule['Query'].strip()

#             if query.startswith('def visit_'):
                
#                 temp_namespace = {}
#                 print(f"Implementing rule: {rule['RuleID']} ...")

#                 exec(query, globals(), temp_namespace)

#                 func_name = next(iter(temp_namespace.keys()))
#                 func = temp_namespace[func_name]
#                 print('func_name: ',func_name, '\nfunc: ', func, '\n temp_namespace: ',temp_namespace)

#                 #Store multiple functions under same name
#                 if func_name not in self.dynamic_methods:
#                     self.dynamic_methods[func_name]=[]

#                 self.dynamic_methods[func_name].append(func)


#         print("Dynamic Methos:\n", getattr(self, "dynamic_methods", "Not Found"))

#         for name, func_list in self.dynamic_methods.items():
#             print('name: ',name, 'func_list: ', func_list)
#             def combined_func(self, node):
#                 for func in func_list: #Execute all functions stored under func name
#                     print('func: ' ,func)
#                     func(self, node)

#             setattr(self, name, combined_func.__get__(self))


# Registration function
# def register(linter: PyLinter):
#     """Register the checker with Pylint."""
#     linter.register_checker(CustomNBCodingStandards(linter))
