"""
def visit_assign(self, node):
        # Rule 7: Check if the assigned value is a hardcoded string
        if isinstance(node.value, astroid.Const) and isinstance(node.value.value, str):
            self.add_message('C1007', node=node)
"""