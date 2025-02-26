from setuptools import setup, find_packages

setup(
    name='pylint_custom_rules',
    version='0.1',
    packages=find_packages(),
    entry_points={
        'pylint.checkers': [
            'notebook_coding_standards = parsers.custom_rules:CustomNBCodingStandards',
            'notebook_coding_standards = parsers.custom_rules:CustomLineChecker',
        ],
    }
)
