import pytest
from parsers.notebook_parser import lint_python_code

def test_sample_lint():
    test_code ='''
"""Pyspark Code"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W

spark = SparkSession.builder.appName("JoinCompliance").getOrCreate()

flights = spark.read.csv("flights.csv")
aircraft = spark.read.csv("aircraft.csv")

# Bad join
flights = flights.join(aircraft, 'aircraft_id')
    '''
    path = "/Workspace/Shared/GasPower/Abhishek Pal/BTP/Data Validation"
    results = lint_python_code(path, test_code)
    print('results-> ',results)
    assert isinstance(results, list)
    assert len(results)>0
    assert results[0]['Status']== 'Compliant'