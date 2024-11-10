from pytest import fixture
from pyspark.sql import SparkSession


@fixture(scope="session")
def spark():
    return SparkSession.builder.appName("pytest").getOrCreate()
