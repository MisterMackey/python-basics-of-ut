from app.fixtures import CleaningConfig, clean_data
from pytest import fixture

from pyspark.sql.types import StructType, StructField, StringType, IntegerType


@fixture
def schema():
    return StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("address", StringType(), True),
        ]
    )


@fixture
def input_df_with_errors(spark):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("age", StringType(), True),
            StructField("address", StringType(), True),
        ]
    )
    return spark.createDataFrame(
        [
            ("1", "Alice", "20", "123 Main St."),
            ("2", "Bob", "25", "456 Oak St."),
            ("2", "Bob", "25", "456 Oak St."),
            ("3", "Charlie", "Thirty Years old", "789 Elm St."),
            ("4", "David", "35", "012 Pine St."),
            ("5", "Eve", "40", "345 Cedar St."),
            ("5", "Eve", "40", "345 Cedar St."),
            (None, "Frank", "45", "678 Maple St."),
        ],
        schema,
    )


@fixture
def input_df_without_errors(spark, schema):
    return spark.createDataFrame(
        [
            (1, "Alice", 20, "123 Main St."),
            (2, "Bob", 25, "456 Oak St."),
            (3, "Charlie", None, "789 Elm St."),
            (4, "David", 35, "012 Pine St."),
            (5, "Eve", 40, "345 Cedar St."),
        ],
        schema,
    )


def test_clean_data_removes_duplicate_removes_null_pk_converts_schema(
    input_df_with_errors, input_df_without_errors, schema
):
    config = CleaningConfig("id", schema)
    cleaned_df = clean_data(input_df_with_errors, config)
    assert cleaned_df.collect() == input_df_without_errors.collect()


# def test_clean_data_removes_conversion_error_if_not_nullable(input_df_with_errors, input_df_without_errors, schema):
#    schema["age"].nullable = False
#    input_df_without_errors = input_df_without_errors.filter("id != 3")
#    config = CleaningConfig("id", schema)
#    cleaned_df = clean_data(input_df_with_errors, config)
#    assert cleaned_df.collect() == input_df_without_errors.collect()
