"""A module with a couple of methods that do simple data cleaning of dataframes."""

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


class CleaningConfig:
    """A configuration object for cleaning data."""

    def __init__(self, pk_col_name: str, schema: StructType):
        self.pk_col_name = pk_col_name
        self.schema = schema


def drop_null_pk(df: DataFrame, pk_col_name: str) -> DataFrame:
    """Drop rows where the primary key is null."""
    return df.filter(df[pk_col_name].isNotNull())


def drop_duplicate_pk(df: DataFrame, pk_col_name: str) -> DataFrame:
    """Drop rows where the primary key is duplicated."""
    return df.dropDuplicates([pk_col_name])


def cast_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """Coerce the schema of a dataframe."""
    for target_column in schema.fieldNames():
        df = df.withColumn(
            target_column, df[target_column].cast(schema[target_column].dataType)
        )
        if not schema[target_column].nullable:
            df = df.filter(df[target_column].isNotNull())
    return df


def clean_data(df: DataFrame, config: CleaningConfig) -> DataFrame:
    """Clean the data in a dataframe."""
    df = drop_null_pk(df, config.pk_col_name)
    df = drop_duplicate_pk(df, config.pk_col_name)
    df = cast_to_schema(df, config.schema)
    return df
