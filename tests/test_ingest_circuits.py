import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for use in unit tests."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("pytest-pyspark-local")
        .getOrCreate()
    )


# helper functions that replicate the transformations defined in Ingest_circuits.py

def get_circuits_schema():
    """Return the schema that the notebook uses when reading the CSV file."""
    return StructType(
        [
            StructField("circuitId", IntegerType(), False),
            StructField("circuitRef", StringType(), True),
            StructField("name", StringType(), True),
            StructField("location", StringType(), True),
            StructField("country", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lng", DoubleType(), True),
            StructField("alt", IntegerType(), True),
            StructField("url", StringType(), True),
        ]
    )


def transform_circuits(df):
    """Apply the select/rename/add-ingestion-date pipeline from the notebook.

    The notebook does not expose a function, so the tests replicate the logic
    verbatim.  This ensures that we are exercising the same column
    manipulations that would be executed in Databricks.
    """

    selected = df.select(
        "circuitId",
        "circuitRef",
        "name",
        "location",
        "country",
        "lat",
        "lng",
        "alt",
    )

    renamed = (
        selected.withColumnRenamed("circuitId", "circuit_id")
        .withColumnRenamed("circuitRef", "circuit_ref")
        .withColumnRenamed("lat", "latitude")
        .withColumnRenamed("lng", "longitude")
        .withColumnRenamed("alt", "altitude")
    )

    final_df = renamed.withColumn("ingestion_date", current_timestamp())
    return final_df


def test_schema_definition():
    """The schema used by the notebook should contain exactly the expected fields."""
    schema = get_circuits_schema()
    names = [f.name for f in schema.fields]
    types = [type(f.dataType) for f in schema.fields]

    assert names == [
        "circuitId",
        "circuitRef",
        "name",
        "location",
        "country",
        "lat",
        "lng",
        "alt",
        "url",
    ]

    assert types == [
        IntegerType,
        StringType,
        StringType,
        StringType,
        StringType,
        DoubleType,
        DoubleType,
        IntegerType,
        StringType,
    ]


def test_transformation_pipeline(spark):
    """Verify that selecting, renaming, and adding the ingestion date work as expected."""

    data = [
        (1, "ref1", "n1", "loc1", "c1", 12.3, 45.6, 100, "ignore"),
        (2, "ref2", "n2", "loc2", "c2", 78.9, 10.1, 200, "ignore"),
    ]
    schema = get_circuits_schema()
    input_df = spark.createDataFrame(data, schema=schema)

    out = transform_circuits(input_df)

    expected_cols = [
        "circuit_id",
        "circuit_ref",
        "name",
        "location",
        "country",
        "latitude",
        "longitude",
        "altitude",
        "ingestion_date",
    ]

    assert out.columns == expected_cols
    assert out.count() == 2

    # ingestion_date should exist and be a timestamp type
    dtype_map = dict(out.dtypes)
    assert dtype_map["ingestion_date"] == "timestamp"


def test_empty_input(spark):
    """An empty dataframe should still produce the correct schema after transformation."""
    schema = get_circuits_schema()
    empty = spark.createDataFrame([], schema=schema)
    transformed = transform_circuits(empty)
    assert transformed.count() == 0
    assert transformed.columns[-1] == "ingestion_date"
    assert dict(transformed.dtypes)["ingestion_date"] == "timestamp"
