"""
Helper module for Ingest_racing.py that contains testable logic.
This module replicates the logic from the Databricks notebook.
"""

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import current_timestamp, col


def get_races_schema():
    """Return the schema that the notebook uses when reading the CSV file."""
    return StructType(
        fields=[
            StructField("raceId", IntegerType(), False),
            StructField("year", IntegerType(), True),
            StructField("round", IntegerType(), True),
            StructField("circuitId", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("date", DateType(), True),
            StructField("time", StringType(), True),
            StructField("url", StringType(), True),
        ]
    )


def add_ingestion_date(df):
    """Add ingestion_date column with current timestamp."""
    return df.withColumn("ingestion_date", current_timestamp())


def select_race_columns(df):
    """Select and rename specific columns from races dataframe."""
    return df.select(
        col("raceId").alias("race_id"),
        col("year").alias("race_year"),
        col("round"),
        col("circuitId").alias("circuit_id"),
        col("name"),
        col("ingestion_date"),
    )


def transform_races(df):
    """Apply the complete transformation pipeline from the notebook."""
    df = add_ingestion_date(df)
    df = select_race_columns(df)
    return df


def validate_csv_path(container, catalogo):
    """Validate and construct the CSV file path."""
    if not container or not catalogo:
        raise ValueError("container and catalogo parameters are required")
    
    return f"/Volumes/{catalogo}/{container}/datasets/races.csv"


def get_table_name(catalogo, esquema):
    """Construct the full table name from catalog and schema."""
    if not catalogo or not esquema:
        raise ValueError("catalogo and esquema parameters are required")
    
    return f"{catalogo}.{esquema}.races"
