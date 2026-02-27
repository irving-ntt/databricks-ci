import pytest
from pyspark.sql.types import *
import sys
from pathlib import Path

# Add the ETL directory to sys.path to import the helper module
sys.path.insert(0, str(Path(__file__).parent.parent / "ETL"))

from ingest_racing_logic import (
    get_races_schema,
    validate_csv_path,
)


def test_races_schema_definition():
    """The schema used by the notebook should contain exactly the expected fields."""
    schema = get_races_schema()
    names = [f.name for f in schema.fields]
    types = [type(f.dataType) for f in schema.fields]

    assert names == [
        "raceId",
        "year",
        "round",
        "circuitId",
        "name",
        "date",
        "time",
        "url",
    ]

    assert types == [
        IntegerType,
        IntegerType,
        IntegerType,
        IntegerType,
        StringType,
        DateType,
        StringType,
        StringType,
    ]


def test_races_schema_field_count():
    """Verify schema has correct number of fields."""
    schema = get_races_schema()
    assert len(schema.fields) == 8


def test_validate_csv_path_construction():
    """Verify that CSV path is constructed correctly."""
    container = "raw"
    catalogo = "workspace"
    
    path = validate_csv_path(container, catalogo)
    
    assert path == "/Volumes/workspace/raw/datasets/races.csv"
    assert "races.csv" in path
    assert "raw" in path
    assert "workspace" in path


def test_validate_csv_path_missing_container():
    """Verify that missing container parameter raises error."""
    with pytest.raises(ValueError, match="container and catalogo parameters are required"):
        validate_csv_path(None, "workspace")



