"""Module that pulls configuration info from a web API."""

from typing import Any
from requests import get, RequestException
import logging

from app.fixtures import CleaningConfig
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


logger = logging.getLogger(__name__)


def _get_config_from_api(file: str) -> Any:
    """Get configuration info from a web API."""
    try:
        response = get("http://api.com/config?file={}".format(file))
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        logger.error("Error getting configuration from API: %s", e)
        return None


def _parse_config(config: dict) -> CleaningConfig:
    """Parse configuration info."""
    schema = StructType()
    map = {
        "StringType": StringType(),
        "IntegerType": IntegerType(),
    }
    for field in config["schema"]:
        schema.add(StructField(field["name"], map[field["type"]], field["nullable"]))
    return CleaningConfig(config["pk_col_name"], schema)


def get_config(file: str) -> CleaningConfig | None:
    """Get configuration info."""
    config = _get_config_from_api(file)
    if config:
        return _parse_config(config)
    return None
