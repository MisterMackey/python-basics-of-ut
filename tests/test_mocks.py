from pytest import fixture, raises

from unittest.mock import MagicMock, patch

from requests import RequestException

from app.mocks import get_config, _get_config_from_api


@fixture
def example_api_response():
    return {
        "pk_col_name": "id",
        "schema": [
            {"name": "id", "type": "IntegerType", "nullable": True},
            {"name": "name", "type": "StringType", "nullable": True},
            {"name": "age", "type": "IntegerType", "nullable": True},
            {"name": "address", "type": "StringType", "nullable": True},
        ],
    }


@patch("app.mocks._get_config_from_api", return_value=None)
def test_get_config_returns_none(mock_function):
    result = get_config("example")
    assert result is None
    mock_function.assert_called_once_with("example")


@patch("app.mocks._get_config_from_api")
def test_get_config_returns_config(mock_function, example_api_response):
    mock_function.return_value = example_api_response
    result = get_config("example")
    assert result.pk_col_name == example_api_response["pk_col_name"]
    for field in result.schema:
        corresponding_field = next(
            col for col in example_api_response["schema"] if col["name"] == field.name
        )
        assert field.dataType.__class__.__name__ == corresponding_field["type"]
        assert field.nullable == corresponding_field["nullable"]
    mock_function.assert_called_once_with("example")


@patch("app.mocks.get")
def test_get_config_from_api_returns_none(mock_get):
    mock_get.side_effect = RequestException("Error")
    result = _get_config_from_api("example")
    assert result == None


@patch("app.mocks.get")
def test_get_config_from_api_returns_response(mock_get, example_api_response):
    mock_response = MagicMock()
    mock_response.json.return_value = example_api_response
    mock_get.return_value = mock_response
    result = _get_config_from_api("example")
    assert result == example_api_response
    mock_get.assert_called_once_with("http://api.com/config?file=example")
