"""Connectors testing module."""
import pytest
from unittest.mock import patch
from src.connectors import MyConnectorAbstract, StdoutConnector


@patch.multiple(MyConnectorAbstract, __abstractmethods__=set())
def test_run_abstract_method():
    """Tests the connectors abstract class."""
    instance = MyConnectorAbstract()
    with pytest.raises(NotImplementedError):
        instance.run("Test")


def test_run_method_with_data_successfully(capsys):
    """Tests the Stdout connector run behavior to expected data."""
    data = {
        "counter": 14,
        "sentence": "Tempora quaerat porro sit numquam sit quaerat.",
        "some_value": 50,
    }

    connector = StdoutConnector()
    connector.run(data)

    captured = capsys.readouterr()
    assert captured.out == str(data) + "\n"


def test_run_method_without_data_successfully(capsys):
    """Tests the Stdout connector run behavior to None data."""
    connector = StdoutConnector()
    connector.run(None)

    captured = capsys.readouterr()
    assert captured.out == "None\n"
