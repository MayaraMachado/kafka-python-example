"""Transformation testing module."""
import pytest
from pandas.testing import assert_frame_equal

from unittest.mock import patch
from src.transformations import Step, NewDefaultColumnStep, WordCountStep
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


@patch.multiple(Step, __abstractmethods__=set())
def test_run_step_abstract_method(spark_session):
    """Tests the connectors abstract class."""
    df = spark_session.createDataFrame(
        [("Java",), ("Python", "100000"), ("Scala", "3000")]
    )
    instance = Step(id_="testing")
    with pytest.raises(NotImplementedError):
        instance.run(df)


def test_run_new_default_column_step_successfully(spark_session):
    schema = StructType(
        [
            StructField("counter", IntegerType(), True),
            StructField("sentence", StringType(), True),
            StructField("some_value", IntegerType(), True),
        ]
    )
    data = [
        {
            "counter": 14,
            "sentence": "Tempora quaerat porro sit numquam sit quaerat.",
            "some_value": 50,
        },
        {
            "counter": 15,
            "sentence": "Tempora sit numquam sit quaerat.",
            "some_value": None,
        },
        {
            "counter": 16,
            "sentence": "Lorem ipsum dolor set.",
            "some_value": None,
        },
    ]

    df = spark_session.createDataFrame(data, schema)
    expected_df = spark_session.createDataFrame(
        [
            {
                "counter": 14,
                "sentence": "Tempora quaerat porro sit numquam sit quaerat.",
                "some_value": 50,
            },
            {
                "counter": 15,
                "sentence": "Tempora sit numquam sit quaerat.",
                "some_value": 0,
            },
            {
                "counter": 16,
                "sentence": "Lorem ipsum dolor set.",
                "some_value": 0,
            },
        ],
        schema,
    )

    step = NewDefaultColumnStep(id_="some_value")

    result = step.run(df, 0)

    assert_frame_equal(result.toPandas(), expected_df.toPandas(), check_like=True)


def test_run_new_default_column_step_with_invalid_value_throws_value_error(
    spark_session, capsys
):
    schema = StructType(
        [
            StructField("counter", IntegerType(), True),
            StructField("sentence", StringType(), True),
            StructField("some_value", IntegerType(), True),
        ]
    )
    data = [
        {
            "counter": 14,
            "sentence": "Tempora quaerat porro sit numquam sit quaerat.",
            "some_value": 50,
        },
        {
            "counter": 15,
            "sentence": "Tempora sit numquam sit quaerat.",
            "some_value": None,
        },
        {
            "counter": 16,
            "sentence": "Lorem ipsum dolor set.",
            "some_value": None,
        },
    ]

    df = spark_session.createDataFrame(data, schema)

    step = NewDefaultColumnStep(id_="some_value")

    with pytest.raises(ValueError):
        result = step.run(df, {"invalid_type": "oops"})
        captured = capsys.readouterr()
        assert "Not a valid type to substitute." in captured.err


def test_run_word_count_step_successfully(spark_session):
    schema = StructType(
        [
            StructField("counter", IntegerType(), True),
            StructField("sentence", StringType(), True),
            StructField("some_value", IntegerType(), True),
        ]
    )
    data = [
        {
            "counter": 14,
            "sentence": "Tempora quaerat porro sit numquam sit quaerat.",
            "some_value": 50,
        },
        {
            "counter": 15,
            "sentence": "Tempora sit numquam sit quaerat.",
            "some_value": None,
        },
        {
            "counter": 16,
            "sentence": "Lorem ipsum dolor set.",
            "some_value": None,
        },
    ]

    df = spark_session.createDataFrame(data, schema)
    expected_df = spark_session.createDataFrame(
        [
            {
                "counter": 14,
                "sentence": "Tempora quaerat porro sit numquam sit quaerat.",
                "some_value": 50,
                "total_word_count": 7,
            },
            {
                "counter": 15,
                "sentence": "Tempora sit numquam sit quaerat.",
                "some_value": None,
                "total_word_count": 5,
            },
            {
                "counter": 16,
                "sentence": "Lorem ipsum dolor set.",
                "total_word_count": 4,
            },
        ],
        StructType(
            [
                StructField("counter", IntegerType(), True),
                StructField("sentence", StringType(), True),
                StructField("some_value", IntegerType(), True),
                StructField("total_word_count", IntegerType(), True),
            ]
        ),
    )

    step = WordCountStep(id_="total_word_count", column="sentence")
    result = step.run(df)

    assert_frame_equal(result.toPandas(), expected_df.toPandas(), check_like=True)
