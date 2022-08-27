import json
from unittest.mock import MagicMock, Mock, patch
from src.pipeline import StreamingPipeline
from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit

from src.transformations import Step


class MockStep(Step):
    def run(self, df: DataFrame):
        return df.withColumn("test_column", lit(True))


@patch("src.pipeline.KafkaConsumer", autospec=True, return_value="consumer")
def tests_streaming_pipeline_run_successfully(
    mock_kafka_consumer: Mock, spark_session: SparkSession, capsys
):
    schema = StructType(
        [
            StructField("counter", IntegerType(), True),
            StructField("sentence", StringType(), True),
            StructField("some_value", IntegerType(), True),
        ]
    )
    event_data = {
        "counter": 14,
        "sentence": "Tempora quaerat porro sit numquam sit quaerat.",
        "some_value": 50,
    }
    mock_event = MagicMock()
    mock_event.value = event_data
    mock_kafka_consumer.__iter__.return_value = [mock_event]
    
    mock_sink = Mock()
    # mock_sink.run.return_value = "Done!"

    expected_data = '{"counter":14,"sentence":"Tempora quaerat porro sit numquam sit quaerat.","some_value":50,"test_column":true}'

    pipeline = StreamingPipeline(
        source=mock_kafka_consumer,
        sink=mock_sink,
        spark_session=spark_session,
        schema=schema,
        steps=[
            MockStep("mock step"),
        ],
    )

    pipeline.run()

    mock_sink.run.assert_called_once_with(expected_data)
