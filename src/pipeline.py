from typing import Any, Dict, List, Optional

from kafka import KafkaConsumer
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from src.connectors import MyConnectorAbstract
from src.transformations import Step


class StreamingPipeline:
    """Pipeline example definition"""

    def __init__(
        self,
        source: KafkaConsumer,
        sink: MyConnectorAbstract,
        spark_session: SparkSession,
        schema: StructType,
        steps: Optional[List[Step]] = None,
    ):
        self.source = source
        self.sink = sink
        self.steps = steps
        self.spark_session = spark_session
        self.schema = schema

    def _run_steps(self, dataframe: DataFrame):
        for step in self.steps:
            dataframe = step.run(dataframe)
            yield dataframe

    def _converts_to_dataframe(self, event_data: Dict[str, Any], schema: StructType):
        return self.spark_session.createDataFrame([event_data], schema)

    def _converts_dataframe_to_dict(self, dataframe: DataFrame):
        return dataframe.toJSON().first()

    def run(self):
        for event in self.source:
            event_data = event.value
            output_result = (
                self._converts_dataframe_to_dict(
                    list(
                        self._run_steps(
                            self._converts_to_dataframe(event_data, self.schema)
                        )
                    )[-1]
                )
                if self.steps
                else event_data
            )
            self.sink.run(output_result)
