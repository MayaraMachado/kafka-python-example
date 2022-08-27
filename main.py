import json
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


from src.pipeline import StreamingPipeline
from src.transformations import NewDefaultColumnStep, WordCountStep
from src.connectors import StdoutConnector

spark_session = SparkSession.builder.appName("PythonNordeste2022").getOrCreate()

pipeline = StreamingPipeline(
    source=KafkaConsumer(
        "topic_test",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="my-group-id",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    ),
    steps=[
        NewDefaultColumnStep("some_value"),
        WordCountStep("total_sentence_words", "sentence"),
    ],
    sink=StdoutConnector(),
    schema=StructType(
        [
            StructField("counter", IntegerType(), True),
            StructField("sentence", StringType(), True),
            StructField("some_value", IntegerType(), True),
        ]
    ),
    spark_session=spark_session,
)

pipeline.run()
