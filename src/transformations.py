from abc import ABC, abstractmethod
from functools import reduce
from typing import Any, Union

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, when, udf


class Step(ABC):
    """Base step definition"""

    def __init__(self, id_: str):
        self.id = id_

    @abstractmethod
    def run(self, df: DataFrame, *arg: Any):
        raise NotImplementedError


class NewDefaultColumnStep(Step):
    def run(self, df: DataFrame, default_value: Union[str, int]):
        try:
            return df.withColumn(
                self.id,
                when(col(self.id).isNull(), lit(default_value)).otherwise(col(self.id)),
            )
        except Exception as ex:
            raise ValueError("Not a valid type to substitute.") from ex


class WordCountStep(Step):
    def __init__(self, id_: str, column: str):
        self.column = column
        super().__init__(id_=id_)

    def run(self, df: DataFrame):
        def word_count(sentence):
            return len(sentence.split(" "))
        countWords = udf(word_count, 'int')
        return df.withColumn(self.id, countWords(df[self.column]))
