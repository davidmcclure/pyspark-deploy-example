

import time

from pyspark.sql import SparkSession


def get_spark():
    """Get Spark connections.

    Returns: SparkContext, SparkSession
    """
    spark = SparkSession.builder.getOrCreate()
    return spark.sparkContext, spark
