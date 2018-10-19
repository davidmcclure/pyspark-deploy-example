

import time

from pyspark.sql import SparkSession


def get_spark(wait=0):
    """Get Spark connections.

    Returns: SparkContext, SparkSession
    """
    spark = SparkSession.builder.getOrCreate()

    # Wait for executors to register.
    time.sleep(wait)

    return spark.sparkContext, spark


def lpf(a):
    """Get the largest prime factor of an integer.
    https://www.slothparadise.com/largest-prime-factor/
    """
    b = 2
    while (a > b):
        if (a % b == 0):
            a /= b
            b = 2
        else:
            b += 1
    return b
