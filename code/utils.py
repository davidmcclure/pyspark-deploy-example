

from pyspark import SparkContext
from pyspark.sql import SparkSession


def get_spark():
    """Get spark connections.
    """
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc).builder.getOrCreate()

    return sc, spark


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
