

import click
import random
import time

from pyspark import SparkContext


def inside(p):
    """Randomly sample a point in the unit square, return true if the point is
    inside a circle of r=1 centered at the origin.
    """
    x, y = random.random(), random.random()
    return x*x + y*y < 1


@click.command()
@click.argument('n', type=int, default=1e9)
@click.argument('res_fh', type=click.File('w'), default='res.txt')
def main(n, res_fh):
    """Estimate pi by sampling a billion random points.
    """
    sc = SparkContext.getOrCreate()
    time.sleep(3)

    count = sc.parallelize(range(n)).filter(inside).count()
    pi = 4 * count / n

    print(pi)
    print(pi, file=res_fh)


if __name__ == '__main__':
    main()
