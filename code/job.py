

import click
import random

from utils import get_spark, lpf


def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1


@click.command()
@click.argument('n', type=int, default=1e9)
@click.argument('res_fh', type=click.File('w'), default='res.txt')
def main(n, res_fh):

    sc, _ = get_spark()

    count = sc.parallelize(range(n)).filter(inside).count()
    pi =  4 * count / n

    print(pi)
    print(pi, file=res_fh)


if __name__ == '__main__':
    main()
