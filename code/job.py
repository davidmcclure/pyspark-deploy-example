

import click
import random

from utils import get_spark, lpf


@click.command()
@click.argument('n', default=100000)
@click.argument('res_fh', type=click.File('w'), default='res.txt')
def main(n, res_fh):

    sc, _ = get_spark()

    data = random.sample(range(n), n)
    data = sc.parallelize(data)

    result = data.map(lpf).max()

    print(result, file=res_fh)
    print(result)


if __name__ == '__main__':
    main()
