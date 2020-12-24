"""Outputs a bonferroni corrected alpha"""

from sys import argv
from pandas import DataFrame, read_csv


def main():
    """Main method"""

    alpha: float = float(argv[1])
    print('Original Alpha:', alpha)
    col_types: DataFrame = read_csv('data/col-types.csv')
    n_features: int = col_types.shape[-1]
    print('Number Of Features:', n_features)
    n_tests: int = (n_features ** 2 - n_features) / 2
    print('Number Of Comparisons / Statistical Tests:', int(n_tests))
    corrected_alpha: float = alpha / n_tests
    print('Bonferroni Corrected Alpha:', corrected_alpha)


if __name__ == '__main__':
    main()
