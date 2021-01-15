"""Outputs a bonferroni corrected alpha"""

from sys import argv
from pandas import DataFrame, read_csv
from pickle import dump

from utils import ALPHAS_PATH, COL_TYPES_PATH


def main():
    """Main method"""

    alpha: float = float(argv[1])
    print('Original Alpha:', alpha)
    col_types: DataFrame = read_csv(COL_TYPES_PATH)
    n_features: int = col_types.shape[-1]
    print('Number Of Features:', n_features)
    n_tests: int = (n_features ** 2 - n_features) / 2
    print('Number Of Comparisons / Statistical Tests:', int(n_tests))
    corrected_alpha: float = alpha / n_tests
    print('Bonferroni Corrected Alpha:', corrected_alpha)
    alphas = alpha, corrected_alpha
    dump(alphas, open(ALPHAS_PATH, 'wb'))


if __name__ == '__main__':
    main()
