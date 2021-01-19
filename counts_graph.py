"""Creates a corresponding stacked bar chart for the counts table"""

from pandas import read_csv, DataFrame
from matplotlib import pyplot as plt
from sys import argv

from utils import (
    COUNTS_TABLE_PATH, INSIGNIFICANT_KEY, UNCORRECTED_ALPHA_KEY, CORRECTED_ALPHA_KEY, SUPER_ALPHA_KEY, IDX_COL,
    MAX_SIGNIFICANCE_KEY
)


def main():
    """Main method"""

    table_type: str = argv[1]
    table: DataFrame = read_csv(COUNTS_TABLE_PATH.format(table_type), index_col=IDX_COL)
    insignificant_counts: list = get_counts(table=table, key=INSIGNIFICANT_KEY)
    uncorrected_counts: list = get_counts(table=table, key=UNCORRECTED_ALPHA_KEY)
    corrected_counts: list = get_counts(table=table, key=CORRECTED_ALPHA_KEY)
    super_alpha_counts: list = get_counts(table=table, key=SUPER_ALPHA_KEY)
    max_significance_counts: list = get_counts(table=table, key=MAX_SIGNIFICANCE_KEY)
    labels: list = list(table.index)[:-1]
    width = 0.35

    _, ax = plt.subplots()
    ax.bar(labels, insignificant_counts, width, label=INSIGNIFICANT_KEY)
    ax.bar(labels, uncorrected_counts, width, label=UNCORRECTED_ALPHA_KEY)
    ax.bar(labels, corrected_counts, width, label=CORRECTED_ALPHA_KEY)
    ax.bar(labels, super_alpha_counts, width, label=SUPER_ALPHA_KEY)
    ax.bar(labels, max_significance_counts, width, label=MAX_SIGNIFICANCE_KEY)
    ax.set_xlabel('Comparison Type')
    ax.set_ylabel('Counts')
    ax.set_title('Significance Level Counts')
    ax.legend()

    save_path: str = 'data/{}-stacked-bar-chart.png'.format(table_type)
    plt.savefig(save_path)


def get_counts(table: DataFrame, key: str) -> list:
    """Gets the list of counts for a given significance level"""

    return list(table[key])[:-1]


if __name__ == '__main__':
    main()
