"""Creates a corresponding stacked bar chart for the counts table"""

from pandas import read_csv, DataFrame
from matplotlib import pyplot as plt
from sys import argv

from utils import (
    COUNTS_TABLE_PATH, INSIGNIFICANT_KEY, UNCORRECTED_ALPHA_KEY, CORRECTED_ALPHA_KEY, SUPER_ALPHA_KEY, IDX_COL
)


def main():
    """Main method"""

    table_type: str = argv[1]
    table: DataFrame = read_csv(COUNTS_TABLE_PATH.format(table_type), index_col=IDX_COL)
    # TODO: Fix counts graph script beginning with including the maximum signigicance counts
    insignificant_counts: list = list(table[INSIGNIFICANT_KEY])
    uncorrected_counts: list = list(table[UNCORRECTED_ALPHA_KEY])
    corrected_counts: list = list(table[CORRECTED_ALPHA_KEY])
    super_alpha_counts: list = list(table[SUPER_ALPHA_KEY])
    labels: list = list(table.index)
    width = 0.35

    _, ax = plt.subplots()
    ax.bar(labels, insignificant_counts, width, label=INSIGNIFICANT_KEY)
    ax.bar(labels, uncorrected_counts, width, label=UNCORRECTED_ALPHA_KEY)
    ax.bar(labels, corrected_counts, width, label=CORRECTED_ALPHA_KEY)
    ax.bar(labels, super_alpha_counts, width, label=SUPER_ALPHA_KEY)
    ax.set_xlabel('Comparison Type')
    ax.set_ylabel('Counts')
    ax.set_title('Significance Level Counts')
    ax.legend()

    plt.savefig('data/stacked-bar-chart.png')


if __name__ == '__main__':
    main()
