"""Combines all the intermediate counts tables into one"""

from os import listdir
from os.path import join
from pandas import read_csv, DataFrame

from utils import INTER_COUNTS_TABLE_DIR, IDX_COL, COUNTS_TABLE_PATH


def main():
    """Main method"""

    inter_counts_tables: list = listdir(INTER_COUNTS_TABLE_DIR)
    counts_table: DataFrame = None

    for inter_counts_table in inter_counts_tables:
        if not inter_counts_table.endswith('.csv'):
            continue

        inter_counts_table: str = join(INTER_COUNTS_TABLE_DIR, inter_counts_table)
        inter_counts_table: DataFrame = read_csv(inter_counts_table, index_col=IDX_COL)

        if counts_table is None:
            counts_table: DataFrame = inter_counts_table
        else:
            counts_table: DataFrame = counts_table + inter_counts_table

    print(counts_table)
    counts_table.to_csv(COUNTS_TABLE_PATH)


if __name__ == '__main__':
    main()
