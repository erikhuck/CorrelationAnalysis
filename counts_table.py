"""Combines all the intermediate counts tables into one"""

from os import listdir
from os.path import join
from pandas import read_csv, DataFrame
from sys import argv

from utils import INTER_COUNTS_TABLE_DIR, IDX_COL, COUNTS_TABLE_PATH


def main():
    """Main method"""

    table_type: str = argv[1]
    inter_counts_table_dir: str = INTER_COUNTS_TABLE_DIR.format(table_type)
    inter_counts_tables: list = listdir(inter_counts_table_dir)
    counts_table: DataFrame = None

    for inter_counts_table in inter_counts_tables:
        assert inter_counts_table.endswith('.csv')

        inter_counts_table: str = join(inter_counts_table_dir, inter_counts_table)
        inter_counts_table: DataFrame = read_csv(inter_counts_table, index_col=IDX_COL)

        if counts_table is None:
            counts_table: DataFrame = inter_counts_table
        else:
            counts_table: DataFrame = counts_table + inter_counts_table

    print(counts_table)
    counts_table.to_csv(COUNTS_TABLE_PATH.format(table_type))


if __name__ == '__main__':
    main()
