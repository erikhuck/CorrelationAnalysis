"""Creates a minimalist column type mapping, minimalist because all expression and MRI data is known to be numeric"""

from sys import argv
from pandas import DataFrame, read_csv
from pickle import dump

from utils import COL_TYPES_PICKLE_PATH, COL_TYPES_PATH


def main():
    """See module doc"""

    adnimerge_col_types_path: str = argv[1]
    col_types: DataFrame = read_csv(COL_TYPES_PATH)
    adnimerge_col_types: DataFrame = read_csv(adnimerge_col_types_path)

    # Only use the ADNIMERGE columns types from the combined data since the types for expression and MRI are all a given
    cols_to_use: list = list(set(adnimerge_col_types).intersection(set(col_types)))
    col_types: DataFrame = col_types[cols_to_use]

    del adnimerge_col_types

    # Convert the column types into a dictionary
    data_types: list = list(col_types.loc[0])
    col_names: list = list(col_types)
    col_types: dict = {}

    for col_name, data_type in zip(col_names, data_types):
        col_types[col_name] = data_type

    with open(COL_TYPES_PICKLE_PATH, 'wb') as f:
        dump(col_types, f)


if __name__ == '__main__':
    main()
