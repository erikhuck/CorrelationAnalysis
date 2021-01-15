"""Creates a smaller data set from the real data set for testing and debugging"""

from sys import argv
from pandas import DataFrame, read_csv
from pickle import load
from random import seed, shuffle

from utils import COL_TYPES_PICKLE_PATH


def debug_data():
    """See module description"""

    data_path: str = argv[1]
    n_cols: int = int(argv[2])

    # Ensure the same columns are selected each time this script is run
    seed(0)

    with open(COL_TYPES_PICKLE_PATH, 'rb') as f:
        col_types: dict = load(f)

    # Sort the headers for consistency
    headers: list = sorted(col_types.keys())
    shuffle(headers)
    headers: list = headers[0:n_cols]

    # Make a subset of the data and column types
    data: DataFrame = read_csv(data_path, usecols=['PTID'] + headers)
    data.to_csv('data/debug-data.csv', index=False)


if __name__ == '__main__':
    debug_data()
