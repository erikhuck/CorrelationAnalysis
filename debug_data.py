"""Creates a smaller data set from the real data set for testing and debugging"""

from sys import argv
from pandas import DataFrame, read_csv
from pickle import load, dump
from random import seed, shuffle


def debug_data():
    """See module description"""

    data_path: str = argv[1]
    col_types_path: str = argv[2]
    n_cols: int = int(argv[3])

    # Ensure the same columns are selected each time this script is run
    seed(0)

    with open(col_types_path, 'rb') as f:
        col_types: dict = load(f)

    # Sort the headers for consistency
    headers: list = sorted(col_types.keys())
    shuffle(headers)
    headers: list = headers[0:n_cols]

    # Make a subset of the data and column types
    col_types: dict = {header: col_types[header] for header in headers}
    assert set(headers) == set(col_types.keys())
    assert len(headers) == len(col_types)

    with open('data/debug-col-types.p', 'wb') as f:
        dump(col_types, f)

    data: DataFrame = read_csv(data_path, usecols=['PTID'] + headers)
    data.to_csv('data/debug-data.csv', index=False)


if __name__ == '__main__':
    debug_data()
