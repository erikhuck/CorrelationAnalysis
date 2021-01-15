"""Creates the input for all the column comparisons jobs"""

from sys import argv
from math import sqrt
from pandas import DataFrame

from utils import START_IDX_KEY, STOP_IDX_KEY, N_ROWS_KEY


def main():
    """Main method"""

    col_comp_inputs_path: str = argv[1]
    stop_idx: int = int(argv[2])
    n_rows: int = int(argv[3])

    # We begin at start index 2 to skip over the patient ID column
    start_idx: int = 2

    n_cols: int = stop_idx - start_idx + 1
    n_total_cells: int = get_n_total_cells(r=n_rows, c=n_cols)
    print('Number Of Total Cells:', n_total_cells)
    job_n: int = 0
    inputs: dict = {
        START_IDX_KEY: [],
        STOP_IDX_KEY: [],
        N_ROWS_KEY: []
    }

    while start_idx < stop_idx:
        print('Job Number:', job_n)
        print('Start: {} Stop: {}'.format(start_idx, stop_idx))

        n_cols: int = stop_idx - start_idx
        print('Number Of Columns:', n_cols)

        # If the content of the square root in the get_n_rows equation equates to a negative
        if sqrt_content(t=n_total_cells, c=n_cols) < 0.0:
            n_rows: int = n_cols
        else:
            n_rows: int = get_n_rows(t=n_total_cells, c=n_cols)

        print('Number Of Rows:', n_rows)
        print('Number Of Total Cells:', get_n_total_cells(r=n_rows, c=n_cols))
        print()

        inputs[START_IDX_KEY].append(start_idx)
        inputs[STOP_IDX_KEY].append(stop_idx)
        inputs[N_ROWS_KEY].append(n_rows)

        start_idx += n_rows
        job_n += 1

    assert start_idx == stop_idx

    inputs: DataFrame = DataFrame(inputs)
    inputs.to_csv(col_comp_inputs_path, index=False)


def get_n_total_cells(r: int, c: int) -> int:
    """Gets the total number of cells in the conceptual matrix given a number of rows and columns"""

    t: int = r * c - (r ** 2 - r) // 2 - r
    return t


def sqrt_content(t: int, c: int) -> float:
    """The calculation within the square root component of the get_n_rows equation"""

    return (2 * c - 1) ** 2 - 8 * t


def get_n_rows(t: int, c: int) -> int:
    """Gets the number of rows for a section of the conceptual matrix given a columns number and total cells number"""

    r: float = -(-2 * c + 1 + sqrt(sqrt_content(t=t, c=c))) / 2
    return int(r)


if __name__ == '__main__':
    main()
