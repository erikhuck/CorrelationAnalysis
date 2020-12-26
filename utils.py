"""Contains shared data and functions"""

from pickle import load

NUMERIC_TYPE: str = 'numeric'
NOMINAL_TYPE: str = 'nominal'
ALPHAS_PATH: str = 'data/alphas.p'
INTER_COUNTS_TABLE_DIR: str = 'data/inter-counts-tables'
COUNTS_TABLE_PATH: str = 'data/counts-table.csv'
INSIGNIFICANT_KEY: str = 'No Significance'
UNCORRECTED_ALPHA_KEY: str = 'Above Uncorrected Alpha'
CORRECTED_ALPHA_KEY: str = 'Above Bonferroni Corrected Alpha'
SUPER_ALPHA_KEY: str = 'Above Super Alpha'
IDX_COL: str = 'idx'


def get_type(header: str, col_types: dict) -> str:
    """Gets the data type of a column given its header"""

    # All the MRI and expression data is numeric and thus does not need to be included in the column types
    if header not in col_types:
        return NUMERIC_TYPE

    return col_types[header]


def get_col_types() -> dict:
    """Gets the dictionary mapping a column header name to its corresponding data type"""

    return load(open('data/col-types.p', 'rb'))
