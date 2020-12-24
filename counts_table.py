"""Creates a table with counts of the comparisons with p-values that reach various significance levels"""

from sys import argv
from os import listdir
from os.path import join
from pickle import load
from pandas import DataFrame
from tqdm import tqdm

from utils import (
    get_type, NUMERIC_TYPE, NOMINAL_TYPE, ALPHAS_PATH, COUNTS_TABLE_PATH, INSIGNIFICANT_KEY, UNCORRECTED_ALPHA_KEY,
    CORRECTED_ALPHA_KEY, SUPER_ALPHA_KEY, get_col_types, IDX_COL
)

NUM_NUM_KEY: str = 'Numerical Numerical'
NOM_NOM_KEY: str = 'Categorical Categorical'
NUM_NOM_KEY: str = 'Numerical Categorical'
TOTAL_KEY: str = 'Total'


def main():
    """Main method"""

    comp_dict_dir: str = argv[1]
    super_alpha: float = float(argv[2])

    table: DataFrame = DataFrame(
        {
            IDX_COL: [NUM_NUM_KEY, NOM_NOM_KEY, NUM_NOM_KEY],
            INSIGNIFICANT_KEY: [0] * 3,
            UNCORRECTED_ALPHA_KEY: [0] * 3,
            CORRECTED_ALPHA_KEY: [0] * 3,
            SUPER_ALPHA_KEY: [0] * 3,
            TOTAL_KEY: [0] * 3
        }
    )
    table: DataFrame = table.set_index(IDX_COL)

    uncorrected_alpha, corrected_alpha = load(open(ALPHAS_PATH, 'rb'))
    col_types: dict = get_col_types()
    comp_dict_dir: str = join('data', comp_dict_dir)
    comp_dicts: list = listdir(comp_dict_dir)

    for comp_dict in tqdm(comp_dicts):
        if not comp_dict.endswith('.p'):
            continue

        comp_dict: str = join(comp_dict_dir, comp_dict)
        comp_dict: dict = load(open(comp_dict, 'rb'))

        for (feat1, feat2), p in comp_dict.items():
            type1: str = get_type(header=feat1, col_types=col_types)
            type2: str = get_type(header=feat2, col_types=col_types)

            if type1 == NOMINAL_TYPE and type2 == NOMINAL_TYPE:
                row_key: str = NOM_NOM_KEY
            elif type1 == NUMERIC_TYPE and type2 == NUMERIC_TYPE:
                row_key: str = NUM_NUM_KEY
            else:
                row_key: str = NUM_NOM_KEY

            table[TOTAL_KEY][row_key] += 1

            if p < super_alpha:
                col_key: str = SUPER_ALPHA_KEY
            elif p < corrected_alpha:
                col_key: str = CORRECTED_ALPHA_KEY
            elif p < uncorrected_alpha:
                col_key: str = UNCORRECTED_ALPHA_KEY
            else:
                col_key: str = INSIGNIFICANT_KEY

            table[col_key][row_key] += 1

    print(table)
    table.to_csv(COUNTS_TABLE_PATH)


if __name__ == '__main__':
    main()
