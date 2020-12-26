"""Creates a table with counts of the comparisons with p-values that reach various significance levels"""

from sys import argv
from os import listdir
from os.path import join
from pickle import load
from pandas import DataFrame
from time import time
from tqdm import tqdm

from utils import (
    get_type, NUMERIC_TYPE, NOMINAL_TYPE, ALPHAS_PATH, INSIGNIFICANT_KEY, UNCORRECTED_ALPHA_KEY, CORRECTED_ALPHA_KEY,
    SUPER_ALPHA_KEY, get_col_types, IDX_COL, INTER_COUNTS_TABLE_DIR
)

NUM_NUM_KEY: str = 'Numerical Numerical'
NOM_NOM_KEY: str = 'Categorical Categorical'
NUM_NOM_KEY: str = 'Numerical Categorical'
TOTAL_KEY: str = 'Total'


def main():
    """Main method"""

    comp_dict_dir: str = argv[1]
    super_alpha: float = float(argv[2])
    idx: int = int(argv[3])
    section_size: int = int(argv[4])

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

    # Remove the files that aren't comparison dictionaries
    new_comp_dicts: list = []

    for comp_dict in comp_dicts:
        if comp_dict.endswith('.p'):
            new_comp_dicts.append(comp_dict)

    comp_dicts: list = sorted(new_comp_dicts)
    del new_comp_dicts

    n_dicts: int = len(comp_dicts)
    print('Total Number Of Comparison Dictionaries:', n_dicts)
    start_idx: int = idx * section_size

    if start_idx >= n_dicts:
        print(
            'ERROR: Start index of {} is greater than or equal to the number of comparison dictionaries {}'.format(
                start_idx, n_dicts
            )
        )
        exit(1)

    print('Start Index:', start_idx)
    stop_idx: int = min(start_idx + section_size, n_dicts)
    print('Stop Index:', stop_idx)
    comp_dicts: list = comp_dicts[start_idx:stop_idx]

    for comp_dict in comp_dicts:
        comp_dict: str = join(comp_dict_dir, comp_dict)
        print('Loading Comparison Dictionary At:', comp_dict)
        time1: float = time()
        comp_dict: dict = load(open(comp_dict, 'rb'))
        time2: float = time()
        print('Load Time: {:.2f} minutes'.format((time2 - time1) / 60))

        n_comparisons: int = len(comp_dict)
        print('Total Number Of Comparisons:', n_comparisons)
        time1: float = time()

        for (feat1, feat2), p in tqdm(comp_dict.items()):
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

        time2: float = time()
        print('Time Iterating Through Comparisons: {:.2f} minutes'.format((time2 - time1) / 60))

    print(table)
    counts_table_path: str = join(INTER_COUNTS_TABLE_DIR, '{}-{}.csv'.format(start_idx, stop_idx))
    table.to_csv(counts_table_path)


if __name__ == '__main__':
    main()
