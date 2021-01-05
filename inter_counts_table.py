"""Creates a table with counts of the comparisons with p-values that reach various significance levels"""

from sys import argv
from os.path import join
from pickle import load
from pandas import DataFrame

from utils import (
    ALPHAS_PATH, INSIGNIFICANT_KEY, UNCORRECTED_ALPHA_KEY, CORRECTED_ALPHA_KEY, SUPER_ALPHA_KEY, get_col_types, IDX_COL,
    INTER_COUNTS_TABLE_DIR, iterate_comp_dicts, get_comparison_type, NUM_NUM_KEY, NOM_NOM_KEY, NUM_NOM_KEY
)

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

    start_idx, stop_idx = iterate_comp_dicts(
        comp_dict_dir=comp_dict_dir, idx=idx, section_size=section_size, func=count_comparisons, col_types=col_types,
        table=table, super_alpha=super_alpha, corrected_alpha=corrected_alpha, uncorrected_alpha=uncorrected_alpha
    )

    print(table)
    counts_table_path: str = join(INTER_COUNTS_TABLE_DIR, '{}-{}.csv'.format(start_idx, stop_idx))
    table.to_csv(counts_table_path)


def count_comparisons(
    feat1: str, feat2: str, p: float, col_types: dict, table: DataFrame, super_alpha: float, corrected_alpha: float,
    uncorrected_alpha: float
):
    """Determines the type of comparison and the alpha it is lower than and updates the counts table accordingly"""

    row_key: str = get_comparison_type(feat1=feat1, feat2=feat2, col_types=col_types)
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


if __name__ == '__main__':
    main()
