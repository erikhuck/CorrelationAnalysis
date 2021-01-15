"""Creates a table with counts of the comparisons with p-values that reach various significance levels"""

from sys import argv
<<<<<<< Updated upstream
from os import listdir
from os.path import join
<<<<<<< Updated upstream
from os import mkdir
from os.path import join, isdir
=======
=======
from os import mkdir
from os.path import join, isdir
>>>>>>> Stashed changes
>>>>>>> Stashed changes
from pickle import load
from pandas import DataFrame

from utils import (
<<<<<<< Updated upstream
    get_col_types, IDX_COL, INTER_COUNTS_TABLE_DIR, iterate_comp_dicts, get_comparison_type, NUM_NUM_KEY, NOM_NOM_KEY,
    NUM_NOM_KEY, MRI_MRI_KEY, EXPRESSION_EXPRESSION_KEY, ADNIMERGE_ADNIMERGE_KEY, MRI_EXPRESSION_KEY, MRI_ADNIMERGE_KEY,
    EXPRESSION_ADNIMERGE_KEY, DATA_TYPE_TABLE_TYPE, DOMAIN_TABLE_TYPE, MIN_ALPHA
=======
<<<<<<< Updated upstream
    get_type, NUMERIC_TYPE, NOMINAL_TYPE, ALPHAS_PATH, INSIGNIFICANT_KEY, UNCORRECTED_ALPHA_KEY, CORRECTED_ALPHA_KEY,
    SUPER_ALPHA_KEY, get_col_types, IDX_COL, INTER_COUNTS_TABLE_DIR
=======
    ALPHAS_PATH, INSIGNIFICANT_KEY, UNCORRECTED_ALPHA_KEY, CORRECTED_ALPHA_KEY, SUPER_ALPHA_KEY, MAX_SIGNIFICANCE_KEY,
    get_col_types, IDX_COL, INTER_COUNTS_TABLE_DIR, iterate_comp_dicts, get_comparison_type, NUM_NUM_KEY, NOM_NOM_KEY,
    NUM_NOM_KEY, MRI_MRI_KEY, EXPRESSION_EXPRESSION_KEY, ADNIMERGE_ADNIMERGE_KEY, MRI_EXPRESSION_KEY, MRI_ADNIMERGE_KEY,
    EXPRESSION_ADNIMERGE_KEY, DATA_TYPE_TABLE_TYPE, DOMAIN_TABLE_TYPE, MIN_ALPHA
>>>>>>> Stashed changes
>>>>>>> Stashed changes
)

TOTAL_KEY: str = 'Total'
ADNIMERGE_KEY: int = 0
EXPRESSION_KEY: int = 1
MRI_KEY: int = 2


def main():
    """Main method"""

    comp_dict_dir: str = argv[1]
    super_alpha: float = float(argv[2])
    idx: int = int(argv[3])
    section_size: int = int(argv[4])
    table_type: str = argv[5]

    table: DataFrame = make_table(table_type=table_type)

    assert table is not None
<<<<<<< Updated upstream

    uncorrected_alpha, corrected_alpha = load(open(ALPHAS_PATH, 'rb'))
    col_types: dict = get_col_types()
    start_idx, stop_idx = iterate_comp_dicts(
        comp_dict_dir=comp_dict_dir, idx=idx, section_size=section_size, func=count_comparisons, col_types=col_types,
        table=table, super_alpha=super_alpha, corrected_alpha=corrected_alpha, uncorrected_alpha=uncorrected_alpha,
        table_type=table_type
    )

    print(table)
    inter_counts_tables_dir: str = INTER_COUNTS_TABLE_DIR.format(table_type)

    if not isdir(inter_counts_tables_dir):
        mkdir(inter_counts_tables_dir)

    counts_table_path: str = join(inter_counts_tables_dir, '{}-{}.csv'.format(start_idx, stop_idx))
    table.to_csv(counts_table_path)


def make_table(table_type: str) -> DataFrame:
    """Creates the counts table of the given type"""

    if table_type == DATA_TYPE_TABLE_TYPE:
        idx_col: list = [NUM_NUM_KEY, NOM_NOM_KEY, NUM_NOM_KEY]
    elif table_type == DOMAIN_TABLE_TYPE:
        idx_col: list = [
            MRI_MRI_KEY, EXPRESSION_EXPRESSION_KEY, ADNIMERGE_ADNIMERGE_KEY, MRI_EXPRESSION_KEY, MRI_ADNIMERGE_KEY,
            EXPRESSION_ADNIMERGE_KEY
        ]
    else:
        return None

    empty_col: list = [0] * len(idx_col)
    table: DataFrame = DataFrame(
        {
            IDX_COL: idx_col,
            INSIGNIFICANT_KEY: empty_col,
            UNCORRECTED_ALPHA_KEY: empty_col,
            CORRECTED_ALPHA_KEY: empty_col,
            SUPER_ALPHA_KEY: empty_col,
            MAX_SIGNIFICANCE_KEY: empty_col,
            TOTAL_KEY: empty_col
        }
    )
    table: DataFrame = table.set_index(IDX_COL)
    return table


def count_comparisons(
    feat1: str, feat2: str, p: float, col_types: dict, table: DataFrame, super_alpha: float, corrected_alpha: float,
    uncorrected_alpha: float, table_type: str
):
    """Determines the type of comparison and the alpha it is lower than and updates the counts table accordingly"""

    if table_type == DATA_TYPE_TABLE_TYPE:
        row_key: str = get_comparison_type(feat1=feat1, feat2=feat2, col_types=col_types)
    else:
        row_key: str = get_comparison_domains(feat1=feat1, feat2=feat2, col_types=col_types)

    table[TOTAL_KEY][row_key] += 1

    if p < MIN_ALPHA:
        col_key: str = MAX_SIGNIFICANCE_KEY
    elif p < super_alpha:
        col_key: str = SUPER_ALPHA_KEY
    elif p < corrected_alpha:
        col_key: str = CORRECTED_ALPHA_KEY
    elif p < uncorrected_alpha:
        col_key: str = UNCORRECTED_ALPHA_KEY
    else:
        col_key: str = INSIGNIFICANT_KEY

    table[col_key][row_key] += 1


def get_comparison_domains(feat1: str, feat2: str, col_types: dict) -> str:
    """Indicates which domains the two features of a comparison come from"""

    domain1: str = get_domain(feat=feat1, col_types=col_types)
    domain2: str = get_domain(feat=feat2, col_types=col_types)

    if domain1 == MRI_KEY and domain2 == MRI_KEY:
        return MRI_MRI_KEY

    if domain1 == EXPRESSION_KEY and domain2 == EXPRESSION_KEY:
        return EXPRESSION_EXPRESSION_KEY

    if domain1 == ADNIMERGE_KEY and domain2 == ADNIMERGE_KEY:
        return ADNIMERGE_ADNIMERGE_KEY

    if (domain1 == MRI_KEY and domain2 == EXPRESSION_KEY) or (domain1 == EXPRESSION_KEY and domain2 == MRI_KEY):
        return MRI_EXPRESSION_KEY

    if (domain1 == MRI_KEY and domain2 == ADNIMERGE_KEY) or (domain1 == ADNIMERGE_KEY and domain2 == MRI_KEY):
        return MRI_ADNIMERGE_KEY

    assert (domain1 == EXPRESSION_KEY and domain2 == ADNIMERGE_KEY) or\
           (domain1 == ADNIMERGE_KEY and domain2 == EXPRESSION_KEY)

    return EXPRESSION_ADNIMERGE_KEY


def get_domain(feat: str, col_types: dict) -> int:
    """Gets the domain of a feature, either ADNIMERGE, Expression, or MRI"""

    if feat in col_types:
        return ADNIMERGE_KEY

    if 'MRI_' in feat:
        return MRI_KEY

    return EXPRESSION_KEY
=======

    uncorrected_alpha, corrected_alpha = load(open(ALPHAS_PATH, 'rb'))
    col_types: dict = get_col_types()
<<<<<<< Updated upstream
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
=======

    start_idx, stop_idx = iterate_comp_dicts(
        comp_dict_dir=comp_dict_dir, idx=idx, section_size=section_size, func=count_comparisons, col_types=col_types,
        table=table, super_alpha=super_alpha, corrected_alpha=corrected_alpha, uncorrected_alpha=uncorrected_alpha,
        table_type=table_type
    )
>>>>>>> Stashed changes

    print(table)
    inter_counts_tables_dir: str = INTER_COUNTS_TABLE_DIR.format(table_type)

    if not isdir(inter_counts_tables_dir):
        mkdir(inter_counts_tables_dir)

    counts_table_path: str = join(inter_counts_tables_dir, '{}-{}.csv'.format(start_idx, stop_idx))
    table.to_csv(counts_table_path)
>>>>>>> Stashed changes


<<<<<<< Updated upstream
=======
def make_table(table_type: str) -> DataFrame:
    """Creates the counts table of the given type"""

    if table_type == DATA_TYPE_TABLE_TYPE:
        idx_col: list = [NUM_NUM_KEY, NOM_NOM_KEY, NUM_NOM_KEY]
    elif table_type == DOMAIN_TABLE_TYPE:
        idx_col: list = [
            MRI_MRI_KEY, EXPRESSION_EXPRESSION_KEY, ADNIMERGE_ADNIMERGE_KEY, MRI_EXPRESSION_KEY, MRI_ADNIMERGE_KEY,
            EXPRESSION_ADNIMERGE_KEY
        ]
    else:
        return None

    empty_col: list = [0] * len(idx_col)
    table: DataFrame = DataFrame(
        {
            IDX_COL: idx_col,
            INSIGNIFICANT_KEY: empty_col,
            UNCORRECTED_ALPHA_KEY: empty_col,
            CORRECTED_ALPHA_KEY: empty_col,
            SUPER_ALPHA_KEY: empty_col,
            MAX_SIGNIFICANCE_KEY: empty_col,
            TOTAL_KEY: empty_col
        }
    )
    table: DataFrame = table.set_index(IDX_COL)
    return table


def count_comparisons(
    feat1: str, feat2: str, p: float, col_types: dict, table: DataFrame, super_alpha: float, corrected_alpha: float,
    uncorrected_alpha: float, table_type: str
):
    """Determines the type of comparison and the alpha it is lower than and updates the counts table accordingly"""

    if table_type == DATA_TYPE_TABLE_TYPE:
        row_key: str = get_comparison_type(feat1=feat1, feat2=feat2, col_types=col_types)
    else:
        row_key: str = get_comparison_domains(feat1=feat1, feat2=feat2, col_types=col_types)

    table[TOTAL_KEY][row_key] += 1

    if p < MIN_ALPHA:
        col_key: str = MAX_SIGNIFICANCE_KEY
    elif p < super_alpha:
        col_key: str = SUPER_ALPHA_KEY
    elif p < corrected_alpha:
        col_key: str = CORRECTED_ALPHA_KEY
    elif p < uncorrected_alpha:
        col_key: str = UNCORRECTED_ALPHA_KEY
    else:
        col_key: str = INSIGNIFICANT_KEY

    table[col_key][row_key] += 1


def get_comparison_domains(feat1: str, feat2: str, col_types: dict) -> str:
    """Indicates which domains the two features of a comparison come from"""

    domain1: str = get_domain(feat=feat1, col_types=col_types)
    domain2: str = get_domain(feat=feat2, col_types=col_types)

    if domain1 == MRI_KEY and domain2 == MRI_KEY:
        return MRI_MRI_KEY

    if domain1 == EXPRESSION_KEY and domain2 == EXPRESSION_KEY:
        return EXPRESSION_EXPRESSION_KEY

    if domain1 == ADNIMERGE_KEY and domain2 == ADNIMERGE_KEY:
        return ADNIMERGE_ADNIMERGE_KEY

    if (domain1 == MRI_KEY and domain2 == EXPRESSION_KEY) or (domain1 == EXPRESSION_KEY and domain2 == MRI_KEY):
        return MRI_EXPRESSION_KEY

    if (domain1 == MRI_KEY and domain2 == ADNIMERGE_KEY) or (domain1 == ADNIMERGE_KEY and domain2 == MRI_KEY):
        return MRI_ADNIMERGE_KEY

    assert (domain1 == EXPRESSION_KEY and domain2 == ADNIMERGE_KEY) or\
           (domain1 == ADNIMERGE_KEY and domain2 == EXPRESSION_KEY)

    return EXPRESSION_ADNIMERGE_KEY


def get_domain(feat: str, col_types: dict) -> int:
    """Gets the domain of a feature, either ADNIMERGE, Expression, or MRI"""

    if feat in col_types:
        return ADNIMERGE_KEY

    if 'MRI_' in feat:
        return MRI_KEY

    return EXPRESSION_KEY


>>>>>>> Stashed changes
if __name__ == '__main__':
    main()
