"""Contains shared data and functions"""

from pickle import load
from os import listdir
from os.path import join
from time import time
from tqdm import tqdm

NUMERIC_TYPE: str = 'numeric'
NOMINAL_TYPE: str = 'nominal'
COL_TYPES_PATH: str = 'data/col-types.csv'
COL_TYPES_PICKLE_PATH: str = 'data/col-types.p'
START_IDX_KEY: str = 'Start Index'
STOP_IDX_KEY: str = 'Stop Index'
N_ROWS_KEY: str = 'Number of Rows'
ALPHAS_PATH: str = 'data/alphas.p'
INTER_COUNTS_TABLE_DIR: str = 'data/inter-counts-tables/{}'
COUNTS_TABLE_PATH: str = 'data/{}-counts-table.csv'
ALPHA_FILTERED_DIR: str = 'data/alpha-filtered-{}'
INSIGNIFICANT_KEY: str = 'No Significance'
UNCORRECTED_ALPHA_KEY: str = 'Below Uncorrected Alpha'
CORRECTED_ALPHA_KEY: str = 'Below Bonferroni Corrected Alpha'
SUPER_ALPHA_KEY: str = 'Below Super Alpha'
MAX_SIGNIFICANCE_KEY: str = 'Maximum Significance'
IDX_COL: str = 'idx'
NUM_NUM_KEY: str = 'Numerical Numerical'
NOM_NOM_KEY: str = 'Categorical Categorical'
NUM_NOM_KEY: str = 'Numerical Categorical'
MRI_MRI_KEY: str = 'MRI MRI'
EXPRESSION_EXPRESSION_KEY: str = 'Expression Expression'
ADNIMERGE_ADNIMERGE_KEY: str = 'ADNIMERGE ADNIMERGE'
MRI_EXPRESSION_KEY: str = 'MRI Expression'
MRI_ADNIMERGE_KEY: str = 'MRI ADNIMERGE'
EXPRESSION_ADNIMERGE_KEY: str = 'Expression ADNIMERGE'
DATA_TYPE_TABLE_TYPE: str = 'data-type'
DOMAIN_TABLE_TYPE: str = 'domain'
MIN_ALPHA: float = 5e-324


def get_type(header: str, col_types: dict) -> str:
    """Gets the data type of a column given its header"""

    # All the MRI and expression data is numeric and thus does not need to be included in the column types
    if header not in col_types:
        return NUMERIC_TYPE

    return col_types[header]


def iterate_comp_dicts(comp_dict_dir: str, idx: int, section_size: int, func: callable, **kwargs) -> tuple:
    """Iterates through the comparison dictionaries in a given section and performs a given function on them"""

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
            func(feat1=feat1, feat2=feat2, p=p, **kwargs)

        time2: float = time()
        print('Time Iterating Through Comparisons: {:.2f} minutes'.format((time2 - time1) / 60))

    return start_idx, stop_idx


def get_col_types() -> dict:
    """Gets the dictionary mapping a column header name to its corresponding data type"""

    return load(open(COL_TYPES_PICKLE_PATH, 'rb'))


def get_comparison_type(feat1: str, feat2: str, col_types: dict):
    """Returns the type of the comparison which is the data type of the first feature and that of the other feature"""

    type1: str = get_type(header=feat1, col_types=col_types)
    type2: str = get_type(header=feat2, col_types=col_types)

    if type1 == NOMINAL_TYPE and type2 == NOMINAL_TYPE:
        comp_type: str = NOM_NOM_KEY
    elif type1 == NUMERIC_TYPE and type2 == NUMERIC_TYPE:
        comp_type: str = NUM_NUM_KEY
    else:
        comp_type: str = NUM_NOM_KEY

    return comp_type
