"""Sorts the alpha filtered comparisons and creates a table containing k comparisons with the lowest p values"""

from sys import argv
from os import listdir
from os.path import join
from pickle import load
from pandas import DataFrame

from utils import ALPHA_FILTERED_DIR, get_col_types, get_comparison_type

FEAT1_KEY: str = 'Feature 1'
FEAT2_KEY: str = 'Feature 2'
P_KEY: str = 'P Value'
COMP_TYPE_KEY: str = 'Comparison Type'


def main():
    """Main method"""

    k: int = int(argv[1])
    alpha: float = float(argv[2])

    print('K:', k)
    print('Alpha:', alpha)
    alpha_filtered_dir: str = ALPHA_FILTERED_DIR.format(alpha)
    filtered_dicts: list = (listdir(alpha_filtered_dir))
    top_k: dict = {}

    for filtered_dict in filtered_dicts:
        filtered_dict: str = join(alpha_filtered_dir, filtered_dict)
        filtered_dict: dict = load(open(filtered_dict, 'rb'))
        top_k.update(filtered_dict)

    n_below_alpha: int = len(top_k)
    print('Number below alpha:', n_below_alpha)

    if n_below_alpha < k:
        print('THE NUMBER OF COMPARISONS BELOW THE ALPHA IS LESS THAN THE GIVEN K')

    top_k: list = [(key, top_k[key]) for key in sorted(top_k, key=top_k.get)]
    top_k: list = top_k[:k]
    col_types: dict = get_col_types()
    table: DataFrame = DataFrame(columns=[FEAT1_KEY, FEAT2_KEY, P_KEY, COMP_TYPE_KEY])

    for (feat1, feat2), p in top_k:
        comp_type: str = get_comparison_type(feat1=feat1, feat2=feat2, col_types=col_types)
        row: dict = {
            FEAT1_KEY: feat1,
            FEAT2_KEY: feat2,
            P_KEY: p,
            COMP_TYPE_KEY: comp_type
        }
        table: DataFrame = table.append(row, ignore_index=True)

    table.to_csv('data/top-k.csv', index=False)


if __name__ == '__main__':
    main()
