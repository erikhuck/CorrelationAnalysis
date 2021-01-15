"""Filters the comparisons with p values that are above a given alpha"""

from sys import argv
from os import mkdir
from os.path import isdir, join
from pickle import dump

from utils import iterate_comp_dicts, ALPHA_FILTERED_DIR


def main():
    """Main method"""

    comp_dict_dir: str = argv[1]
    alpha: float = float(argv[2])
    idx: int = int(argv[3])
    section_size: int = int(argv[4])

    alpha_filtered_dir: str = ALPHA_FILTERED_DIR.format(alpha)

    if not isdir(alpha_filtered_dir):
        mkdir(alpha_filtered_dir)

    filtered_comparisons: dict = {}

    start_idx, stop_idx = iterate_comp_dicts(
        comp_dict_dir=comp_dict_dir, idx=idx, section_size=section_size, func=filter_by_alpha, alpha=alpha,
        filtered_comparisons=filtered_comparisons
    )
    print('Number of filtered comparisons:', len(filtered_comparisons))
    filtered_comparisons_path: str = join(alpha_filtered_dir, '{}-{}.p'.format(start_idx, stop_idx))
    dump(filtered_comparisons, open(filtered_comparisons_path, 'wb'))


def filter_by_alpha(feat1: str, feat2: str, p: float, alpha: float, filtered_comparisons: dict):
    """Adds a comparison to a dictionary of filtered comparisons if its p value is lower than a given alpha"""

    if p < alpha:
        key = feat1, feat2
        filtered_comparisons[key] = p


if __name__ == '__main__':
    main()
