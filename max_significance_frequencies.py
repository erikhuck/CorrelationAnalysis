"""Makes a mapping from feature to the number of its comparisons that have a maximum significance (lowest possible p)"""

from os import listdir
from os.path import join
from pickle import load
from pandas import DataFrame

from utils import ALPHA_FILTERED_DIR, MIN_ALPHA

FEAT_KEY: str = 'Feature'
FREQ_KEY: str = 'Frequency'


def main():
    """Main method"""

    alpha_filtered_dir: str = ALPHA_FILTERED_DIR.format(MIN_ALPHA)
    filtered_dicts: list = (listdir(alpha_filtered_dir))
    max_significance_frequencies: dict = {}

    for filtered_dict in filtered_dicts:
        filtered_dict: str = join(alpha_filtered_dir, filtered_dict)
        filtered_dict: dict = load(open(filtered_dict, 'rb'))

        for (feat1, feat2), p in filtered_dict.items():
            assert p == 0.0

            add_frequency(feat=feat1, max_significance_frequencies=max_significance_frequencies)
            add_frequency(feat=feat2, max_significance_frequencies=max_significance_frequencies)

    max_significance_frequencies: list = [
        (key, max_significance_frequencies[key]) for key in sorted(
            max_significance_frequencies, key=max_significance_frequencies.get, reverse=True
        )
    ]

    table: DataFrame = DataFrame(columns=[FEAT_KEY, FREQ_KEY])

    for feat, frequency in max_significance_frequencies:
        row: dict = {
            FEAT_KEY: feat,
            FREQ_KEY: frequency,
        }
        table: DataFrame = table.append(row, ignore_index=True)

    table.to_csv('data/max_significance_frequencies.csv', index=False)


def add_frequency(feat: str, max_significance_frequencies: dict):
    """Increments the frequency for a given feature"""

    if feat in max_significance_frequencies:
        max_significance_frequencies[feat] += 1
    else:
        max_significance_frequencies[feat] = 1


if __name__ == '__main__':
    main()
