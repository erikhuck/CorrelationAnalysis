"""Confirms that the dictionaries resulting from a newer implementation match those from an older implementation"""

from os import listdir
from os.path import join
from sys import argv
from pickle import load
from numpy import isclose


def main():
    """MAIN"""

    old_dicts_path: str = argv[1]
    new_dicts_path: str = argv[2]

    old_matrix: dict = combine_dicts(dicts_path=old_dicts_path)
    new_matrix: dict = combine_dicts(dicts_path=new_dicts_path)

    assert len(old_matrix) == len(new_matrix)
    assert old_matrix.keys() == new_matrix.keys()

    for p1, p2 in zip(sorted(old_matrix.values()), sorted(new_matrix.values())):
        assert isclose(p1, p2)


def combine_dicts(dicts_path) -> dict:
    """Combines the dictionaries, each representing a portion of the comparison matrix, into one"""

    matrix: dict = {}
    dicts: list = listdir(dicts_path)

    for dict_ in dicts:
        if dict_.endswith('.p'):
            dict_: str = join(dicts_path, dict_)
            dict_: dict = load(open(dict_, 'rb'))
            matrix.update(dict_)

    return matrix


if __name__ == '__main__':
    main()
