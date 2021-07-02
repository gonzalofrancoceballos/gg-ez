import re
from typing import Callable, Iterable, List, Union

import numpy as np


def apply_regex_filter(values: Iterable[str], pattern: str) -> List[str]:
    """
    Applies regular expresion filter over a list of values

    :param values: list-like object iterable (type: iterable[str])
    :param pattern: regex pattern (type: str)

    :return: filtered elements (type: list[str])
    """

    return list(filter(lambda x: re.search(pattern, x), values))


def round_to_resolution(
    x: np.array, resolution: Union[float, int], round_method: Callable = np.round
) -> np.array:
    """
    Rounds a value or list of values to a given resolution

    :param x:
    :param resolution:
    :param round_method:

    :return:
    """

    return resolution * round_method(x / resolution)


def kv_list_to_dict(kv_list: Iterable[List]) -> dict:
    """Converts list of key-values to dictionary"""
    output = {}
    for k, v in kv_list:
        output[k] = v
    return output


def dict_to_kv_list(my_dict: dict) -> Iterable:
    """Converts dictionary to list of key-values """
    output = []
    for k in my_dict:
        output.append([k, my_dict[k]])
    return output
