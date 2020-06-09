import re
import numpy as np
import pandas as pd
from typing import List, Iterable, Union, Callable


def apply_regex_filter(values: Iterable[str], pattern: str) -> List[str]:
    """
    Applies regular expresion filter over a list of values

    :param values: list-like object iterable (type: iterable[str])
    :param pattern: regex pattern (type: str)

    :return: filtered elements (type: list[str])
    """

    return list(filter(lambda x: re.search(pattern, x), values))


def count_by(table: pd.DataFrame, agg_columns: Union[str, List[str]]):
    """
    Computes a count by keys over a table

    :param table: input table
    :param agg_columns: key columns

    :return:
    """

    error = "Input must be str or List[str]"
    assert isinstance(agg_columns, str) | isinstance(agg_columns, list), error

    if isinstance(agg_columns, str):
        agg_columns = [agg_columns]

    return (
        table.groupby(agg_columns)
        .size()
        .reset_index()
        .rename(columns={0: "cnt"})
        .sort_values("cnt", ascending=False)
    )


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
