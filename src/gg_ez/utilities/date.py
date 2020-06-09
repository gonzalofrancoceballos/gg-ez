import pandas as pd
import numpy as np
from typing import List, Any


def expand_dates(
    df: pd.DataFrame,
    key_cols: List[Any],
    from_col: str,
    to_col: str,
    date_col: str = "date",
    freq: str = "D",
    include_last: bool = False,
) -> pd.DataFrame:
    """
    Expands a DataFrame by from-to columns

    Original:
    |-------------------------------------|
    | col_1 | col_2 |   from   |    to    |
    |-------------------------------------|
    |   A   |   X   | 20191001 | 20191003 |
    |   B   |   Y   | 20191007 | 20191008 |
    |-------------------------------------|

    Expanded:
    |--------------------------|
    | col_1 | col_2 |   date   |
    |--------------------------|
    |   A   |   X   | 20191001 |
    |   A   |   X   | 20191002 |
    |   A   |   X   | 20191003 |
    |   B   |   Y   | 20191007 |
    |   B   |   Y   | 20191008 |
    |--------------------------|

    :param df: table to expand. Should have at least one key columns,a from-column and a to-column
    :param key_cols: name of columns to keep along with from, to columns
    :param from_col: name of "from" column
    :param to_col: name of "to" column
    :param date_col: name fo resulting date column
    :param freq: day, hour, etc
    :param include_last: to_date is included

    :return: expanded table
    """

    if freq not in ["H", "D"]:
        raise ValueError(f"got freq={freq}, expected 'H' or 'D'")

    df[from_col] = pd.to_datetime(df[from_col])
    df[to_col] = pd.to_datetime(df[to_col])

    df["date_to_1"] = df[from_col].dt.date
    df["date_to_2"] = df[to_col].dt.date
    df["hour_to_1"] = df[from_col].dt.hour
    df["n_to_extend"] = df[to_col] - df[from_col]
    if freq == "H":
        df["n_to_extend"] = (
            df["n_to_extend"].dt.seconds / 3600 + df["n_to_extend"].dt.days * 24
        )
    else:
        df["n_to_extend"] = df["n_to_extend"].dt.days

    if include_last:
        df["n_to_extend"] = df["n_to_extend"] + 1

    n_to_extend = list(df["n_to_extend"].values)
    datetimes_to_1 = list(df[from_col].values)
    datetimes_to_2 = list(df[to_col].values)

    cols_to_keep_exp = key_cols + ["date_to_1", "hour_to_1"]
    data_to_expand_np = df[cols_to_keep_exp].values

    data_expanded = []
    for np_element, n_repeat, dt_1, dt_2 in zip(
        data_to_expand_np, n_to_extend, datetimes_to_1, datetimes_to_2
    ):
        if include_last:
            date_sequence = pd.date_range(dt_1, dt_2, freq=f"1{freq}")
        else:
            date_sequence = pd.date_range(dt_1, dt_2, freq=f"1{freq}")[0:-1]
        date_sequence = np.array(date_sequence).reshape([-1, 1])

        new_element = np.repeat(np_element.reshape([1, -1]), n_repeat, axis=0)
        new_element = np.hstack([new_element, date_sequence])
        data_expanded.append(new_element)

    data_expanded = np.vstack(data_expanded)
    data_expanded = pd.DataFrame(data_expanded)
    data_expanded.columns = cols_to_keep_exp + [date_col]
    data_expanded = data_expanded[key_cols + [date_col]].copy()

    if freq == "D":
        data_expanded[date_col] = pd.to_datetime(data_expanded[date_col])
        data_expanded[date_col] = data_expanded[date_col].dt.date
    else:
        data_expanded[date_col] = pd.to_datetime(data_expanded[date_col])

    return data_expanded
