from typing import Union, List, Any

import pandas as pd


def unpack_columns(df, cols: List[Any]) -> pd.DataFrame:
    """
    Unpacks 2-level columns in a DataFrame

    :param df: table to unpack
    :param cols: name of columns to unpack

    :return: table with unpacked columns
    """

    if isinstance(cols, str):
        cols = [cols]
    res = []
    for col in cols:
        unpacked = df[col].apply(pd.Series)
        unpacked.columns = [f"{col}_{x}" for x in unpacked.columns]
        res.append(unpacked)

    return pd.concat(res, axis=1)


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
