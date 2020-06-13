import pandas as pd


def unpack_dict(df, cols):
    """

    :param df:
    :param cols:

    :return:
    """

    if isinstance(cols, str):
        cols = [cols]
    res = []
    for col in cols:
        unpacked = df[col].apply(pd.Series)
        unpacked.columns = [f"{col}_{x}" for x in unpacked.columns]
        res.append(unpacked)

    return pd.concat(res, axis=1)
