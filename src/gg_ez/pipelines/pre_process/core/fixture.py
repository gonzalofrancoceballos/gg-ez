import pandas as pd
from gg_ez.utilities.pandas import unpack_dict

COLS_TO_UNPACK = ["league", "homeTeam", "awayTeam", "score"]


def process_fixture(fixture: dict):
    """
    Pre-processes fixtures stats into a table

    :param fixture:

    :return: processed table (type: pd.DataFrame)
    """

    fixture_df = pd.DataFrame([fixture])
    unpacked_cols = unpack_dict(fixture_df, COLS_TO_UNPACK)
    fixture_df = pd.concat([fixture_df, unpacked_cols], axis=1)
    fixture_df = fixture_df.drop(columns=COLS_TO_UNPACK)

    return fixture_df


def wrapper_fixture_league(league):
    res = []
    for fixture in league["api"]["fixtures"]:
        res.append(process_fixture(fixture))

    return pd.concat(res)
