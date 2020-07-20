import pandas as pd

from gg_ez.utilities.pandas import unpack_dict

COLS_TO_UNPACK_LEAGUES = [["coverage"], ["coverage_fixtures"]]


def leagues_dict2df(leagues):
    """Converts raw leagues dict to processed table"""

    leagues_df = pd.DataFrame(leagues["api"]["leagues"])
    for cols_to_unpack in COLS_TO_UNPACK_LEAGUES:
        unpacked_cols = unpack_dict(leagues_df, cols_to_unpack)
        leagues_df = pd.concat([leagues_df, unpacked_cols], axis=1)
        leagues_df = leagues_df.drop(columns=cols_to_unpack)

    return leagues_df