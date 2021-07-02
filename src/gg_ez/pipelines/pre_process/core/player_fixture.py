from typing import List, Tuple

import numpy as np
import pandas as pd

from gg_ez.utilities.pandas import unpack_columns

COLS_TO_UNPACK = [
    "shots",
    "goals",
    "passes",
    "tackles",
    "duels",
    "dribbles",
    "fouls",
    "cards",
    "penalty",
]


def process_players_fixture_stats(game: List) -> Tuple:
    """

    :param game:

    :return:
    """

    game_id, game_stats = game
    game_stats = pd.DataFrame(game_stats["api"]["players"])
    unpacked_cols = unpack_columns(game_stats, COLS_TO_UNPACK)
    game_stats = pd.concat([game_stats, unpacked_cols], axis=1)
    game_stats = game_stats.drop(columns=COLS_TO_UNPACK)

    return game_id, game_stats


def fix_rating(x):
    """ Helper function to conver rating to numeric"""

    if x == "–" or x is None:
        return np.nan
    else:
        return float(x)


def wrapper_player_fixture(league):
    res = []
    for fixture in league["api"]["fixtures"]:
        res.append(process_players_fixture_stats(fixture))

    return pd.concat(res)
