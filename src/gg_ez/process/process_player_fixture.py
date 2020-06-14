import pandas as pd
import numpy as np
from gg_ez.utilities.pandas import unpack_dict
from typing import List


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


def process_players_fixtures_stats(games: List[dict]):
    """

    :param games:

    :return:
    """

    games_stats = []
    for i, game in enumerate(games):
        games_stats.append(process_players_fixture_stats(game))

    games_stats = pd.concat(games_stats)

    games_stats["rating"] = games_stats["rating"].apply(fix_rating)
    return games_stats


def process_players_fixture_stats(game: dict):
    """

    :param game:

    :return:
    """

    game_stats = pd.DataFrame(game["api"]["players"])
    unpacked_cols = unpack_dict(game_stats, COLS_TO_UNPACK)
    game_stats = pd.concat([game_stats, unpacked_cols], axis=1)
    game_stats = game_stats.drop(columns=COLS_TO_UNPACK)

    return game_stats


def fix_rating(x):
    """ Helper function to conver rating to numeric"""
    if x == "–" or x is None:
        return np.nan
    else:
        return float(x)
