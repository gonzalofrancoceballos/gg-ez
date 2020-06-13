import pandas as pd
from gg_ez.utilities.pandas import unpack_dict

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


def process_games_stats(games):
    """

    :param games:

    :return:
    """

    games_stats = []
    for i, game in enumerate(games):
        games_stats.append(process_game_stats(game))

    games_stats = pd.concat(games_stats)
    return games_stats


def process_game_stats(game):
    """

    :param game:

    :return:
    """

    game_stats = pd.DataFrame(game["api"]["players"])
    unpacked_cols = unpack_dict(game_stats, COLS_TO_UNPACK)
    game_stats = pd.concat([game_stats, unpacked_cols], axis=1)
    game_stats = game_stats.drop(columns=COLS_TO_UNPACK)

    return game_stats
