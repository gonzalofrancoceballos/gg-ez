import numpy as np

from typing import List, Any
from gg_ez.utilities.processing import apply_multiprocessing
from gg_ez.utilities.dict import flatten_dict


def pre_process_leagues(leagues: List[dict]) -> List[dict]:
    """
    Pre-processes leagues into flatten distionaries that can rapidly be casted
    to pd.DataFrame

    :param leagues: raw leagues info

    :return: processed leagues
    """

    flatten_leagues = [flatten_dict(data) for data in leagues]

    return flatten_leagues


def pre_process_fixtures(fixtures: List[dict], n_cores: int = 1) -> List[dict]:
    """
    Pre-processes fixtures into flatten distionaries that can rapidly be casted
    to pd.DataFrame

    :param fixtures: raw fixtures info
    :param n_cores: number of corres in multi-processing

    :return: processed fixtures
    """

    processed_fixtures = apply_multiprocessing(flatten_dict, fixtures, n_cores=n_cores)

    return processed_fixtures


def pre_process_players(players: List[dict], n_cores: int = 1) -> List[dict]:
    """
    Pre-processes fixtures into flatten distionaries that can rapidly be casted
    to pd.DataFrame

    :param players: raw players stats
    :param n_cores: number of corres in multi-processing

    :return: processed players
    """

    processed_players = apply_multiprocessing(flatten_dict, players, n_cores=n_cores)
    for player in processed_players:
        player["rating"] = to_float(player["rating"])
    return processed_players


def to_float(x: Any) -> float:
    """
    Converts a string to float

    :param x: value to convert

    :return: converted value
    """

    if isinstance(x, str):
        x = x.replace(",", ".")
    else:
        return np.nan

    try:
        x = float(x)
    except ValueError:
        x = np.nan
    return x
