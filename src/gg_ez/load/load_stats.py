import os
from typing import Iterable

from gg_ez.utilities.utils import apply_regex_filter
from gg_ez.utilities.io import read_json


def load_player_stats_in_fixture(data_path, fixture_ids=None):
    """
    Loads local players stats in a list of games. If no specified games, load all

    :param data_path: base path where data is stored (type. pathlib.Path)
    :param fixture_ids: id of the game (type: Iterable)

    :return:
    """

    fixture_ids_to_download = os.listdir(data_path / "players/fixture")
    fixture_ids_to_download = apply_regex_filter(fixture_ids_to_download, "[0-9]+")

    if fixture_ids:
        if not isinstance(fixture_ids, Iterable):
            fixture_ids = [fixture_ids]

        fixture_ids = [str(x) for x in fixture_ids]
        fixture_ids_to_download = set.intersection(
            set(fixture_ids_to_download), set(fixture_ids)
        )

    stats = {}
    for id_to_load in fixture_ids_to_download:
        stats[id_to_load] = read_json(
            data_path / f"players/fixture/{id_to_load}/stats.json"
        )

    return stats
