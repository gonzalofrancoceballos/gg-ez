import pandas as pd

import logging
from typing import Iterable

from gg_ez.pipelines.pre_process.core.league import leagues_dict2df
from gg_ez.utilities.folder_data import FolderData
from gg_ez.utilities.processing import apply_multiprocessing
from gg_ez.utilities.utils import kv_list_to_dict, dict_to_kv_list
from gg_ez.pipelines.pre_process.core.fixture import wrapper_fixture_league
from gg_ez.pipelines.pre_process.core.player_fixture import (
    process_players_fixture_stats,
)


def pre_process_leagues(leagues: dict) -> pd.DataFrame:
    """

    :param leagues:

    :return:
    """

    leagues_df = leagues_dict2df(leagues)

    return leagues_df


def pre_process_fixtures_leagues(
    fixture_league: FolderData,
    existing_processed_fixtures: FolderData,
    n_cores: int = 1,
) -> Iterable[pd.DataFrame]:
    """

    :param fixture_league:
    :param existing_processed_fixtures:
    :param n_cores:

    :return:
    """

    logger = logging.getLogger(__name__)

    fixture_league_data = fixture_league.get_data()
    leagues = dict_to_kv_list(fixture_league_data)

    existing = list(existing_processed_fixtures.paths_dict.values())
    existing = [p.name.split("_")[0] for p in existing]
    to_process = [[k, v] for k, v in leagues if k not in existing]

    logger.info(f"Pre-processing games for {len(to_process)} leagues...")

    processed_fixtures = apply_multiprocessing(
        wrapper_fixture_league, to_process, n_cores=n_cores
    )
    processed_fixtures = kv_list_to_dict(processed_fixtures)

    return processed_fixtures


def pre_process_players_fixtures(
    player_fixture_stats: FolderData,
    existing_processed_players: FolderData,
    n_cores: int = 1,
) -> Iterable[pd.DataFrame]:
    """

    :param player_fixture_stats:
    :param existing_processed_players:
    :param n_cores:

    :return:
    """

    logger = logging.getLogger(__name__)

    player_fixture_stats = player_fixture_stats.get_data()
    stats = dict_to_kv_list(player_fixture_stats)

    existing = list(existing_processed_players.paths_dict.values())
    existing = [p.name.split("_")[0] for p in existing]
    to_process = [[k, v] for k, v in stats if k not in existing]

    logger.info(f"Pre-processing stats for {len(to_process)} games...")

    processed_stats = apply_multiprocessing(
        process_players_fixture_stats, to_process, n_cores=n_cores
    )

    processed_stats = kv_list_to_dict(processed_stats)

    return processed_stats
