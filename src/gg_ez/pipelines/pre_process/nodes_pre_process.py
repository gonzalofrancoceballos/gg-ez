import pandas as pd

from gg_ez.pipelines.pre_process.core.league import leagues_dict2df
from gg_ez.utilities.folder_data import JSONFolderData
from gg_ez.utilities.processing import apply_multiprocessing
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
    fixture_league: JSONFolderData, n_cores: int = 1
) -> pd.DataFrame:
    """

    :param fixture_league:
    :param n_cores:

    :return:
    """

    fixture_league_data = fixture_league.get_data()
    leagues = list(fixture_league_data.values())
    processed_fixtures = apply_multiprocessing(
        wrapper_fixture_league, leagues, n_cores=n_cores
    )

    processed_fixtures = pd.concat(processed_fixtures)

    return processed_fixtures


def pre_process_players_fixtures(
    player_fixture_stats: JSONFolderData, n_cores: int = 1
) -> pd.DataFrame:
    """

    :param player_fixture_stats:
    :param n_cores:

    :return:
    """

    player_fixture_stats = player_fixture_stats.get_data()
    stats = list(player_fixture_stats.values())
    processed_stats = apply_multiprocessing(
        process_players_fixture_stats, stats, n_cores=n_cores
    )

    return pd.concat(processed_stats)
