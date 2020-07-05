import logging
import time
from typing import List

from gg_ez.pipelines.fetch.core.helpers import get_league_ids, get_finished_fixtures
from gg_ez.utilities.folder_data import FolderData
from gg_ez.api.handlers import JSONHandler
from gg_ez.api.connector import RapidApiConnector
from gg_ez.pipelines.fetch.core.player_fixture import fetch_player_stats_in_fixture


def fetch_leagues(api_token: str) -> dict:
    """
    Fetches list of all available leagues

    :param api_token: API token to fetch the data

    :return:
    """

    handler = JSONHandler(RapidApiConnector(api_token))
    leagues = handler.get_json(f"leagues")

    return leagues


def fetch_games(
    valid_leagues: List[List[str]],
    api_token: str,
    only_current: bool = False,
    sleep: float = None,
):
    """
    Fetches all games in selected leagues

    :param valid_leagues: list of leagues to consider. Each element of the list is a
        list with [{country}, {name}] wich needs to match the ``leagues`` table
    :param api_token: API token to fetch the data
    :param only_current: only download games from current year
    :param sleep: sleep time between calls

    :return:
    """

    logger = logging.getLogger(__name__)
    league_ids = get_league_ids(valid_leagues, api_token, only_current)

    logger.info(f"Fetching game info: {len(league_ids)} leagues")
    handler = JSONHandler(RapidApiConnector(api_token))
    all_games = {}
    for league_id in league_ids:
        logger.info(f"Fetching game info for league: {league_id}")
        game = handler.get_json(f"fixtures/league/{league_id}")
        all_games[league_id] = game
        time.sleep(sleep)

    return all_games


def fetch_player_stats_in_leagues(
    player_fixture_stats: FolderData,
    valid_leagues: List[List[str]],
    api_token: str,
    only_current: bool = False,
    sleep: float = None,
):
    """
    For a given league, fetches stats of all games at player level

    :param player_fixture_stats:
    :param valid_leagues: list of leagues to consider. Each element of the list is a
        list with [{country}, {name}] wich needs to match the ``leagues`` table
    :param api_token: API token to fetch the data
    :param only_current: only download games from current year
    :param sleep: sleep time between calls

    :return:
    """

    logger = logging.getLogger(__name__)
    handler = JSONHandler(RapidApiConnector(api_token))

    # Get league_ids of all leagues to download stats from
    league_ids = get_league_ids(valid_leagues, api_token, only_current)
    logger.info(f"{len(league_ids)} leagues will be checked")

    # Identify all games that have finished in those leagues
    finished_fixtures = []
    for league_id in league_ids:
        logger.info(f"Exploring league_id: {league_id}")
        fixtures_in_league = handler.get_json(f"fixtures/league/{league_id}")
        finished_fixtures += (get_finished_fixtures(fixtures_in_league))
        if sleep:
            time.sleep(sleep)

    # Identify finished games not downloaded
    existing_stats = player_fixture_stats.paths_dict.keys()
    finished_fixtures_not_downloaded = list(
        filter(lambda x: x not in existing_stats, finished_fixtures)
    )
    logger.info(
        f"{len(finished_fixtures_not_downloaded)} player-fixture stats to download"
    )

    # Download games
    all_stats = {}
    for fixture_id in finished_fixtures_not_downloaded:
        stats_i = fetch_player_stats_in_fixture(handler, fixture_id)
        if stats_i["api"]["results"] > 0:
            all_stats[fixture_id] = stats_i
        else:
            logger.warning(f"Fixture {fixture_id} fetched, but empty. Skipping...")
        if sleep:
            time.sleep(sleep)

    return all_stats
