import logging
import time
from typing import Any
from gg_ez.api.handlers import JSONHandler
from gg_ez.api.connector import RapidApiConnector
from gg_ez.fetch.fetch_stats import fetch_player_stats_in_fixture
from gg_ez.utilities.io import JSONData


def fetch_player_stats_in_league(
    player_fixture_stats: JSONData, api_token: str, league_id: Any, sleep: float = None,
):
    """
    For a given  league, fetches stats of all games at player level

    :param player_fixture_stats:
    :param api_token:
    :param league_id:
    :param sleep:

    :return:
    """

    logger = logging.getLogger(__name__)
    logger.info(f"Fetching stats for league: {league_id}")

    existing_stats = player_fixture_stats.paths_dict.keys()

    handler = JSONHandler(RapidApiConnector(api_token))
    games = handler.get_json(f"fixtures/league/{league_id}")
    fixture_ids = [
        str(x["fixture_id"])
        for x in games["api"]["fixtures"]
        if x["status"] == "Match Finished"
    ]

    fixture_ids = list(filter(lambda x: x not in existing_stats, fixture_ids))
    logger.info(f"{len(fixture_ids)} game stats to download")

    all_stats = {}
    for fixture_id in fixture_ids:
        stats_i = fetch_player_stats_in_fixture(handler, fixture_id)
        if stats_i["api"]["results"] > 0:
            all_stats[fixture_id] = stats_i
        else:
            logger.warning(f"Fixture {fixture_id} fetched, but empty. Skipping...")
        if sleep:
            time.sleep(sleep)

    return all_stats


def fetch_all_games(api_token: str, sleep: float = None):
    """
    Fetches all games in a league
    :param api_token:
    :param sleep:

    :return:
    """

    logger = logging.getLogger(__name__)

    handler = JSONHandler(RapidApiConnector(api_token))
    leagues = handler.get_json("leagues")
    league_ids = [league["league_id"] for league in leagues["api"]["leagues"]]
    all_games = {}

    logger.info(f"Fetching game info: {len(league_ids)} leagues")
    for league_id in league_ids:
        logger.info(f"Fetching game info for league: {league_id}")
        game = handler.get_json(f"fixtures/league/{league_id}")
        all_games[league_id] = game
        time.sleep(sleep)

    return all_games
