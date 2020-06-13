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
        all_stats[fixture_id] = fetch_player_stats_in_fixture(handler, fixture_id)
        if sleep:
            time.sleep(sleep)

    return all_stats
