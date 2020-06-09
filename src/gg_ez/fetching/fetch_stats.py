import os
import logging
from gg_ez.utilities.io import save_json


def fetch_player_stats_in_league(handler, league_id, data_path=None):
    """
    For a given  league, fetches stats of all games at player level

    :param handler:
    :param league_id:
    :param data_path:

    :return:
    """

    logger = logging.getLogger(__name__)
    logger.info(f"Fetching stats for league: {league_id}")

    games = handler.get_json(f"fixtures/league/{league_id}")

    existing_stats = os.listdir(data_path / "players/fixture")
    fixture_ids = [x["fixture_id"] for x in games["api"]["fixtures"]]
    fixture_ids = [str(x) for x in fixture_ids]
    fixture_ids = list(filter(lambda x: x not in existing_stats, fixture_ids))

    logger.info(f"{len(fixture_ids)} game stats to wodnload")
    all_stats = []
    for fixture_id in fixture_ids:
        stats = fetch_player_stats_in_fixture(handler, fixture_id, data_path=data_path)
        all_stats.append(stats)

    return all_stats


def fetch_player_stats_in_fixture(handler, fixture_id, data_path=None):
    """
    Fetches player stats from a fixture, wand saves json if data_path is not None

    :param handler:
    :param fixture_id:
    :param data_path:

    :return:
    """

    logger = logging.getLogger(__name__)
    logger.info(f"Fetching player stats for fixure: {fixture_id}")

    stats = handler.get_json(f"players/fixture/{fixture_id}")
    if data_path:
        fixture_stats_path = data_path / f"players/fixture/{fixture_id}"
        logger.info(f"Saving to {fixture_stats_path}")
        os.makedirs(fixture_stats_path, exist_ok=True)
        save_json(stats, fixture_stats_path / "stats.json")

    return stats
