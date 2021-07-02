import logging


def fetch_player_stats_in_fixture(rapidapi, fixture_id):
    """
    Fetches player stats from a fixture

    :param rapidapi:
    :param fixture_id:

    :return:
    """

    logger = logging.getLogger(__name__)
    logger.info(f"Fetching player stats for fixure: {fixture_id}")
    stats = rapidapi(f"players/fixture/{fixture_id}")

    return stats
