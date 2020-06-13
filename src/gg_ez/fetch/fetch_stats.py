import logging


def fetch_player_stats_in_fixture(handler, fixture_id):
    """
    Fetches player stats from a fixture, wand saves json if data_path is not None

    :param handler:
    :param fixture_id:

    :return:
    """

    logger = logging.getLogger(__name__)
    logger.info(f"Fetching player stats for fixure: {fixture_id}")
    stats = handler.get_json(f"players/fixture/{fixture_id}")

    return stats
