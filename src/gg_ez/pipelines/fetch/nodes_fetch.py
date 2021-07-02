import logging
import time

from typing import List
from datetime import datetime

from gg_ez.pipelines.fetch.core.helpers import get_league_ids, get_finished_fixtures
from gg_ez.pipelines.fetch.core.player_fixture import fetch_player_stats_in_fixture


def fetch_leagues(rapidapi: callable) -> dict:
    """
    Fetches list of all available leagues

    :param rapidapi: fetch function for RapidAPI

    :return: leagues data
    """
    leagues = rapidapi("leagues")
    leagues = leagues["api"]["leagues"]
    for league in leagues:
        league["_id"] = league["league_id"]

    return leagues


def fetch_games(
    rapidapi: callable,
    leagues_raw: List[dict],
    valid_leagues: List[List[str]],
    only_current: bool = False,
    sleep: float = None,
) -> List[dict]:
    """
    Fetches all games in selected leagues

    :param rapidapi: fetch function for RapidAPI
    :param leagues_raw:
    :param valid_leagues: list of leagues to consider. Each element of the list is a
        list with [{country}, {name}] wich needs to match the ``leagues`` table
    :param only_current: only download games from current year
    :param sleep: sleep time between calls

    :return:
    """

    logger = logging.getLogger(__name__)
    league_ids = get_league_ids(leagues_raw, valid_leagues, only_current)

    logger.info(f"Fetching game info: {len(league_ids)} leagues")
    all_games = []
    for league_id in league_ids:
        logger.info(f"Fetching games info for league: {league_id}")
        games = rapidapi(f"fixtures/league/{league_id}")
        games = games["api"]["fixtures"]
        for game in games:
            game["_id"] = game["fixture_id"]
            all_games.append(game)
        time.sleep(sleep)

    return all_games


def fetch_player_stats_in_leagues(
    rapidapi: callable,
    existing_player_stats: List[dict],
    leagues: List[dict],
    empty_games: List[dict],
    valid_leagues: List[List[str]],
    only_current: bool = False,
    sleep: float = None,
):
    """
    For a given league, fetches stats of all games at player level

    :param rapidapi: fetch function for RapidAPI
    :param existing_player_stats:
    :param leagues:
    :param empty_games:
    :param valid_leagues: list of leagues to consider. Each element of the list is a
        list with [{country}, {name}] wich needs to match the ``leagues`` table
    :param only_current: only download games from current year
    :param sleep: sleep time between calls

    :return:
    """

    logger = logging.getLogger(__name__)

    # Get league_ids of all leagues to download stats from
    league_ids = get_league_ids(
        leagues, valid_leagues, only_current, fixtures_players_statistics=True
    )
    logger.info(f"{len(league_ids)} leagues will be checked")

    # Identify all games that have finished in those leagues
    finished_fixtures = []
    for league_id in league_ids:
        logger.info(f"Exploring league_id: {league_id}")
        fixtures_in_league = rapidapi(f"fixtures/league/{league_id}")
        finished_fixtures += get_finished_fixtures(fixtures_in_league)
        if sleep:
            time.sleep(sleep)

    finished_fixtures = set(finished_fixtures)

    # Identify finished games not downloaded
    existing_stats = set([str(stat["event_id"]) for stat in existing_player_stats])
    empty_game_ids = {game["_id"] for game in empty_games}
    finished_fixtures_not_downloaded = finished_fixtures - existing_stats
    finished_fixtures_not_downloaded = finished_fixtures_not_downloaded - empty_game_ids

    logger.info(
        f"{len(finished_fixtures_not_downloaded)} player-fixture stats to download"
    )

    # Download games
    all_stats = []
    all_empty_stats = []
    for fixture_id in list(finished_fixtures_not_downloaded)[0:70000]:
        game = fetch_player_stats_in_fixture(rapidapi, fixture_id)
        if game["api"]["results"] > 0:
            for player in game["api"]["players"]:
                player["_id"] = f"{player['player_id']}_{player['event_id']}"
                all_stats.append(player)
        else:
            logger.warning(
                f"Fixture {fixture_id} fetched, but empty. Logging and skipping..."
            )
            all_empty_stats.append({"_id": fixture_id, "fetch_time": datetime.now()})

        if sleep:
            time.sleep(sleep)

    return all_stats, all_empty_stats
