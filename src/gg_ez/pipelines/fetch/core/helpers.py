from typing import List

import pandas as pd

from gg_ez.pipelines.pre_process.core.league import leagues_dict2df


def get_league_ids(
    leagues: List[dict],
    valid_leagues: List[List[str]],
    only_current: bool = False,
    fixtures_players_statistics: bool = False,
):
    """
    Filters league ids to download based on is of leagues

    :param valid_leagues: list of valid leagues to consider
    :param leagues: all leagues info
    :param only_current: filter leagues taking place in current seasons
    :param fixtures_players_statistics: filter leagues that have player-fixture stats

    :return: list of filteres leagues
    """

    leagues = leagues_dict2df(leagues)
    valid_leagues_df = pd.DataFrame(valid_leagues, columns=["country", "name"])
    leagues = pd.merge(leagues, valid_leagues_df)
    if only_current:
        leagues = leagues[(leagues["is_current"] == 1)].copy()

    if fixtures_players_statistics:
        leagues = leagues[leagues["coverage_fixtures_players_statistics"]]

    league_ids = leagues["league_id"]
    return league_ids


def get_finished_fixtures(fixtures_in_league):
    finshed_fistures = [
        str(x["fixture_id"])
        for x in fixtures_in_league["api"]["fixtures"]
        if x["status"] == "Match Finished"
    ]
    return finshed_fistures
