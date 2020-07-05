import pandas as pd

from gg_ez.api.connector import RapidApiConnector
from gg_ez.api.handlers import JSONHandler
from gg_ez.pipelines.pre_process.core.league import leagues_dict2df


def get_league_ids(valid_leagues, api_token, only_current=False):
    handler = JSONHandler(RapidApiConnector(api_token))
    leagues = handler.get_json("leagues")
    leagues = leagues_dict2df(leagues)
    valid_leagues_df = pd.DataFrame(valid_leagues, columns=["country", "name"])
    leagues = pd.merge(leagues, valid_leagues_df)
    if only_current:
        leagues = leagues[leagues["is_current"] == 1].copy()
    league_ids = leagues["league_id"]
    return league_ids


def get_finished_fixtures(fixtures_in_league):
    finshed_fistures = [
        str(x["fixture_id"])
        for x in fixtures_in_league["api"]["fixtures"]
        if x["status"] == "Match Finished"
    ]
    return finshed_fistures
