from kedro.pipeline import Pipeline, node
from .nodes_fetch import fetch_player_stats_in_leagues, fetch_games, fetch_leagues


def create_pipeline_fetch_leagues(**kwargs):
    return Pipeline([node(fetch_leagues, "params:api_token", "leagues",),])


def create_pipeline_fetch_games(**kwargs):
    return Pipeline(
        [
            node(
                fetch_games,
                [
                    "params:leagues",
                    "params:api_token",
                    "params:only_current",
                    "params:sleep",
                ],
                "fixture_league_save",
            ),
        ]
    )


def create_pipeline_player_stats(**kwargs):
    return Pipeline(
        [
            node(
                fetch_player_stats_in_leagues,
                [
                    "player_fixture_stats_load",
                    "params:leagues",
                    "params:api_token",
                    "params:only_current",
                    "params:sleep",
                ],
                "player_fixture_stats_save",
            ),
        ]
    )
