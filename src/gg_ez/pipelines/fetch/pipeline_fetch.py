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
                    "leagues",
                    "params:api_token",
                    "params:only_current",
                    "params:sleep",
                ],
                "fixtures",
            ),
        ]
    )


def create_pipeline_player_stats(**kwargs):
    return Pipeline(
        [
            node(
                fetch_player_stats_in_leagues,
                [
                    "players_check_existing",
                    "leagues",
                    "empty_games_check_existing",
                    "params:leagues",
                    "params:api_token",
                    "params:only_current",
                    "params:sleep",
                ],
                ["players", "empty_games"],
            ),
        ]
    )
