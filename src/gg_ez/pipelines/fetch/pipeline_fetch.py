from kedro.pipeline import Pipeline, node
from .nodes_fetch import fetch_player_stats_in_league, fetch_all_games


def create_pipeline_player_stats(**kwargs):
    return Pipeline(
        [
            node(
                fetch_player_stats_in_league,
                [
                    "player_fixture_stats_load",
                    "params:api_token",
                    "params:league_id",
                    "params:sleep",
                ],
                "player_fixture_stats_save",
            ),
        ]
    )


def create_pipeline_fetch_all_games(**kwargs):
    return Pipeline(
        [
            node(
                fetch_all_games,
                ["params:api_token", "params:sleep",],
                "fixture_league_save",
            ),
        ]
    )
