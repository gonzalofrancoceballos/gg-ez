from kedro.pipeline import Pipeline, node
from .nodes import fetch_player_stats_in_league


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                fetch_player_stats_in_league,
                ["player_fixture_stats_load", "params:api_token", "params:league_id"],
                "player_fixture_stats_save",
            )
        ]
    )
