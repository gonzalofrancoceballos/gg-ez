from kedro.pipeline import Pipeline, node
from .nodes_fetch import fetch_player_stats_in_leagues, fetch_games, fetch_leagues


def create_pipeline_fetch_leagues(**kwargs):
    return Pipeline([node(fetch_leagues, "rapidapi", "leagues_raw")])


def create_pipeline_fetch_games(**kwargs):
    return Pipeline(
        [
            node(
                fetch_games,
                [
                    "rapidapi",
                    "leagues_raw",
                    "params:leagues",
                    "params:only_current",
                    "params:sleep",
                ],
                "games_raw",
            ),
        ]
    )


def create_pipeline_player_stats(**kwargs):
    return Pipeline(
        [
            node(
                fetch_player_stats_in_leagues,
                [
                    "rapidapi",
                    "players_check_existing",
                    "leagues_raw",
                    "empty_games_check_existing",
                    "params:leagues",
                    "params:only_current",
                    "params:sleep",
                ],
                ["players", "empty_games"],
            ),
        ]
    )
