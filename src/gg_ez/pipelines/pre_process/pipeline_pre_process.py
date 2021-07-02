from kedro.pipeline import Pipeline, node

from .nodes_pre_process import (
    pre_process_fixtures,
    pre_process_leagues,
    pre_process_players,
)


def create_pipeline_preprocess_leagues(**kwargs):
    return Pipeline([node(pre_process_leagues, "leagues_raw", "leagues_processed")])


def create_pipeline_preprocess_fixtures_leagues(**kwargs):
    return Pipeline(
        [
            node(
                pre_process_fixtures,
                ["games_raw", "params:n_cores"],
                "games_processed",
            ),
        ]
    )


def create_pipeline_preprocess_players_fixtures(**kwargs):
    return Pipeline(
        [
            node(
                pre_process_players, ["players", "params:n_cores"], "players_processed",
            ),
        ]
    )
