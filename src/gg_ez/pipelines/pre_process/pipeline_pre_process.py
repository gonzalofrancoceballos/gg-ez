from kedro.pipeline import Pipeline, node
from .nodes_pre_process import (
    pre_process_fixtures,
    pre_process_leagues,
    pre_process_players,
)


def create_pipeline_preprocess_leagues(**kwargs):
    return Pipeline([node(pre_process_leagues, "leagues", "leagues_processed")])


def create_pipeline_preprocess_fixtures_leagues(**kwargs):
    return Pipeline(
        [
            node(
                pre_process_fixtures,
                ["fixtures", "params:n_cores"],
                "fixtures_processed",
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
