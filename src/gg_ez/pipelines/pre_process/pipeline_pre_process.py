from kedro.pipeline import Pipeline, node
from .nodes_pre_process import (
    pre_process_fixtures_leagues,
    pre_process_leagues,
    pre_process_players_fixtures,
)


def create_pipeline_preprocess_leagues(**kwargs):
    return Pipeline([node(pre_process_leagues, "leagues", "leagues_processed",),])


def create_pipeline_preprocess_fixtures_leagues(**kwargs):
    return Pipeline(
        [
            node(
                pre_process_fixtures_leagues,
                ["fixture_league_save", "params:n_cores",],
                "fixtures_leagues_processed",
            ),
        ]
    )


def create_pipeline_preprocess_players_fixtures(**kwargs):
    return Pipeline(
        [
            node(
                pre_process_players_fixtures,
                ["player_fixture_stats_save", "params:n_cores",],
                "players_fixtures_processed",
            ),
        ]
    )
