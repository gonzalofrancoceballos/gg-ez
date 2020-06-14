from kedro.pipeline import Pipeline, node
from .nodes_process import process_fixtures_leagues


def create_pipeline_process_fixtures_leagues(**kwargs):
    return Pipeline(
        [
            node(
                process_fixtures_leagues,
                ["fixture_league_save", "params:n_cores",],
                "fixtures_leagues_processsed",
            ),
        ]
    )
