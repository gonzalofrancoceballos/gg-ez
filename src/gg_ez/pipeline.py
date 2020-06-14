from typing import Dict
from kedro.pipeline import Pipeline
from gg_ez.pipelines import fetch, pre_process


def create_pipelines(**kwargs) -> Dict[str, Pipeline]:
    """Create the project's pipeline.

    Args:
        kwargs: Ignore any additional arguments added in the future.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.

    """

    fetch_player_stats_pipeline = fetch.create_pipeline_player_stats()
    fetch_all_games_pipeline = fetch.create_pipeline_fetch_all_games()
    process_fixtures_leagues = pre_process.create_pipeline_process_fixtures_leagues()

    return {
        "fetch-player-stats": fetch_player_stats_pipeline,
        "fetch-all-games": fetch_all_games_pipeline,
        "pre-process-games": process_fixtures_leagues,
        "__default__": fetch_player_stats_pipeline,
    }
