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
    fetch_games_pipeline = fetch.create_pipeline_fetch_games()
    fetch_leagues_pipeline = fetch.create_pipeline_fetch_leagues()

    preprocess_leagues = pre_process.create_pipeline_preprocess_leagues()
    preprocess_fixtures_leagues = (
        pre_process.create_pipeline_preprocess_fixtures_leagues()
    )
    preprocess_players_fixtures = (
        pre_process.create_pipeline_preprocess_players_fixtures()
    )

    fatch_all = (
            fetch_player_stats_pipeline +
            fetch_games_pipeline +
            fetch_leagues_pipeline
    )

    preprocess_all = (
            preprocess_leagues
            + preprocess_fixtures_leagues
            + preprocess_players_fixtures
    )

    return {
        "fetch-leagues": fetch_leagues_pipeline,

        "fetch-all": (
            fetch_leagues_pipeline + fetch_games_pipeline + fetch_player_stats_pipeline
        ),
        "pre-process-leagues": preprocess_leagues,
        "pre-process-games": preprocess_fixtures_leagues,
        "pre-process-players": preprocess_players_fixtures,
        "pre-process-all": (
            preprocess_leagues
            + preprocess_fixtures_leagues
            + preprocess_players_fixtures
        ),
        "__default__": (
            fetch_leagues_pipeline
            + fetch_games_pipeline
            + fetch_player_stats_pipeline
            + preprocess_leagues
            + preprocess_fixtures_leagues
            + preprocess_players_fixtures
        ),
    }
